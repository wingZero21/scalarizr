'''
Created on Aug 13, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.platform import PlatformError
from scalarizr.platform.ec2 import Ec2Platform

from scalarizr.util.filetool import write_file

import logging, os
from urlparse import urlparse

import boto
from boto.ec2.regioninfo import RegionInfo
from boto.s3.connection import OrdinaryCallingFormat
from M2Crypto import SSL

def get_platform():
	return EucaPlatform()

"""
User data options 
"""

UD_OPT_S3_URL = 's3_url'
UD_OPT_EC2_URL = 'ec2_url'


class EucaPlatform(Ec2Platform):
	name = 'euca'

	def __init__(self):
		Ec2Platform.__init__(self)
		self._logger = logging.getLogger(__name__)
		cnf = bus.cnf; 
		cnf.on('apply_user_data', self.on_cnf_apply_user_data)
			
	def on_cnf_apply_user_data(self, cnf):
		user_data = self.get_user_data()
		cnf.update_ini(self.name, {self.name: {
			's3_url' : user_data[UD_OPT_S3_URL],
			'ec2_url' : user_data[UD_OPT_EC2_URL] 
		}})
		
			
	def get_ec2_cert(self):
		if not self._ec2_cert:
			cert_path = os.path.join(bus.etc_path, self._cnf.rawini.get(self.name, 'cloud_cert_path'))
			if not os.path.exists(cert_path):
				
				ec2_url = self._cnf.rawini.get(self.name, 'ec2_url')
				url = urlparse(ec2_url)
				addr = (url.hostname, url.port if url.port else 80)

				ctx = SSL.Context()
				conn = SSL.Connection(ctx)
				conn.set_post_connection_check_callback(None)
				conn.connect(addr)
				cert = conn.get_peer_cert()
				cert.save_pem(cert_path)
				
			self._ec2_cert = self._cnf.read_key(cert_path, title="Eucalyptus certificate")
		return self._ec2_cert	
	
	def new_ec2_conn(self):
		''' @rtype: boto.ec2.connection.EC2Connection '''
		self._logger.debug('Creating eucalyptus ec2 connection')
		if not hasattr(self, '_ec2_conn_params'):
			url = self._cnf.rawini.get(self.name, 'ec2_url')
			if not url:
				raise PlatformError('EC2(Eucalyptus) url is empty')
			u = urlparse(url)
			self._ec2_conn_params = dict(
				is_secure = u.scheme == 'https', 
				port = u.port, 
				path = '/'+u.path,
				region = RegionInfo(name='euca', endpoint=u.hostname)
			)
			
		return boto.connect_ec2(*self.get_access_keys(), **self._ec2_conn_params)
	
	def new_s3_conn(self):
		''' @rtype: boto.ec2.connection.S3Connection '''		
		self._logger.debug('Creating eucalyptus s3 connection')
		if not hasattr(self, '_s3_conn_params'):
			url = self._cnf.rawini.get(self.name, 's3_url')
			if not url:
				raise PlatformError('S3(Walrus) url is empty')
			u = urlparse(url)
			self._s3_conn_params = dict(
				is_secure = u.scheme == 'https', 
				port = u.port, 
				path = '/'+u.path,
				host = u.hostname,
				calling_format = OrdinaryCallingFormat()				
			)
				
		return boto.connect_s3(*self.get_access_keys(), **self._s3_conn_params)	