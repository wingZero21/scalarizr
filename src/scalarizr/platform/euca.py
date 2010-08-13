'''
Created on Aug 13, 2010

@author: marat
'''

from scalarizr.platform.ec2 import Ec2Platform
from scalarizr.platform import PlatformError
from urlparse import urlparse
import boto
import logging

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
	
	def _get_conn_params(self, service='ec2'):
		attr = '_%s_conn_params' % service
		if not hasattr(self, attr):
			url = self.get_user_data(UD_OPT_EC2_URL if service == 'ec2' else UD_OPT_S3_URL)  
			if not url:
				s = service.upper() + " URL user data property is empty"
				raise PlatformError(s)
			u = urlparse(url)
			setattr(self, attr, dict(is_secure=u.scheme, host=u.hostname, port=u.port, part='/'+u.path))
		return getattr(self, attr)
	
	def new_ec2_conn(self):
		''' @rtype: boto.ec2.connection.EC2Connection '''
		self._logger.debug('Return eucaliptus ec2 connection (url: %s)', self.get_user_data(UD_OPT_EC2_URL))
		return boto.connect_ec2(*self.get_access_keys(), **self._get_conn_params('ec2'))
	
	def new_s3_conn(self):
		''' @rtype: boto.ec2.connection.S3Connection '''		
		self._logger.debug('Return eucaliptus s3 connection (url: %s)', self.get_user_data(UD_OPT_S3_URL))		
		return boto.connect_s3(*self.get_access_keys(), **self._get_conn_params('s3'))	