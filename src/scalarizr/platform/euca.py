'''
Created on Aug 13, 2010

@author: marat
'''

from scalarizr.platform.ec2 import Ec2Platform
from scalarizr.platform import PlatformError
from scalarizr.util import configtool
from urlparse import urlparse
import boto
import logging
from scalarizr.bus import bus
from scalarizr.config import ScalarizrState


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
		
		if self._cnf.state in (ScalarizrState.BOOTSTRAPPING, ScalarizrState.UNKNOWN) and self.get_user_data():
			# Apply options from user-data
			user_data = self.get_user_data()
			self._cnf.update_ini({self.name: {
				's3_url' : user_data[UD_OPT_S3_URL],
				'ec2_url' : user_data[UD_OPT_EC2_URL] 
			}})
			
	
	def _get_conn_params(self, service='ec2'):
		
		attr = '_%s_conn_params' % service
		
		if not hasattr(self, attr):
			url = self._cnf.rawini.get(self.name, service == 'ec2' and 'ec2_url' or 's3_url')
			if not url:
				raise PlatformError('%s url is empty' % service)
			u = urlparse(url)
			setattr(self, attr, dict(is_secure=u.scheme, host=u.hostname, port=u.port, part='/'+u.path))
			
		return getattr(self, attr)
	
	def new_ec2_conn(self):
		''' @rtype: boto.ec2.connection.EC2Connection '''
		self._logger.debug('Return eucaliptus ec2 connection')
		return boto.connect_ec2(*self.get_access_keys(), **self._get_conn_params('ec2'))
	
	def new_s3_conn(self):
		''' @rtype: boto.ec2.connection.S3Connection '''		
		self._logger.debug('Return eucaliptus s3 connection')		
		return boto.connect_s3(*self.get_access_keys(), **self._get_conn_params('s3'))	