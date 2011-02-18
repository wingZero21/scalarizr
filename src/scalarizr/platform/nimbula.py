'''
Created on Feb 14, 2011

@author: spike
'''
from scalarizr.util import wait_until, filetool
from . import Ec2LikePlatform, PlatformError

import os

def get_platform():
	return NimbulaPlatform()
OPT_USERDATA_TIMEOUT = "wait_user_data_timeout"

class NimbulaPlatform(Ec2LikePlatform):
	
	name = "nimbula"
	
	_userdata_key = 'latest/userdata'
	_metadata_key = 'latest/metadata'
	
	def get_user_data(self, key=None):
		"""
		Since userdata is not implemented on Nimbula cloud yet, .user-data file is used.		
		""" 
		if self._userdata is None:
			path = self._cnf.private_path('.user-data')
			try:
				timeout = int(self._cnf.rawini.get(self.name, OPT_USERDATA_TIMEOUT))
			except:
				timeout = 180
			wait_until(os.path.exists, (path, ), logger=self._logger, timeout=timeout, sleep=1)
			rawmeta = filetool.read_file(path)
			if not rawmeta:
				raise PlatformError("Empty user-data")
			self._userdata = self._parse_user_data(rawmeta)
		if key:
			return self._userdata[key] if key in self._userdata else None
		else:
			return self._userdata