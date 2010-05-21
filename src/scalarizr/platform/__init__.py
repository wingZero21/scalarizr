'''
Created on Dec 24, 2009

@author: marat
'''
from scalarizr.bus import bus
import os
import re

class PlatformError(BaseException):
	pass

class UserDataOptions:
	SERVER_ID = "serverid"	
	ROLE_NAME = "realrolename"
	CRYPTO_KEY = "szr_key"
	QUERYENV_URL = "queryenv_url"
	MESSAGE_SERVER_URL = "p2p_producer_endpoint"	

class PlatformFactory(object):
	_platforms = {}
	
	def new_platform(self, name):
		if not self._platforms.has_key(name):
			pl = __import__("scalarizr.platform." + name, globals(), locals(), fromlist=["get_platform"])
			self._platforms[name] = pl.get_platform()

		return self._platforms[name];

class Platform():
	name = None
	_arch = None
	
	def get_private_ip(self):
		"""
		@return string 
		"""
		pass
	
	def get_public_ip(self):
		"""
		@return string 
		"""
		pass
	
	def get_user_data(self, key=None):
		"""
		@return dict|any
		"""
		return {} if key else None

	
	def get_architecture(self):
		"""
		@return Architectures 
		"""
		if self._arch is None:
			uname = os.uname()
			if re.search("^i\\d86$", uname[4]):
				self._arch = Architectures.I386
			elif re.search("^x86_64$", uname[4]):
				self._arch = Architectures.X86_64
			else:
				self._arch = Architectures.UNKNOWN
		return self._arch


class Architectures:
	I386 = "i386"
	X86_64 = "x86_64"
	UNKNOWN = "unknown"