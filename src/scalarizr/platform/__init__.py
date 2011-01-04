'''
Created on Dec 24, 2009

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.util.filetool import read_file
import os
import re
import socket

class PlatformError(BaseException):
	pass

class UserDataOptions:
	SERVER_ID = "serverid"	
	ROLE_NAME = "realrolename"
	CRYPTO_KEY = "szr_key"
	QUERYENV_URL = "queryenv_url"
	MESSAGE_SERVER_URL = "p2p_producer_endpoint"
	FARM_HASH = "hash"

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
	_access_data = None
	_metadata = None
	
	def get_private_ip(self):
		return self.get_public_ip()
	
	def get_public_ip(self):
		return socket.gethostbyname(socket.gethostname())
	
	def get_user_data(self, key=None):
		cnf = bus.cnf
		path = cnf.private_path('.user-data')
		if self._metadata is None and os.path.exists(path):
			rawmeta = read_file(path)
			if not rawmeta:
				raise PlatformError("Empty user-data")
			
			self._metadata = {}
			for k, v in re.findall("([^=]+)=([^;]*);?", rawmeta):
				self._metadata[k] = v			
		if key:
			return self._metadata[key] if key in self._metadata else None
		else:
			return self._metadata 

	def set_access_data(self, access_data):
		self._access_data = access_data
	
	def get_access_data(self, prop=None):
		if prop:
			try:
				return self._access_data[prop]
			except TypeError, KeyError:
				raise PlatformError("Platform access data property '%s' doesn't exists" % (prop,))
		else:
			return self._access_data
		
	def clear_access_data(self):
		self._access_data = None
	
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
	
	@property
	def cloud_storage_path(self):
		pass


class Architectures:
	I386 = "i386"
	X86_64 = "x86_64"
	UNKNOWN = "unknown"