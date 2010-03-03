'''
Created on Dec 24, 2009

@author: marat
'''

class PlatformError(BaseException):
	pass

class PlatformFactory(object):
	_platforms = {}
	
	def new_platform(self, name):
		if not self._platforms.has_key(name):
			pl = __import__("scalarizr.platform." + name, globals(), locals(), fromlist=["get_platform"])
			self._platforms[name] = pl.get_platform()

		return self._platforms[name];

class Platform():
	name = None
	
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
	
	def get_metadata(self):
		"""
		@return dict
		"""
		pass	
