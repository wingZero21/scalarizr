'''
Created on Dec 24, 2009

@author: marat
'''
from scalarizr.core import Bus, BusEntries
from scalarizr.util import save_config
import os
import re

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
	_bus = None
	_arch = None
	
	def __init__(self):
		self._bus = Bus()
	
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

	def set_config_options(self, options):
		"""
		Inject into scalarizr configuration platform config
		"""
		config = self._bus[BusEntries.CONFIG]
		section = "platform_" + self.name
		if not config.has_section(section):
			config.add_section(section)
		self._set_config(config, section, options)
		save_config()
		
	def get_config_option(self, option):
		return self._bus[BusEntries.CONFIG].get("platform_" + self.name, option)
		
	def _set_config_options(self, config, section, options):
		pass


class Architectures:
	I386 = "i386"
	X86_64 = "x86_64"
	UNKNOWN = "unknown"