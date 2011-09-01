'''
Created on Jul 7, 2011

@author: shaitanich
'''

import os
import logging

from scalarizr.libs.metaconf import Configuration, NoPathError


def lazy(init):
	def wrapper(cls, *args, **kwargs):
		obj = init(cls, *args, **kwargs)
		return LazyInitScript(obj)
	return wrapper


class LazyInitScript(object):
	
	_script = None
	reload_queue = None
	restart_queue = None
	
	def __init__(self, script):
		self._script = script
		self.reload_queue = []
		self.restart_queue = []

	def start(self):
		try:
			if not self._script.running:
				self._script.start()
			elif self.restart_queue:
				reasons = ' '.join([req+',' for req in self.restart_queue])[:-1]
				self._script.restart(reasons)	
			elif self.reload_queue:
				reasons = ' '.join([req+',' for req in self.reload_queue])[:-1]
				self._script.reload(reasons)		
		finally:
			self.restart_queue = []
			self.reload_queue = []	
		
	def stop(self, reason=None):
		if self._script.running:
			try:
				self._script.stop(reason)
			finally:
				self.restart_queue = []
				self.reload_queue = []	

	def restart(self, reason=None, force=False):
		if force:
			self._script.restart(reason)
		elif  self._script.running:
			self.restart_queue.append(reason)
		
	def reload(self, reason=None):
		if self._script.running:
			self.reload_queue.append(reason)
	
	@property		
	def running(self):
		return self._script.running
	
	def status(self):
		return self._script.status()
			

class BaseService(object):

	_objects = None
	
	def _set(self, key, obj):
		self._objects[key] = obj
		
	def _get(self, key, callback, *args, **kwargs):
		if not self._objects.has_key(key):
			self._set(key, callback(*args, **kwargs))
		return self._objects[key]	
	
	
class BaseConfig(object):
	'''
	Parent class for object representations of postgresql.conf and recovery.conf which fortunately both have similar syntax
	'''
	
	autosave = None
	path = None
	data = None
	config_name = None
	config_type = None
	
	def __init__(self, path, autosave=True):
		self._logger = logging.getLogger(__name__)
		self.autosave = autosave
		self.path = path
		
	@classmethod
	def find(cls, config_dir):
		return cls(os.path.join(config_dir.path, cls.config_name))
		
	def set(self, option, value):
		if not self.data:
			self.data = Configuration(self.config_type)
			if os.path.exists(self.path):
				self.data.read(self.path)
		self.data.set(option,value, force=True)
		if self.autosave:
			self.save_data()
			self.data = None
			
	def set_path_type_option(self, option, path):
		if not os.path.exists(path):
			raise ValueError('%s %s does not exist' % (option, path))
		self.set(option, path)		
		
	def set_numeric_option(self, option, number):
		try:
			assert not number or int(number)
			self.set(option, str(number))
		except ValueError:
			raise ValueError('%s must be a number (got %s instead)' % (option, number))
					
	def get(self, option):
		if not self.data:
			self.data =  Configuration(self.config_type)
			if os.path.exists(self.path):
				self.data.read(self.path)	
		try:
			value = self.data.get(option)	
		except NoPathError:
			try:
				value = getattr(self, option+'_default')
			except AttributeError:
				value = None
		if self.autosave:
			self.data = None
		return value
	
	def get_numeric_option(self, option):
		value = self.get(option)
		assert not value or int(value)
		return int(value) if value else 0
	
	def save_data(self):
		if self.data:
			self.data.write(self.path)			

	