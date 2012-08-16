'''
Created on Jul 7, 2011

@author: shaitanich
'''

import os
import logging

from scalarizr.libs.metaconf import Configuration, NoPathError
from scalarizr.util import initdv2
import shutil


LOG = logging.getLogger(__name__)


def lazy(init):
	def wrapper(cls, *args, **kwargs):
		obj = init(cls, *args, **kwargs)
		return LazyInitScript(obj)
	return wrapper


class LazyInitScript(object):
	
	_script = None
	reload_queue = None
	restart_queue = None
	
	def __getattr__(self, name):
		return getattr(self._script, name) 
	
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
				LOG.info('Stopping service: %s' % reason)
				self._script.stop(reason)
			finally:
				self.restart_queue = []
				self.reload_queue = []	

	def restart(self, reason=None, force=False):
		if force:
			self._script.restart(reason)
		elif  self._script.running:
			self.restart_queue.append(reason)
		
	def reload(self, reason=None, force=False):
		if force:
			self._script.reload(reason)
		elif self._script.running:
			self.reload_queue.append(reason)
			
	def configtest(self, path=None):
		self._script.configtest(path)
		
	
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
	comment_empty = False
	
	
	def __init__(self, path, autosave=True):
		self._logger = logging.getLogger(__name__)
		self.autosave = autosave
		self.path = path
		
		
	@classmethod
	def find(cls, config_dir):
		return cls(os.path.join(config_dir.path, cls.config_name))
		
		
	def set(self, option, value):
		self._init_configuration()
		if value:
			self.data.set(option,str(value), force=True)
		elif self.comment_empty: 
			self.data.comment(option)
		if self.autosave:
			self.save_data()
			self.data = None
			
			
	def set_path_type_option(self, option, path):
		if not os.path.exists(path):
			raise ValueError('%s %s does not exist' % (option, path))
		self.set(option, path)		
		
		
	def set_numeric_option(self, option, number):
		try:
			assert number is None or type(number) is int
		except AssertionError:
			raise ValueError('%s must be a number (got %s instead)' % (option, number))
		
		is_numeric = type(number) is int
		self.set(option, str(number) if is_numeric else None)

					
	def get(self, option):
		self._init_configuration()	
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
		try:
			assert value is None or int(value)
		except AssertionError:
			raise ValueError('%s must be a number (got %s instead)' % (option, type(value)))
		return value if value is None else int(value)
	
	
	def save_data(self):
		if self.data:
			self.data.write(self.path)		
			
			
	def to_dict(self, section=None):
		self._init_configuration()
		section = './' + section if section else './'		
		try:
			kv = dict(self.data.items())	
		except NoPathError:
			kv = {}
		if self.autosave:
			self.data = None
		return kv
	
	
	def apply_values(self, kv, section=None):
		for option, value in kv.items():
			path = '%s/%s' (section, option) if section else option
			self.set(path, value)
	
	
	def _init_configuration(self, file_path):
		if not self.data:
			self.data = Configuration(self.config_type)
			if os.path.exists(self.path):
				self.data.read(self.path)
			
				
class ServiceError(BaseException):
	pass


class PresetError(BaseException):
	pass


class PresetProvider(object):
	
	service = None
	config_data = None
	backup_prefix = '.scalr.backup'
	
	
	def __init__(self, service, *config_objects, **kv):
		self.service = service
		self.config_data = kv or dict()
		for obj in config_objects:
			self.config_data[obj] = None
			

	def get_preset(self):
		preset = {}
		for obj, section in self.config_sections.items():
			preset[obj.config_name] = obj.to_dict(section)
		return preset
			
	
	def set_preset(self, settings):
		self.backup()
		
		for obj in self.config_data:
			if obj.config_name in settings:
				obj.apply_values(settings[obj.config_name])
				
		try:
			self.configtest()
		except initdv2.InitdError, e:
			self.rollback(cleanup=True)
			raise PresetError('Service %s was unable to pass configtest' % self.service.name)
			
		try:
			self.restart('Applying configuration preset to %s service' % self.service.name)
		except BaseException, e:
				if not self.service.running:
					self.rollback()
					self.restart('Restarting %s service with old configuration files' % self.service.name)
					raise PresetError('Service %s was unable to start with the new preset.' % self.service.name)
		else:
			self.cleanup()
			
		
	def backup(self):
		for obj in self.config_data:
			src = obj.path
			if os.path.exists(src):
				dst = src + self.backup_prefix
				shutil.copy(src, dst)


	def cleanup(self):
		for obj in self.config_data:
			src = obj.path + self.backup_prefix
			if os.path.exists(src):
				os.remove(src)

					
	def rollback(self, cleanup=False):
		for obj in self.config_data:
			src = obj.path + self.backup_prefix
			if os.path.exists(src):
				dst = obj.path
				shutil.copy(src, dst)
				self.rollback_hook()
		if cleanup:
			self.cleanup()
			
				
	def rollback_hook(self):
		'''
		for tasks like setting bitmask and owner
		'''
		pass
				
				
	def configtest(self):
		return self.service.configtest()
	
	
	def restart(self, reason=None):
		self.service.restart(reason)

	
		