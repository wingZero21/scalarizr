'''
Created on Sep 7, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.libs.metaconf import Configuration
import os

class CnfPreset:
	name = None
	settings = None
	
	def __init__(self, name=None, settings=None):
		self.name = name
		self.settings = settings or {}
		#where is restart variable?

class CnfPresetStore:
	class PresetType:
		DEFAULT = 'default'
		LAST_SUCCESSFUL = 'last_successful'
		CURRENT = 'current'
	
	def __init__(self):
		cnf = bus.cnf
		self.presets_path = os.path.join(cnf.home_path, 'presets')
		if not os.path.exists(self.presets_path):
			try:
				os.makedirs(self.presets_path)
			except OSError,e:
				pass
	
	def _filename(self, service_name, preset_type):
		return os.path.join(self.presets_path,service_name, '.', preset_type)
	
	def load(self, service_name, preset_type):
		'''
		@rtype: Preset
		@raise OSError, MetaconfError: 
		'''
		ini = Configuration('ini')
		ini.read(self._filename(service_name, preset_type))
		#why we need name section if we have preset_type variable?
		return CnfPreset(ini.get('general/name'), ini.get_dict('settings')) 
		
	def save(self, service_name, preset, preset_type):
		'''
		@type service_name: str
		@type preset: CnfPreset
		@type preset_type: CnfPresetStore.PresetType
		'''
		ini = Configuration('ini')
		ini.set('general/name', preset.name or 'Noname')
		for k, v in preset.settings:
			ini.set('settings/%s' % k, v)
		ini.write(self._filename(service_name, preset_type))
		
class CnfController(object):
	def current_preset(self):
		'''
		Собрать все настройки, подлежащие обязательному обязательному сохранению.
		@rtype: CnfPreset
		'''
		pass

	def apply_preset(self, preset):
		'''
		Применить настройки
		@type preset: CnfPreset
		@raise:
		'''
		pass	
