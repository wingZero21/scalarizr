'''
Created on Sep 7, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.libs.metaconf import Configuration
import os
import logging
from scalarizr.libs.metaconf import Configuration, ParseError, MetaconfError,\
	PathNotExistsError

class CnfPreset:
	#TODO: overload equasion
	name = None
	settings = None
	behaviour = None
	
	def __init__(self, name=None, settings=None, behaviour = None):
		self.name = name
		self.settings = settings or {}
		self.behaviour = behaviour

	def __repr__(self):
		return 'name = ' + str(self.name) \
	+ "; settings = " + str(self.settings)
	
	def __eq__(self, preset):
		#fetch manifest
		#iter manifest
		#ignore inaccurate values
		#if no key in preset
		#if keys in preset.settings and self.settings ain`t equal
		return False
		

class CnfPresetStore:
	class PresetType:
		DEFAULT = 'default'
		LAST_SUCCESSFUL = 'last_successful'
		CURRENT = 'current'
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		cnf = bus.cnf
		self.presets_path = os.path.join(cnf.home_path, 'presets')
		if not os.path.exists(self.presets_path):
			try:
				os.makedirs(self.presets_path)
			except OSError,e:
				pass
	
	def _filename(self, service_name, preset_type):
		return os.path.join(self.presets_path,service_name + '.' + preset_type)
	
	def load(self, service_name, preset_type):
		'''
		@rtype: Preset
		@raise OSError, MetaconfError: 
		'''
		self._logger.debug('Loading %s %s preset' % (preset_type, service_name))
		ini = Configuration('ini')
		ini.read(self._filename(service_name, preset_type))
		return CnfPreset(ini.get('general/name'), dict(ini.items('settings/'))) 
		
	def save(self, service_name, preset, preset_type):
		'''
		@type service_name: str
		@type preset: CnfPreset
		@type preset_type: CnfPresetStore.PresetType
		'''
		if not preset or not hasattr(preset, 'settings'):
			self._logger.error('Cannot save preset: No settings in preset found.')
			return
		
		self._logger.debug('Saving preset as %s' % preset_type)
		ini = Configuration('ini')
		ini.add('general')
		ini.add('general/name', preset.name if (hasattr(preset, 'name') and preset.name) else 'Noname')
		ini.add('settings')
		print 'saving:', preset
		for k, v in preset.settings.items():
			ini.add('settings/%s' % k, v)
		ini.write(open(self._filename(service_name, preset_type), 'w'))
		
		
class CnfController(object):
	
	behaviour = None
	options = None
	_config = None
	_config_format = None
	
	def __init__(self, behaviour, config):
		self._logger = logging.getLogger(__name__)
		self.behaviour = behaviour
		self._config = config
		self.options=_CnfManifest(self._get_manifest(self.behaviour))

	def current_preset(self):
		self._logger.debug('Getting current %s preset', self.behaviour)
		preset = CnfPreset(name='current', behaviour = self.behaviour)
		
		#conf = Configuration(self._get_config_type(self.behaviour))
		conf = Configuration(self._config_format)
		conf.read(self._config)
		
		vars = {}
		
		for option_spec in self.options:
			try:
				vars[option_spec.name] = conf.get(option_spec.name)
			except PathNotExistsError:
				#self._logger.debug('%s does not exist in %s. Using default value' 
				#		%(option_spec.name, self._config))
				pass

				if option_spec.default_value:
					vars[option_spec.name] = option_spec.default_value
				else:
					self._logger.debug('default value for %s not found'%(option_spec.name))
				
		preset.settings = vars
		return preset

	def apply_preset(self, preset):
		self._logger.debug('Applying %s preset' % (preset.name if preset.name else 'undefined',))
		
		conf = Configuration(self._get_config_type(self.behaviour))
		conf.read(self._config)
		
		for option_spec in self.options:
			var = option_spec.name if not option_spec.section else '%s/%s'%(option_spec.section, option_spec.name)
			
			if preset.settings.has_key(option_spec.name):
				
				# Skip unsupported
				if option_spec.supported_from and option_spec.supported_from > self._get_version():
					self._logger.debug('%s supported from %s. Cannot apply.' 
							% (option_spec.name, option_spec.supported_from))
					continue
								
				if not option_spec.default_value:
					self._logger.debug('No default value for %s' % option_spec.name)
					
				elif preset.settings[option_spec.name] == option_spec.default_value:
					try:
						conf.remove(var)
						self._logger.debug('%s value is equal to default. Removed from config.' % option_spec.name)
					except PathNotExistsError:
						pass
					continue	

				if conf.get(var) == preset.settings[option_spec.name]:
					self._logger.debug('Variable %s wasn`t changed. Skipping.' % option_spec.name)
				else:
					self._logger.debug('Setting variable %s to %s' % (option_spec.name, preset.settings[option_spec.name]))
					conf.set(var, preset.settings[option_spec.name], force=True)
					
			else:
				try:
					conf.remove(var)
					self._logger.debug('%s option not found in preset. Removing from config.' % option_spec.name)
				except PathNotExistsError:
					pass							

		conf.write(open(self._config, 'w'))	
	
	def _get_manifest(self, behaviour):
		#TODO: make GET query to scalr
		return os.path.join(os.path.realpath('./test/unit/resources/manifest'), behaviour + '.ini')
	
	def _get_config_type(self, service_name):
		services = {'mysql':'mysql',
				'app':'apache',
				'www':'nginx',
				'cassandra':'cassandra'}
		return services[service_name] if services.has_key(service_name) else service_name
 			
	def _get_version(self):
		pass

class Options:
	
	_options = None
	def __init__(self, *args):
		
		self._options = args
		
		for optspec in args:
			setattr(self, optspec.name, optspec)	
			
	def __iter__(self):
		return self._options.__iter__()


class _OptionSpec():
	name = None
	section = None
	default_value = None
	supported_from = None
	need_restart = None
	inaccurate = None
	extension = None
	
	def __init__(self, name, section, default_value=None, supported_from=None, 
				need_restart=False, inaccurate=False, **extension):
		self.name = name
		self.section = section
		self.default_value = default_value
		self.supported_from = supported_from
		self.need_restart = need_restart
		self.inaccurate = inaccurate
		self.extension = extension
			
	__ini_mapping = {
		'section':'config-section',
		'default_value':'default-value',
		'supported_from':'supported-from',
		'need_restart':'need-restart',
		'inaccurate':'inaccurate'
	}
			
	@staticmethod
	def from_ini(ini, section, defaults=None):
		ret = _OptionSpec(section)
		
		ini_pairs = dict(ini.items(section))
		defaults = defaults or _OptionSpec(None, None)	
			
		ret.section = ini_pairs.get('config-section', defaults.get('config-section', None))
		ret.default_value = ini_pairs.get('default-value', defaults.default_value)
		#ret.supported_from = tuple(map(int, ini_pairs['supported-from'].split('.')))


			
	def __repr__(self):
		return '%s (section: %s, default_value: %s)' % (self.name, self.section, self.default_value)
		
	
class _CnfManifest:
	_options = None
	_defaults = None
		
	def __init__(self, manifest_path):
		self._options = []
		ini = Configuration('ini')
		ini.read(manifest_path)
		try:
			self._defaults = dict(ini.items('__defaults__'))
		except PathNotExistsError:
			self._defaults = None
		
		for name in ini.sections("./"):
			self._options.append(_OptionSpec.from_ini(ini, name, self._defaults))
			
		'''
		params = {
			'section':'config-section',
			'default_value':'default-value',
			'supported_from':'supported-from',
			'need_restart':'need-restart',
			'inaccurate':'inaccurate'
		}
		
		for name in variables:
			if name == default_section:
				continue
			
			specs = {}
			
			for param, manifest_param in params.items():
				
				specs[param] = None
				
				try:
					specs[param] = ini.get('./' +name+ '/' + manifest_param)
				except:

					try:
						specs[param] = ini.get('./' + default_section + '/' + manifest_param)
					except:
						pass
					
				if type(specs[param]) == type(""):
					if specs[param].startswith('"'):
						specs[param] = specs[param][1:]
					if specs[param].endswith('"'):
						specs[param] = specs[param][:-1]
			
			#conversions
			if specs['section']	== '""':
				specs['section'] = None
			
			if specs['supported_from']:
				specs['supported_from'] = tuple(map(int,specs['supported_from'].split('.')))
				
			if specs['need_restart']:
				specs['need_restart'] = False if '0' == specs['need_restart'] else True
				
			if not specs['inaccurate']:
				specs['inaccurate'] = False
						
			self._options.append(_OptionSpec(name,**specs))
		'''
		
	def __iter__(self):
		return self._options.__iter__()			
