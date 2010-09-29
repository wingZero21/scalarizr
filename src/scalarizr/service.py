'''
Created on Sep 7, 2010

@author: marat
'''
from scalarizr.bus import bus
from scalarizr.libs.metaconf import Configuration, NoPathError
from scalarizr.util.filetool import read_file, write_file
import os
import time
import logging
import urllib2


class CnfPreset:
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
		@raise OSError: When cannot read preset file
		@raise MetaconfError: When experience problems with preset file parsing
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
		@raise ValueError: When `preset` is not an instance of CnfPreset
		@raise OSError: When cannot save preset file
		'''
		if not isinstance(preset, CnfPreset):
			raise ValueError('argument `preset` should be a CnfPreset instance, %s is given', type(preset))
		
		self._logger.debug('Saving preset as %s' % preset_type)
		ini = Configuration('ini')
		ini.add('general')
		ini.add('general/name', preset.name if (hasattr(preset, 'name') and preset.name) else 'Noname')
		ini.add('settings')

		for k, v in preset.settings.items():
			ini.add('settings/%s' % k, v)
		ini.write(open(self._filename(service_name, preset_type), 'w'))
		
		
class CnfController(object):
	behaviour = None

	_config_path = None
	_config_format = None
	
	def __init__(self, behaviour, config_path, config_format):
		self._logger = logging.getLogger(__name__)
		self.behaviour = behaviour
		self._config_path = config_path
		self._config_format = config_format

	def current_preset(self):
		self._logger.debug('Getting %s current configuration preset', self.behaviour)
		preset = CnfPreset(name='current', behaviour = self.behaviour)
		
		conf = Configuration(self._config_format)
		conf.read(self._config_path)
		
		vars = {}
		for opt in self._manifest:
			try:
				vars[opt.name] = conf.get(opt.name)
			except NoPathError:
				#self._logger.debug('%s does not exist in %s. Using default value' 
				#		%(option_spec.name, self._config))
				pass

				if opt.default_value:
					vars[opt.name] = opt.default_value
				else:
					self._logger.debug("Option '%s' has no default value" % opt.name)
				
		preset.settings = vars
		return preset

	def apply_preset(self, preset):
		self._logger.debug('Applying %s preset' % (preset.name if preset.name else 'undefined',))
		
		conf = Configuration(self._get_config_type(self.behaviour))
		conf.read(self._config_path)
		
		self._before_apply_preset()
		
		ver = self._software_version
		for opt in self._manifest:
			path = opt.name if not opt.section else '%s/%s' % (opt.section, opt.name)
			
			if opt.name in preset.settings:
				new_value = preset.settings[opt.name]
				
				# Skip unsupported
				if ver and opt.supported_from and opt.supported_from > ver:
					self._logger.debug("Skip option '%s'. Supported from %s; installed %s" % 
							(opt.name, opt.supported_from, ver))
					continue
								
				if not opt.default_value:
					self._logger.debug("Option '%s' has no default value" % opt.name)
					
				elif new_value == opt.default_value:
					self._logger.debug("Remove option '%s'. Equal to default" % opt.name)						
					conf.remove(path)
					self._after_remove_option(opt)				
					continue	

				if conf.get(path) == new_value:
					self._logger.debug("Skip option '%s'. Not changed" % opt.name)
				else:
					self._logger.debug("Set option '%s' = '%s'" % (opt.name, new_value))
					conf.set(path, new_value, force=True)
					self._after_set_option(opt, path)
			else:
				self._logger.debug("Remove option '%s'. Not found in preset" % opt.name)					
				conf.remove(path)
				self._after_remove_option(opt)
				
		self._after_apply_preset()						

		conf.write(open(self._config_path, 'w'))	
	
	def _after_set_option(self, option_spec, value):
		pass
	
	def _after_remove_option(self, option_spec):
		pass
	
	def _before_apply_preset(self):
		pass
	
	def _after_apply_preset(self):
		pass
	
	@property
	def _manifest(self):		
		manifest_url = bus.scalr_url + '/storage/service-configuration-manifests/%s.ini' % self.behaviour		
		manifests_dir = self.presets_path + "/manifests"
		path = os.path.join(manifests_dir, self.behaviour + '.ini')
		
		if not os.path.exists(manifests_dir):
			os.makedirs(manifests_dir)
			
		req = urllib2.Request(manifest_url)
		url_handle = urllib2.urlopen(req)
		headers = url_handle.info()
		url_last_modified = headers.getdate("Last-Modified")
		
		file_modified = tuple(time.localtime(os.path.getmtime(path))) if os.path.exists(path) else None
		
		if not file_modified or url_last_modified > file_modified:
			response = urllib2.urlopen(manifest_url)
			data = response.read()
			if data:
				write_file(path, data, logger=self._logger)
		
		return _CnfManifest(path)
	
	@property
	def _software_version(self):
		'''
		Override is subclass
		'''
		pass
	
	'''
	Move into <Service>CnfController
	def _get_config_type(self, service_name):
		services = {'mysql':'mysql',
				'app':'apache',
				'www':'nginx',
				'cassandra':'xml'}
		return services[service_name] if services.has_key(service_name) else service_name
	'''


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
				need_restart=True, inaccurate=False, **extension):
		self.name = name
		self.section = section
		self.default_value = default_value
		self.supported_from = supported_from
		self.need_restart = need_restart
		self.inaccurate = inaccurate
		self.extension = extension or dict()
			
	@staticmethod
	def from_ini(ini, section, defaults=None):
		ret = _OptionSpec(section)
		
		spec = dict(ini.items(section))
		defaults = defaults or dict()
			
		for key, value in spec:
			if 'config-section' == key:
				ret.section = spec.get(key, defaults.get(key, None))
			elif 'default-value' == key:
				ret.default_value = spec.get(key, defaults.get(key, None))
			elif 'supported-from' == key:
				tmp = spec.get(key, defaults.get(key, None))
				ret.supported_from = tmp and tuple(map(int, tmp.split('.'))) or None
			elif 'need-restart' == key:
				ret.need_restart = bool(spec.get(key, defaults.get(key, True)))
			elif 'inaccurate' == key:
				ret.inaccurate = bool(spec.get(key, defaults.get(key, False)))
			else:
				ret.extension[key] = value
				
		return ret
			
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
		except NoPathError:
			self._defaults = dict()
		
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
