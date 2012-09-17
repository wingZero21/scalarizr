
import types


class ConfigDriven(object):
	default_config = {}

	_config = None
	initial_config = None

	error_messages = {
		'empty_attr': 'Attribute should be specified: %s',
		'empty_param': 'Parameter should be specified: %s'
	}

	def __init__(self, **kwds):
		if not self._config:
			self._config = self.default_config.copy()
		self._config.update(kwds)
		self.initial_config = self._config.copy()		


	def config(self):
		return self._dictify(self._config.copy())


	def _dictify(self, data=None):
		if isinstance(data, dict):
			ret = {}
			for key in data:
				ret[key] = self._dictify(data[key])
			return ret
		elif isinstance(data, list):
			ret = [self._dictify(item) for item in data]
		elif type(data) in (str, unicode, bool, int, long, float, types.NoneType):
			ret = data
		else:
			ret = dict(data)
			
		return ret


	def __iter__(self):
		for key, value in self.config().items():
			yield (key, value)


	def __setattr__(self, name, value):
		data = self.__dict__ if name in dir(self) else self.__dict__['_config']
		data[name] = value
	
	
	def __getattr__(self, name):
		if name in self.__dict__['_config']:
			return self.__dict__['_config'][name]
		raise AttributeError(name)
	
	
	def __hasattr__(self, name):
		return name in self.__dict__['_config']

	def _check_attr(self, name):
		assert hasattr(self, name) and getattr(self, name),  \
				self.error_messages['empty_attr'] % name
