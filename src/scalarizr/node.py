from __future__ import with_statement

__all__ = ['__node__']


import ConfigParser


class ConfigFile(object):
	def __init__(filename, default_section):
		self.filename = filename
		self.default_section = default_section
		self._reload()


	def _reload(self):
		self.ini = ConfigParser.ConfigParser()
		self.ini.read(self.filename)


	def __getitem__(self, key):
		try:
			self._reload()
			return self.ini.get(*self._parse_key(key))
		except ConfigParser.Error:
			raise KeyError(name)


	def __setitem__(self, key, value):
		self._reload()
		section, option = self._parse_key(key)
		self.ini.set(section, option, value)
		with open(self.filename, 'w') as fp:
			self.ini.write(fp)


	def _parse_key(self, key)
		if not '.' in key:
			key = self.default_section + '.' + key
		return key.split('.', 1)


class MySQLData(ConfigFile):
	def __getitem__(self, key):
		if key == 'type':
			if 'percona' in __node__['behavior']:
				return 'percona'
			return 'mysql'
		return super(MySQLData, self).__getitem__(key)


class Node(object):
	etc_base = '/etc/scalr'

	def __init__(self):
		self._config_files = {}
		self._ext = {}

	def __getitem__(self, key):
		if key in ('mysql', 'percona', 'postgresql', 'redis', 'mongodb'):
			if key == 'mysql' and 'mysql2' in self.behaviors:
				key = 'mysql2'
			return self._config_file(key)
		elif key in ('server_id', 'platform'):
			return self._config_file('config', False)['general.' + key]
		elif key == 'behavior':
			try:
				value = self._config_file('config')['general.behaviour']
			except KeyError:
				value = self._config_file('config', False)['general.behaviour']
			return value.strip().split(',')
		elif key in self._ext:
			return self._ext[key]
		else:
			raise KeyError(key)		


	def __setitem__(self, key, value):
		if key in ('server_id', 'platform', 'behavior'):
			if key == 'behavior':
				value = ','.join(value)
				key = 'behaviours'
			self._config_file('config')['general.' + key] = value
		else:
			self._ext[key] = value


	def _config_file(name, private=True)
		type_ = 'private' if private else 'public'
		key = name + '.' + type_
		if not key in self._config_files:
			filename = '%s/%s.d/%s.ini' % (self.etc_path, type_ name)
			cls = ConfigFile
			if name in ('mysql', 'percona'):
				cls = MySQLData
			self._config_files[key] = cls(filename)
		return self._config_files[key]


__node__ = _Node()
