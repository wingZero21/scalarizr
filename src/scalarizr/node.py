from __future__ import with_statement


import re
import ConfigParser
import sys
try:
	import json
except ImportError:
	import simplejson as json 


class Store(dict):
	pass


class Compound(Store):
	def __init__(self, patterns=None):
		self.__re_map = {}
		self.__plain_map = {}
		patterns = patterns or {}
		for pattern, store in patterns.items():
			keys = pattern.split(',')
			for key in keys:
				if '*' in key:
					key = re.compile(r'^%s$' % key.replace('*', '.+'))
					self.__re_map[key] = store
				elif isinstance(store, Store):
					self.__plain_map[key] = store
				else:
					dict.__setitem__(self, key, store)


	def __setitem__(self, key, value):
		store = self.__find_store(key)
		if store:
			store.__setitem__(key, value)
		else:
			dict.__setitem__(self, key, value)


	def __getitem__(self, key):
		store = self.__find_store(key)
		if store:
			return store.__getitem__(key)
		else:
			return dict.__getitem__(self, key)


	def __find_store(self, key):
		if key in self.__plain_map:
			return self.__plain_map[key]
		else:
			for rkey, store in self.__re_map.items():
				if rkey.match(key):
					return store

		

class Json(Store):
	def __init__(self, filename, fn):
		'''
		Example:
		jstore = Json('/etc/scalr/private.d/storage/mysql.json', 
					'scalarizr.storage2.volume')
		'''
		self.filename = filename
		self.fn = fn

	def __getitem__(self, key):
		try:
			with open(self.filename, 'r') as fp:
				kwds = json.load(fp)
		except:
			raise KeyError(key)
		else:
			return self.fn(**kwds)


	def __setitem__(self, key, value):
		if hasattr(value, 'config'):
			value = value.config()
		with open(self.filename, 'w+') as fp:
			json.dump(value, fp)


class Ini(Store):
	def __init__(self, filename, section):
		self.filename = filename
		self.section = section
		self.ini = None


	def _reload(self):
		self.ini = ConfigParser.ConfigParser()
		self.ini.read(self.filename)


	def __getitem__(self, key):
		try:
			self._reload()
			return self.ini.get(self.section, key)
		except ConfigParser.Error:
			raise KeyError(key)


	def __setitem__(self, key, value):
		self._reload()
		if value is None:
			value = ''
		elif isinstance(value, bool):
			value = str(int(value))
		else:
			value = str(value)
		self.ini.set(self.section, key, value)
		with open(self.filename, 'w+') as fp:
			self.ini.write(fp)	


class IniOption(Ini):
	def __init__(self, filename, section, option, 
			getfilter=None, setfilter=None):
		self.option = option
		self.getfilter = getfilter
		self.setfilter = setfilter
		super(IniOption, self).__init__(filename, section)


	def __getitem__(self, key):
		value = super(IniOption, self).__getitem__(self, self.option)
		if self.getfilter:
			return self.getfilter(value)
		return value


	def __setitem__(self, key, value):
		if self.setfilter:
			value = self.setfilter(value)
		super(IniOption, self).__setitem__(self, self.option, value)


class File(Store):
	def __init__(self, filename):
		self.filename = filename


	def __getitem__(self, key):
		try:
			with open(self.filename) as fp:
				return fp.read().strip()
		except:
			raise KeyError(key)


	def __setitem__(self, key, value):
		with open(self.filename, 'w+') as fp:
			fp.write(str(value).strip())


class BoolFile(Store):
	def __init__(self, filename):
		self.filename = filename


	def __getitem__(self, key):
		return os.path.isfile(self.filename)


	def __setitem__(self, key, value):
		if value:
			open(self.filename, 'w+').close()
		else:
			if os.path.isfile(self.filename):
				os.remove(self.filename)


class State(Store):
	def __init__(self, key):
		self.key = key

	def __getitem__(self, key):
		from scalarizr.config import STATE
		return STATE[self.key]

	def __setitem__(self, key, value):
		from scalarizr.config import STATE
		STATE[self.key] = value


class Attr(Store):
	def __init__(self, module, attr):
		self.module = module
		self.attr = attr
		self.getter = None


	def __getitem__(self, key):
		try:
			if isisntance(self.module, basestring):
				__import__(self.module)
				self.module = sys.modules[self.module]
			if not self.getter:
				path = self.attr.split('.')
				base = self.module
				for name in path[:-1]:
					base = getattr(base, name)
				self.getter = lambda: getattr(base, path[-1])
		except:
			raise KeyError(key) 
		return self.getter()


class Call(Attr):
	def __getitem__(self, key):
		attr = Attr.__getitem__(self, key)
		return attr()	


_base_dir = '/etc/scalr'
_private_dir = _base_dir + '/private.d'
_public_dir = _base_dir + '/public.d'
_storage_dir = _private_dir + '/storage'

__node__ = {}
for behavior in ('mysql', 'mysql2', 'percona'):
	__node__[behavior] = Compound({
		'volume,volume_config': 
				Json('%s/%s.json' % (_private_dir, behavior), 
					'scalarizr.storage2.volume'),
		'*_password,log_*,replication_master': 
				Ini('%s/%s.ini' % (_private_dir, behavior), 
					behavior),
		'mysqldump_options': 
				Ini('%s/%s.ini' % (_public_dir, behavior), behavior)		
	})
__node__['ec2'] = Compound({
	't1micro_detached_ebs': State('ec2.t1micro_detached_ebs'),
	'hostname_as_pubdns': 
				Ini('%s/%s.ini' % (_public_dir, 'ec2'), 'ec2'),
	'ami_id': Call('scalarizr.bus', 'bus.platform.get_ami_id'),
	'kernel_id': Call('scalarizr.bus', 'bus.platform.get_kernel_id'),
	'ramdisk_id': Call('scalarizr.bus', 'bus.platform.get_ramdisk_id'),
	'instance_id': Call('scalarizr.bus', 'bus.platform.get_instance_id'),
	'instance_type': Call('scalarizr.bus', 'bus.platform.get_instance_type'),
	'avail_zone': Call('scalarizr.bus', 'bus.platform.get_avail_zone'),
	'region': Call('scalarizr.bus', 'bus.platform.get_region'),
	'connect_ec2': Attr('scalarizr.bus', 'bus.platform.new_ec2_conn'),
	'connect_s3': Attr('scalarizr.bus', 'bus.platform.new_s3_conn')
})
__node__['behavior'] = IniOption(
						_private_dir + '/config.ini', 'general', 'behaviour', 
						lambda val: val.strip().split(','),
						lambda val: ','.join(val))
__node__['platform,server_id'] = Ini(_public_dir + '/config.ini', 'general')
__node__['public_ip'] = Call('scalarizr.bus', 'bus.platform.get_public_ip'),
__node__['private_ip'] = Call('scalarizr.bus', 'bus.platform.get_private_ip'),
__node__['state'] = File(_private_dir + '/.state')
__node__['reboot'] = BoolFile(_private_dir + '/.reboot')
__node__['halt'] = BoolFile(_private_dir + '/.halt')
__node__ = Compound(__node__)


