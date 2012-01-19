
import os, sys
import logging
import signal, csv, cStringIO, socket
import string
import re

from scalarizr import util
from scalarizr.util import initdv2
from scalarizr.util import filetool
from scalarizr.libs import metaconf


BEHAVIOUR = SERVICE_NAME = 'haproxy'
LOG = logging.getLogger(__name__)
HAPROXY_EXEC = '/usr/sbin/haproxy'
HAPROXY_CFG_PATH = '/etc/haproxy/haproxy.cfg'

class HAProxyError(Exception):
	pass


class HAProxyCfg2(object):
	class slice_(dict):
		def __init__(self, conf, xpath):
			dict.__init__(self)
			self.conf = conf
			self.xpath = xpath
			self.name = os.path.basename(xpath)
			#current_section for path ./listen/server/app app in server section
			self.current_section = ''

		def __contains__(self, name):
			raise NotImplemented()
		'''
		def __len__(self):
			#raise NotImplemented()
			#LOG.debug('__len__ xpath: %s; length: %s; self: %s', self.xpath, self.conf.get_dict(self.xpath).__len__(), type(self))
			#return self.conf.get_dict(self.xpath).__len__()'''

		def __getitem__(self, name):
			raise NotImplemented()

		def __setitem__(self, name, value):
			LOG.debug('slice_.__setitem__: path: name: `%s`, value: `%s`, xpath: `%s`' % (name, value, self.xpath))
			
			if isinstance(value, str):
				try:
					self.conf.get('%s/%s'%(self.xpath, name))
					self.conf.set('%s/%s'%(self.xpath, name), value)
				except:
					self.conf.add('%s/%s'%(self.xpath, name), value)
				LOG.debug('name: `%s`, value: `%s`, xpath: `%s`'% (name, value, self.xpath))
			else:
				LOG.debug('slice.__setitem__: value dict %s', value)
					
				index = ''
				list_ = self.conf.get_list(self.xpath)
				for el in range(1, len(list_)):
					if list_[el-1] == name:
						index = el
						break
				
				#temp_ = self.xpath.split('/')
				_section = None
				for el in HAProxyCfg2.option_group.NAMES.keys():
					if el in self.xpath:
						_section = el
						break
				
				LOG.debug('xpath: `%s`, section: `%s`', self.xpath, _section)
				
				var =  _serializers[_section].serialize(value)
				
				LOG.debug('var: `%s`', '%s %s' % (name, var))
					
				if isinstance(index, int):
					LOG.debug('slice_.__setitem__: el set in: `%s`, var: %s', self._child_xpath(index), var)
					self.conf.set(self._child_xpath(index), '%s %s'%(name, var))
				else:
					LOG.debug('slice_.__setitem__: el add in: `%s`, var: %s', self.xpath, var)
					self.conf.add(self.xpath, '%s %s'%(name, var))
				'''
				else:
					if isinstance(value, dict):
						for key in value:
							
							index = ''
							list_ = self.conf.get_list(self.xpath)
							for el in range(1, len(list_)):
								if list_[el] == name:
									index = el
									break
							
							LOG.debug('!!! xpath: `%s`, section: `%s`', self.xpath, _section)
							
							var =  _serializers[name].serialize(value)
							
							LOG.debug('!!!           var: `%s`', '%s %s' % (name, var))
							
							if isinstance(index, int):
								LOG.debug('!option_group.__setitem__: el set in: `%s`, var: %s', self._child_xpath(index), _serializers[name].serialize(value))
								self.conf.set(self._child_xpath(index), '%s %s'%(name, var))
							else:
								LOG.debug('!option_group.__setitem__: el set in: `%s`, var: %s', self.xpath, _serializers[name].serialize(value))
								self.conf.add(self.xpath, '%s %s'%(name, var))		
				'''
				
			#	LOG.debug('slice.__setitem__: %s value: %s', type(value), value)
			'''
			if isinstance(value, str):
				try:
					self.conf.set(self._child_xpath(name), value)
				except:
					self.conf.add(self._child_xpath(name), value)
				LOG.debug('%s added in %s', value, self._child_xpath(name))
			elif isinstance(value, dict):
								
				list_ = self.conf.get_list(self._child_xpath(name))
				index = None
				for index in range(0, len(list_)-1):
					if name == list_[index]:
						break

				var = _serializers[self.current_section or name].serialize(value)
				LOG.debug('var: %s; xpath: %s'%(' '.join([name, var]), self.xpath))
				if not index:
					self.conf.add(self.xpath, ' '.join([name, var]))
				else:
					self.conf.set(self._child_xpath(index), ' '.join([name, var]))
			else:	
				var = _serializers[self.current_section or name].serialize(value)
				try:
					self.conf.set(self._child_xpath(name), var)
				except:
					self.conf.add(self._child_xpath(name), var)
				LOG.debug('var: `%s` added at path: `%s`' % (var, self._child_xpath(name)))
			'''
			
		def __iter__(self):
			index = 1
			try:
				while True:
					#LOG.debug('in slice_.__iter__  %s; index: %s; self: %s', self.conf.get(self._child_xpath(index)), index, type(self))
					yield self.conf.get(self._child_xpath(index))
					index += 1
			except metaconf.NoPathError:
				raise StopIteration()

		def _child_xpath(self, key):
			if isinstance(key, int):
				return '%s[%d]' % (self.xpath, key)
			return '%s/%s' % (self.xpath, key)

		def _len_xpath(self):
			return len(filter(None, self.xpath.replace('.', '').split('/')))

	class option_group(slice_):
		NAMES = {'server':1, 'option':1, 'timeout':1, 'log':0, 'stats':1}

		def __init__(self, conf, xpath):
			HAProxyCfg2.slice_.__init__(self, conf, xpath)
			self.current_section = ''
			for elem in self.NAMES.keys():
				if elem in self.xpath:
					self.current_section = elem

		def __getitem__(self, name):
			index = 1
			for val in self:
				if isinstance(val, str):
					if val.startswith(name + ' ')  or val == name:
						LOG.debug('str. self.conf.get(self._child_xpath(index): %s', self.conf.get(self._child_xpath(index)))
						return _serializers[self.current_section or name].unserialize(self.conf.get(self._child_xpath(index))[len(name):])
				elif isinstance(val, dict):
					if val.get(name):
						return val.get(name)
					else:
						continue
				elif isinstance(val, list):
					if val[0] == name:
						return _serializers[self.current_section].unserialize(val[1:])
				else:
					LOG.debug('Other type in option_group.__getitem__(%s) %s', name, type(val))
				index += 1
			raise KeyError(name)

		def __contains__(self, name):
			name_ = name + ' '
			for val in self:
				if val.startswith(name_):
					return True
			return False

		def __iter__(self):
			index = 1
			try:
				while True:
					#LOG.debug('type `%s`, self.xpath: `%s`, xpath[index]: `%s`', type(_serializers[self.current_section].unserialize(self.conf.get(self._child_xpath(index)))), self.xpath, self._child_xpath(index))
					yield _serializers[self.current_section].unserialize(self.conf.get(self._child_xpath(index)))
					index += 1
			except metaconf.NoPathError:
				raise StopIteration()
		
		def __setitem__(self, name, value):
			LOG.debug('option_group.__setitem__ path: name: `%s`, value: `%s`, xpath: `%s`' % (name, value, self.xpath))
			
			_section = None
			for el in self.NAMES.keys():
				if el in self.xpath:
					_section = el
					break
			
			ind = ''	
			if _section == name:
				for key in value.keys():
					if isinstance(value[key], dict):
						slice_ = HAProxyCfg2.slice_(self.conf, self._child_xpath(name))
						HAProxyCfg2.slice_.__setitem__(slice_, key, value[key])
			else:
				flag = True
				if isinstance(value, dict):
					for key in value.keys():
						if isinstance(value[key], dict):
							flag = False
							slice_ = HAProxyCfg2.slice_(self.conf, self._child_xpath(name))
							HAProxyCfg2.slice_.__setitem__(slice_, key, value[key])
						else:
							LOG.debug('key %s value %s, path %s', key, value[key], self.xpath)
							LOG.debug('key_list %s', self.conf.get_list(self.xpath))

							index = 1
							for el in self.conf.get_list(self.xpath):
								if el.startswith(name):
									ind = index
									break
								index += 1
					if isinstance(ind, int):
						LOG.debug('ind: %s', ind)
						var = _serializers[_section].serialize(value)
						LOG.debug('at path: `%s` set value: `%s`', self._child_xpath(ind), '%s %s' % (name, var))
						self.conf.set(self._child_xpath(ind), '%s %s' % (name, var))
					elif flag and self._len_xpath() > 1:
						var = _serializers[_section].serialize(value)
						LOG.debug('at path: `%s` add value: `%s`', self._child_xpath(ind), '%s %s' % (name, var))
						self.conf.add(self.xpath, '%s %s' % (name, var))
				else:
					var = _serializers[_section].serialize(value)
					index = ''
					list_ = self.conf.get_list(self.xpath)
					for el in range(1, len(list_)):
						if list_[el-1] == name:
							index = el
							break
					if isinstance(index, int):
						LOG.debug('at path: `%s` set value: `%s`', self._child_xpath(index), '%s %s' % (name, var))
						self.conf.set(self._child_xpath(index), '%s %s' % (name, var))
					else:
						LOG.debug('at path: `%s` add  value: `%s`', self.xpath, '%s %s' % (name, var))
						self.conf.add(self.xpath, '%s %s' % (name, var))

	class section(slice_):
		def __getitem__(self, name):
			#self.current_section = name
			if name in HAProxyCfg2.option_group.NAMES.keys():
				return HAProxyCfg2.option_group(self.conf, self._child_xpath(name))
			try:
				LOG.debug(self._child_xpath(name))
				return _serializers[name].unserialize(self.conf.get(self._child_xpath(name)))
			except metaconf.NoPathError:
				raise KeyError(name)

		def __contains__(self, name):
			return name in self.conf.options(self.xpath)

		def __len__(self):
			return len(self.conf.options(self.xpath))

		def __setitem__(self, name, value):
			LOG.debug('section.__setitem__ path: name: `%s`, value: `%s`, xpath: `%s`' % (name, value, self.xpath))
			sections_ = HAProxyCfg2.sections(self.conf, self.xpath)
			HAProxyCfg2.sections.__setitem__(sections_, name, value)
			

	class sections(slice_):

		def __len__(self):
			return sum(int(t == self.name) for t in self.conf.sections('./'))

		def __contains__(self, name):
			return name in self.conf.sections('./')

		def __getitem__(self, name):
			#self.current_section = name
			for index in range(0, len(self)):
				if self.conf.get(self._child_xpath(index)) == name:
					#LOG.debug('%s %s'% self._child_xpath(name), )
					return HAProxyCfg2.section(self.conf, self._child_xpath(index))
			raise KeyError(self._child_xpath(name))

		def __setitem__(self, name, value):
			LOG.debug('sections.__setitem__ path: name: `%s`, value: `%s`, xpath: `%s`' % (name, value, self.xpath))

			index = ''
			sections_ = self.conf.get_list(self.xpath)
			LOG.debug('section_ `%s`, len %s', sections_, sections_.__len__())
			if sections_.__len__() != 0:
				for el in range(1, len(sections_)):
					if self.conf.get(self._child_xpath(el)) == name:
						index = el
						break
				#TODO: need to fix index verification if it not define
				if not isinstance(index, int):
					self.conf.add(self.xpath, name)
					sections_ = self.conf.get_list(self.xpath)
					index = len(sections_)
					LOG.debug('\n%s  path %s\n ', self.conf.get(self._child_xpath(index)), self._child_xpath(index))
					if self.conf.get(self._child_xpath(index)) != name:
						index = ''
			else:
				index = 1
				self.conf.add(self.xpath, name)
				LOG.debug('sections.__setitem__: path %s added into conf'%self._child_xpath(name))

			if isinstance(index, int):
				self.xpath = self._child_xpath(index)
				
			LOG.debug('-> xpath: %s name %s', self.xpath, name)

			if isinstance(value, str):
				LOG.debug('sections.__setitem__: name `%s`, xpath `%s`, value: `%s`', name, self.xpath, value)
				self.conf.add(self._child_xpath(name), value)
			elif isinstance(value, dict):
				for key in value.keys():
					if key in HAProxyCfg2.option_group.NAMES.keys():
						LOG.debug('!    sections.__setitem__ -> go to option_group')
						#self.current_section = key
						option_group_ = HAProxyCfg2.option_group(self.conf, self.xpath)
						HAProxyCfg2.option_group.__setitem__(option_group_, key, value[key])
					else:
						LOG.debug('    sections.__setitem__ -> go to slice_ key `%s`, value[key] `%s`, xpath: `%s`, name: `%s`'%(key, value[key], self.xpath, name))

						if name in HAProxyCfg2.option_group.NAMES.keys():
							slice_ = HAProxyCfg2.slice_(self.conf, self._child_xpath(name))
							HAProxyCfg2.slice_.__setitem__(slice_, key, value[key])
						else:
							HAProxyCfg2.slice_.__setitem__(self, key, value[key])
			else:
				LOG.debug('sections.__setitem__: value `%s`', value)
				self.conf.add(self.xpath, _serializers[name].serialize(value))
			'''
			try: 
				self.conf.get('%s/%s'%(self.xpath, name))
			except:
				self.conf.add(self.xpath, name)
				LOG.debug('sections.__setitem__: path %s added into conf'%self._child_xpath(name))
				
			sections_ = self.conf.get_list(self.xpath)
			index = 0
			for el in range(0, len(sections_)):
				if sections_[el] == name:
					index = el
					break

			if isinstance(value, str):
				self.conf.add(self._child_xpath(name), value)
			elif isinstance(value, dict):
				for key in value.keys():
					var = _serializers[key].serialize(value[key])
					self.conf.add('%s/%s' % (self._child_xpath(index), key), var)
			else:
				var = _serializers[name].serialize(value)
				LOG.debug('Try add at path: `%s/%s` option: `%s`; value: `%s`;' % (self._child_xpath(index), name, key, var))
				self.conf.add('%s/%s' % (self._child_xpath(index), key), var)
			'''


	def __init__(self, path=None):
		self.conf = metaconf.Configuration('haproxy')
		self.conf.read(path or HAPROXY_CFG_PATH)

	def __getitem__(self, name):
		cls = self.sections
		if name in ('global', 'defaults'):
			cls = self.section
		return cls(self.conf, './' + name)

	def __setitem__(self, name, value):
		LOG.debug('HAProxyCfg.__setitem__ name = %s; value = %s.'%( name, value))
		
		if not self.conf.children('./'+name):
			self.conf.add('./%s' % name)

		LOG.debug('HAProxyCfg.__setitem__ name = %s; value = %s.'%( name, value))

		if isinstance(value, str):
			LOG.debug('value: `%s`; name: `%s`' % (value, name))
			try:
				self.conf.set('./%s'%(name), value)
			except:
				self.conf.add('./%s'%(name), value)
		elif isinstance(value, dict):
			for key in value.keys():
				var = _serializers[key].serialize(value[key])
				LOG.debug('key: `%s`; var: `%s`; name: `%s`' % (key, var, name))
				self.conf.add('./%s/%s' % (name, key), var)


class OptionSerializer(object):
	def unserialize(self, s):
		value = filter(None, map(string.strip, s.replace('\t', ' ').split(' '))) if isinstance(s, str) else s
		if len(value) == 0:
			return True
		elif len(value) == 1:
			return value[0]
		return value

	def serialize(self, v):
		if isinstance(v, list):
			return ' '.join(v)
		elif isinstance(v, dict):
			res = ''
			for key in v.keys():
				if isinstance(v[key], str):
					res += ' %s %s' % (key, v[key])
				elif isinstance(v[key], bool):
					res += ' ' + key
				else:
					res += ' %s %s' % (key, self.serialize(v[key]))
			return res
		elif isinstance(v, bool):
			return ''
		else:
			return v

	def _parse(self, list_par, input_str):
		temp = {}
		last_key = ''
		count = 0
		if not isinstance(list_par, list):
			list_par = [list_par,]
		for elem in list_par:
			if not last_key:
				el_in_list= False
				for num_arg in self._number_args.keys():
					if elem in self._number_args[num_arg]:
						el_in_list =True
						if num_arg > 0:
							last_key = elem
							count = num_arg
							temp[last_key] = []
						else:
							temp[elem] = True
				if not el_in_list:
					raise "Can't parse string in %s._parse input str = `%s`" %(type(self), input_str)

			elif count > 0:
				count -= 1
				temp[last_key].append(elem)

				if count == 0:
					if temp[last_key].__len__() == 1:
						temp[last_key] = temp[last_key][0]
					elif temp[last_key].__len__() == 0:
						temp[last_key] = True
					last_key = ''
		return temp


class ServerSerializer(OptionSerializer):

	def __init__(self):
		self._number_args = {
			0:['backup', 'check', 'disabled'], 
			1:['addr', 'cookie', 'error-limit', 'fall', 'id', 'inter',
				'fastinter', 'downinter', 'maxconn', 'maxqueue', 'minconn',
				'observe', 'on-error', 'port', 'redir', 'rise', 'slowstart',
				'source', 'track', 'weight']}

	def unserialize(self, s):
		try:
			list_par = OptionSerializer.unserialize(self, s)
			temp = {}
			name = list_par.pop(0)
			temp['address'] = list_par.pop(0)
			if ':' in temp['address']:
				temp['address'], temp['port'] = temp['address'].split(':')
			temp.update(self._parse(list_par, s))
			return {name:temp} if isinstance(temp, dict) else s
		except:
			LOG.debug("Details: %s%s", sys.exc_info()[1], sys.exc_info()[2])
			return OptionSerializer.unserialize(self, s)

	def serialize(self, d):
		LOG.debug('ServerSerializer.serialize: input `%s`', d)
		res = []
		if isinstance(d, dict):
			if d.get('address'):
				res.append('%s%s' % (d['address'], ':' + d.get('port') if d.get('port') else ''))
				del d['address']
				if d.get('port'):
					del d['port']

			for key in d.keys():
				if isinstance(d[key], bool):
					res.append(key)
				else:
					LOG.debug('d[key]: `%s`', d[key])
					res.append(' '.join([key, d[key]]))
			LOG.debug('res: `%s`, res_str: `%s`'%(res, ' '.join(res)))
			return ' '.join(res)
		else:
			#LOG.debug('res: `%s`, res_str: %s'%(res, ' '.join(res)))
			return ' '.join(d)


class StatsSerializer(OptionSerializer):

	def __init__(self):
		self._number_args = {
			0:['enable'],
			1:['socket', 'timeout', 'maxconn', 'uid', 'user', 'gid', 'group', 'mode', 'level'], 
			2:['admin']}
	'''
	def unserialize(self, s):
		try:
			LOG.debug('inp unserialize `%s`', s)
			list_par = OptionSerializer.unserialize(self, s)
			LOG.debug('list_par unserialize `%s`', list_par)
			temp = {}
			
			if not isinstance(list_par, list):
				temp = list_par
			else:
				name = list_par.get(0)
				temp.update(self._parse(list_par, s))
			
			if isinstance(temp, dict):
				return {name : temp}
			return temp
		except:
			LOG.debug("Details: %s%s", sys.exc_info()[1], sys.exc_info()[2])
			return OptionSerializer.unserialize(self, s)'''


class Serializers(dict):
	def __init__(self, **kwds):
		dict.__init__(self, **kwds)
		self.update({
			'server': ServerSerializer(),
			'stats': StatsSerializer()
		})
		self.__default = OptionSerializer()

	def __getitem__(self, option):
		return self.get(option, self.__default)

_serializers = Serializers()









class BaseOption():
	def __init__(self, cls, mpath):
		self.haproxy_cfg = cls
		self.mpath = mpath
	
	def all(self):
		return self.__getitem__('')
	
	def flash(self):
		#write into haproxy.cfg
		pass


class Server(BaseOption):

	def __getitem__(self, key):
		res = []

		servers = self.haproxy_cfg.get_dict('%sserver'% self.mpath['value'])
		
		for elem in servers:
			list_par = elem['value'].split(' ')
			
			_required = ['name', 'address']
			_single = ['backup', 'check', 'disabled']
			'''
			most of the params have value as argument so we can checking only single parametrs
			_pair = ['addr', 'cookie', 'error-limit', 'fall', 'id', 'inter',
				'fastinter', 'downinter', 'maxconn', 'maxqueue', 'minconn',
				'observe', 'on-error', 'port', 'redir', 'rise', 'slowstart',
				'source', 'track', 'weight']
			'''
			temp = {}
			for item in _required:
				temp[item] = list_par.pop(0)
			if ':' in temp['address']:
				temp['address'], temp['port'] = temp['address'].split(':')
			i=0
			while i < list_par.__len__():
				if list_par[i] in _single:
					temp[list_par[i]] = True
					i+=1
				else:
					temp[list_par[i]] = list_par[i+1]
					i+=2
			res.append(temp)
		return res

	def __setitem__(self):
		pass



class Option(BaseOption):

	def __getitem__(self, key):
		options = self.haproxy_cfg.get_dict('%soption'% self.mpath['value'])
		res = {}
		
		for elem in options:
			list_par = elem['value'].split(' ')
			
			if list_par.__len__()>1:
				res[list_par[0]] = ' '.join(list_par[1:]) 
			else:
				res[list_par[0]] = True
		return res


class Stats(BaseOption):

	def __getitem__(self, key):
		stats = self.haproxy_cfg.get_dict('%sstats'% self.mpath['value'])

		_pair = ['socket', 'timeout', 'maxconn', 'uid', 'user', 'gid', 'group', 'mode', 'level']
		_single = ['enable']
		res = []

		for elem in stats:
			temp = {}
			list_par = elem['value'].split(' ')

			i = 0
			while i < list_par.__len__():
				if list_par[i] in _pair:
					temp[list_par[i]] = list_par[i+1]
					i+=2
				else:
					temp[list_par[i]] = True
					i+=1
			res.append(temp)
		return res if res.__len__()>1 else res[0]


class HAProxyCfg(filetool.ConfigurationFile):
	'''
	haproxy.cfg configuration object.
	- access configuration in a dict-like way 
	- wrap all setting in standart python types

	Create object:
	>> cfg = HaProxyCfg()

	Access settings:
	>> cfg['global']['stats']['socket']
   	/var/run/haproxy-stats.sock

    	>> cfg['backend']['scalr:backend:role:456:port:8080']['server']
  	[{'name': 'srv0', 'address': '10.156.7.18', 'port': 1234, 'check': True},
	{'name': 'srv1', 'address': '10.156.26.59', 'port': 1234, 'check': True}]

	>> cfg['listen']['scalr:listener:tcp:12345']['option']
	{'forwardfor': True, 'httpchk': True}

	Modify settings:
	>> cfg['listen']['scalr:listener:tcp:12345'] = {
	'bind': '*:12345', 
	'mode': 'tcp',
	'balance': 'roundrobin',
	'option': {'tcplog': True},
	'default_backend': 'scalr:backend:port:1234'
	}

	Find sections:
	>> cfg.sections('scalr:backend')
	['scalr:backend:port:1234', 'scalr:backend:role:456:port:8080']
	'''

	_path = {}

	_sections_without_names = ('global', 'defaults')

	def __init__(self, path=None):
		try:
			self._config = metaconf.Configuration('haproxy')
			self._config.read(path or HAPROXY_CFG_PATH) 
			#TODO: copy from _config to dict config
		except:
			raise HAProxyError, 'Can\'t read HAProxy configuration file.' \
				' Details: %s' % str(sys.exc_info()[1]), sys.exc_info()[2]

		self.options = {'server': Server, 'option': Option, 'stats': Stats}

		super(HAProxyCfg, self).__init__(path or HAPROXY_CFG_PATH)

	def sections(self, filter=None):
		self._config.get()
		raise NotImplemented()

	def __getitem__(self, key):
		'''
        sections = list(conf.children("./defaults"))
        keys = conf.children("./listen[@value='ssl-relay 0.0.0.0:8443']/")
        values = conf.get_dict("./listen[@value='ssl-relay 0.0.0.0:8443']/")
        
        >> cfg['global']['stats']['socket']
        /var/run/haproxy-stats.sock

        >> cfg['backend']['scalr:backend:role:456:port:8080']['server']
        [{'name': 'srv0', 'address': '10.156.7.18', 'port': 1234, 'check': True},
        {'name': 'srv1', 'address': '10.156.26.59', 'port': 1234, 'check': True}]
    
        >> cfg['listen']['scalr:listener:tcp:12345']['option']
        {'forwardfor': True, 'httpchk': True}
		'''

		try:
			if key in self._config.children("./"):
				self._path.clear()
				self._path['section'] = key
				return self

			if self._path.get('section') in self._sections_without_names:
				self._path['without_sname'] = 1
			elif 'without_sname' in self._path:
				self._path.__delitem__('without_sname')

			if self._path.get('section') and not self._path.get('section_sname') and not self._path.get('without_sname'):
				res=[]
				sections_names = self._config.get_dict("./%s"%self._path.get('section'))
				for name in sections_names:
					res.append(name['value'])

				if key in res:
					self._path['section_name'] = key
					return self


			mpath = {}
			if self._path.get('section') and self._path.get('section_name'):
				mpath['key'] = "./%s[@value='%s']/"%(self._path['section'], self._path['section_name'])
				mpath['value'] = "./%s[@value='%s']/"%(self._path['section'], self._path['section_name'])
			elif self._path.get('without_sname') and self._path.get('section'):
				mpath['key'] = "./%s/" % self._path['section']
				mpath['value'] = "./%s/" % self._path['section']
			else:
				mpath = None

			if mpath and key in self.options:
				return self.options[key](self._config, mpath).all()

			elif mpath:
				keys = list(self._config.children(mpath['key']))
				values = list(self._config.get_dict(mpath['value']))

				i=0
				res=[]
				if keys.__len__() == values.__len__():
					for _key in keys:
						if key == _key:
							res.append(values[i]['value'])
						i+=1
					return res if res.__len__() > 1 else res[0]

			self._path.clear()
		except:
			raise Exception, 'Error details: %s' % sys.exc_info()[1], sys.exc_info()[2]
			#self.__getitem__(self, key)
		#self.local.path = ('backend', 'scalr:backend:role:456:port:8080', 'server')

	'''
    def __setitem__(self, key):
        if key in self.config:
            pass
	'''


class StatSocket(object):
	'''
    haproxy unix socket API
    - one-to-one naming
    - connect -> request -> disconnect

    Create object:
    >> ss = StatSocket('/var/run/haproxy-stats.sock')

    Show stat:
    >> ss.show_stat()
    [{'status': 'UP', 'lastchg': '68', 'weight': '1', 'slim': '', 'pid': '1', 'rate_lim': '', 
    'check_duration': '0', 'rate': '0', 'req_rate': '', 'check_status': 'L4OK', 'econ': '0', 
    'wredis': '0', 'dresp': '0', 'ereq': '', None: [''], 'tracked': '', 'pxname': 'scalr:backend:port:1234', 
    'dreq': '', 'hrsp_5xx': '', 'check_code': '', 'sid': '1', 'bout': '0', 'hrsp_1xx': '', 
    'qlimit': '', 'hrsp_other': '', 'bin': '0', 'smax': '0', 'req_tot': '', 'lbtot': '0', 
    'stot': '0', 'wretr': '0', 'req_rate_max': '', 'iid': '1', 'hrsp_4xx': '', 'chkfail': '0', 
    'hanafail': '0', 'downtime': '0', 'qcur': '0', 'eresp': '0', 'cli_abrt': '0', 'srv_abrt': '0', 
    'throttle': '', 'scur': '0', 'type': '2', 'bck': '0', 'qmax': '0', 'rate_max': '0', 'hrsp_2xx': '', 
    'act': '1', 'chkdown': '0', 'svname': 'srv0', 'hrsp_3xx': ''}]
	'''

	def __init__(self, address='/var/run/haproxy-stats.sock'):
		try:
			self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
			self.sock.connect(address)
			self.adress = address
		except:
			raise Exception, "Couldn't connect to socket on address: %s" % address, sys.exc_info()[2]


	def show_stat(self):
		'''
        @rtype: list[dict]
		'''
		try:
			self.sock.send('show stat\n')
			stat = self.sock.makefile('r').read()

			fieldnames = filter(None, stat[2:stat.index('\n')].split(','))
			reader = csv.DictReader(cStringIO.StringIO(stat[stat.index('\n'):]), fieldnames)
			res=[]
			for row in reader:
				res.append(row)
			return res
		except:
			raise Exception, "Error working with sockets. Details: %s" % sys.exc_info()[1],\
				sys.exc_info()[2]


def naming(type_, protocol=None, port=None, backend=None):
	ret = 'scalr:%s' % type_
	if type_ == 'backend' and backend:
		ret += ':%s' % backend
	if protocol:
		ret += ':%s' % protocol
	if port:
		ret += ':%s' % port
	return ret


class HAProxyInitScript(initdv2.InitScript):
	'''
    haproxy init script
    - start
    - stop
    - restart
    - reload
    - status
	'''

	def __init__(self):
		self.pid_file = '/var/run/haproxy.pid'
		self.config_path = HAPROXY_CFG_PATH
		self.haproxy_exec = '/usr/sbin/haproxy'
		self.socks = None
		self.timeout = 30


	def start(self):
		if self.status() == 0:
			raise initdv2.InitdError("Cannot start HAProxy. It already running.")

		util.system2([self.haproxy_exec, '-f', self.config_path, '-p', self.pid_file, '-D'],)
		if self.pid_file:
			try:
				util.wait_until(lambda: os.path.exists(self.pid_file), timeout=self.timeout,
						sleep=0.2, error_text="HAProxy pid file %s does'not exist"%
						self.pid_file)
			except:
				err = "Cannot start HAProxy: pid file %s hasn't been created. " \
					"Details: %s" % (self.pid_file, sys.exc_info()[1])
				raise initdv2.InitdError, err, sys.exc_info()[2]


	def stop(self):
		if os.path.exists(self.pid_file):
			try:
				pid = self.pid()
				if pid:
					os.kill(pid, signal.SIGTERM)
					util.wait_until(lambda: not os.path.exists('/proc/%s' % pid),
							timeout=self.timeout, sleep=0.2, error_text="Can't stop HAProxy")
					if os.path.exists('/proc/%s' % pid):
						os.kill(pid, signal.SIGKILL)
			except:
				err = "Error stopping service. Details: %s" % sys.exc_info()[1]
				raise initdv2.InitdError, err, sys.exc_info()[2]
			finally:
				os.remove(self.pid_file)


	def restart(self):
		try:
			self.stop()
		except:
			LOG.debug('Error stopping HAProxy. Details: %s%s'% (sys.exc_info()[1], sys.exc_info()[2]))
		self.start()


	def reload(self):
		try:
			if os.path.exists(self.pid_file):
				pid = self.pid()
				if pid:
					args = [self.haproxy_exec, '-f', self.config_path, '-p', self.pid_file, '-D', '-sf', pid]
					util.system2(args, close_fds=True, logger=LOG, preexec_fn=os.setsid)
					util.wait_until(lambda: self.pid() and self.pid() != pid,
						timeout=self.timeout, sleep=0.5, error_text="Error reloading HAProxy service process.")
					if self.status() != 0:
						raise initdv2.InitdError("HAProxy service not running.")
			else:
				raise LookupError('File %s not exist'%self.pid_file)
		except:
			raise initdv2.InitdError, "HAProxy service not running can't reload it."\
				" Details: %s" % sys.exc_info()[1], sys.exc_info()[2]


	def pid(self):
		'''Read #pid of the process from pid_file'''
		if os.path.isfile(self.pid_file):
			with open(self.pid_file, 'r') as fp:
				return long(fp.read())

initdv2.explore(SERVICE_NAME, HAProxyInitScript)