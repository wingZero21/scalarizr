
import os, sys
import logging
import signal, csv, cStringIO, socket

from scalarizr import util
from scalarizr.util import initdv2
from scalarizr.util import filetool
from scalarizr.libs import metaconf

BEHAVIOUR = SERVICE_NAME = 'haproxy'
LOG = logging.getLogger(__name__)
HAPROXY_EXEC = '/usr/sbin/haproxy'
HAPROXY_CFG_PATH = '/etc/haproxy/haproxy.cfg'


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
			
			'''most of the params have value as argument so we can checking only single parametrs
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
		res = []

		options = self.haproxy_cfg.get_dict('%soption'% self.mpath['value'])
		res = {}
		
		for elem in options:
			list_par = elem['value'].split(' ')
			
			if list_par.__len__()>1:
				res[list_par[0]] = ' '.join(list_par[1:]) 
			else:
				res[list_par[0]] = True
			
		return res


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
			raise initdv2.InitdError, 'Cannot read/parse HAProxy main configuration file.'\
				' Details: %s' % sys.exc_info()[2]

		self.options = {'server': Server, 'option': Option}

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