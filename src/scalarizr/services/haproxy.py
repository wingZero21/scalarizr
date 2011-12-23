'''
Created on Nov 25, 2011

@author: marat
@author: semen
'''
import os
import logging
import signal

from scalarizr import util
from scalarizr.util import initdv2
from scalarizr.util import filetool
import sys


BEHAVIOUR = SERVICE_NAME = 'haproxy'
LOG = logging.getLogger(__name__)
HAPROXY_EXEC = '/usr/sbin/haproxy'
HAPROXY_CFG_PATH = '/etc/haproxy/haproxy.cfg'


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
	
	DEFAULT = '/etc/haproxy/haproxy.cfg'
	
	def __init__(self, path=None):
		super(HAProxyCfg, self).__init__(path or self.DEFAULT)
	
	def sections(self, filter=None):
		raise NotImplemented()

	def __getitem__(self, key):
		self.local.path = ('backend', 'scalr:backend:role:456:port:8080', 'server')

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
	
	def __init__(self, address):
		pass
	
	def show_stat(self):
		'''
		@rtype: list[dict] 
		'''
		raise NotImplemented()


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
					util.wait_until(lambda: not os.path.exists('/proc/%s' % pid), timeout=self.timeout,
						sleep=0.2, error_text="Can't stop HAProxy service process.")
					if os.path.exists('/proc/%s' % pid):
						os.kill(pid, signal.SIGKILL)
			except Exception, e:
				raise initdv2.InitdError("Error stopping service. Details: %s" % (e))
			finally:
					os.remove(self.pid_file)


	def restart(self):
		try:
			self.stop()
		except Exception, e:
			LOG.debug('Error stopping HAProxy. Details: %s'%e)
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
		except Exception, e:
			raise initdv2.InitdError("HAProxy service not running can't reload it. Details: %s" % e)


	def pid(self):
		'''Read #pid of the process from pid_file'''
		if os.path.isfile(self.pid_file):
			with open(self.pid_file, 'r') as fp:
				return long(fp.read())

initdv2.explore(SERVICE_NAME, HAProxyInitScript)