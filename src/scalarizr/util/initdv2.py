'''
Created on Aug 29, 2010

@author: marat
@author: spike
'''
import socket
from scalarizr.util.initd import InitdError
import os
import time
from subprocess import Popen, PIPE


_services  = dict()
_instances = dict()


class InitdError(BaseException):
	GENERIC_ERR = 1
	INVALID_ARG = 2
	UNIMPLEMENTED = 3
	INSUFFICIENT_PRIVILEGE = 4
	NOT_INSTALLED = 5
	NOT_CONFIGURED = 6
	NOT_RUNNING = 7	

class Status:
	RUNNING = 0
	DEAD_PID_FILE_EXISTS = 1
	DEAD_VAR_LOCK_EXISTS = 2
	NOT_RUNNING = 3
	UNKNOWN = 4

class InitScript(object):
	name = None
	pid_file = None
	lock_file = None
	
	def start(self):
		'''
		@raise InitdError: 
		'''
		pass
	
	def stop(self):
		'''
		@raise InitdError: 
		'''		
		pass
	
	def restart(self):
		'''
		@raise InitdError: 
		'''		
		pass

	def reload(self):
		'''
		@raise InitdError: 
		'''		
		pass

	def status(self):
		'''
		@return: Service status
		@rtype: scalarizr.util.initdv2.Status
		'''
		pass

class SockParam:
	def __init__(self, port=None, family=socket.AF_INET, type=socket.SOCK_STREAM, conn_address=None, timeout=5):
		
		self.family = family
		self.type = type
		self.conn_address = (conn_address or '127.0.0.1', int(port))
		self.timeout = timeout

class ParametrizedInitScript(InitScript):
	name = None
	
	def __init__(self, name, initd_script, pid_file=None, lock_file=None, socks=None):
		
		self.name = name
		self.initd_script = initd_script
		self.pid_file = pid_file
		self.lock_file = lock_file
		self.socks = socks
		
		'''
		@param socks: list(SockParam)
		@todo: implement all sclarizr.util.initd stuff here
		'''
		
	def _start_stop_reload(self, action):
		try:
			cmd = [self.initd_script, action]
			proc = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=False, close_fds=True)
			out, err = proc.communicate()
		except OSError, e:
			raise InitdError("Popen failed with error %s" % (e.strerror,))
		
		if proc.returncode:
			raise InitdError("Cannot %s %s" % (action, self.name), output=out + " " + err)
		
		if action != "stop" and self.socks:
			for sock in self.socks:
				ping_service2(sock)
			
		if self.pid_file:
			if (action == "start" or action == "restart") and not os.path.exists(self.pid_file):
				raise InitdError("Cannot start %s. pid file %s doesn't exists" % (self.name, self.pid_file))
			if action == "stop" and os.path.exists(self.pid_file):
				raise InitdError("Cannot stop %s. pid file %s still exists" % (self.name, self.pid_file))	
			
		return True
	
	def start(self):
		return self._start_stop_reload('start')
	
	def stop(self):
		return self._start_stop_reload('stop')
	
	def restart(self):
		return self._start_stop_reload('restart')
	
	def reload(self):
		return self._start_stop_reload('reload') 
	
	def status(self):
		try:
			for sock in self.socks:
				ping_service2(sock)
		except InitdError:
			return Status.NOT_RUNNING
		
		return Status.RUNNING
	
	def is_running(self):
		return not self.status()
		

def explore(name, init_script_cls):
	_services[name] = init_script_cls

def lookup(name):
	'''
	Lookup init script object by service name
	'''
	if not _services.has_key(name):
		raise InitdError('No service has been explored with name %s ' % name)
	
	if not _instances.has_key(name):
		_instances[name] = _services[name]()
		
	return _instances[name]
		
def ping_service2(sock = None):
	if not isinstance(sock, SockParam):
		raise InitdError('Socks parameter must be instance of SockParam class')
	
	s = socket.socket(sock.family, sock.type)
	time_start = time.time()
	while time.time() - time_start < sock.timeout:
		try:
			s.connect(sock.conn_address)
			s.shutdown(2)
			return
		except:
			time.sleep(0.1)
			pass
	raise InitdError ("Service unavailable after %d seconds of waiting" % sock.timeout)