'''
Created on Aug 29, 2010

@author: marat
@author: spike
'''
from socket import socket

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
		self.conn_address = conn_address or ('127.0.0.1', port)
		self.timeout = timeout

class ParametrizedInitScript(InitScript):
	name = None
	def __init__(self, name, initd_script, pid_file=None, lock_file=None, socks=None):
		'''
		@param socks: list(SockParam)
		@todo: implement all sclarizr.util.initd stuff here
		'''
		pass

def explore(init_script_cls):
	pass

def lockup(name):
	'''
	Lookup init script object by service name
	'''
	pass
