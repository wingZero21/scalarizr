'''
Created on Aug 1, 2012

@author: dmitry
'''

from __future__ import with_statement

import os
import sys
import time
import logging
import threading
from scalarizr import config
from scalarizr.bus import bus
from scalarizr import handlers, rpc
from scalarizr.util import system2, PopenError
from scalarizr.services import redis as redis_service
from scalarizr.handlers import redis as redis_handler


BEHAVIOUR = CNF_SECTION = redis_handler.CNF_SECTION
OPT_REPLICATION_MASTER = redis_handler.OPT_REPLICATION_MASTER
OPT_PERSISTENCE_TYPE = redis_handler.OPT_PERSISTENCE_TYPE
STORAGE_PATH = redis_handler.STORAGE_PATH
DEFAULT_PORT = redis_service.DEFAULT_PORT
BIN_PATH = redis_service.BIN_PATH
DEFAULT_CONF_PATH = redis_service.DEFAULT_CONF_PATH
MAX_CUSTOM_PROCESSES = 15
PORTS_RANGE = range(DEFAULT_PORT+1, DEFAULT_PORT+MAX_CUSTOM_PROCESSES)



LOG = logging.getLogger(__name__)


class RedisAPI(object):

	_cnf = None
	_queryenv = None
	
	def __init__(self):
		self._cnf = bus.cnf
		self._queryenv = bus.queryenv_service
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)


	
	@rpc.service_method
	def launch_processes(self, num=None, ports=None, passwords=None, async=False):	
		if ports and passwords and len(ports) != len(passwords):
			raise AssertionError('Number of ports must be equal to number of passwords')
		if num and ports and num != len(ports):
				raise AssertionError('When ports range is passed its length must be equal to num parameter')
		if not self.is_replication_master:
			if not passwords or not ports:
				raise AssertionError('ports and passwords are required to launch processes on redis slave')
		available_ports = self.available_ports
		if num > len(available_ports):
			raise AssertionError('Cannot launch %s new processes: Ports available: %s' % (num, str(available_ports)))
		
		if not ports:
			ports = available_ports[:num]
		
		if async:
			txt = 'Launch Redis processes'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						result = self._launch(ports, passwords, op)
				op.ok(data=dict(ports=result[0], passwords=result[1]))
			threading.Thread(target=block).start()
			return op.id
		
		else:
			result = self._launch(ports, passwords)
			return dict(ports=result[0], passwords=result[1])

		
	@rpc.service_method
	def shutdown_processes(self, ports, remove_data=False, async=False):
		if async:
			txt = 'Shutdown Redis processes'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						self._shutdown(ports, remove_data, op)
				op.ok()
			threading.Thread(target=block).start()
			return op.id
		else:
			self._shutdown(ports, remove_data)
			
	@property
	def available_ports(self):
		free_ports = []
		args = ('ps', '-G', 'redis', '-o', 'command', '--no-headers')
		out = system2(args, silent=True)[0].split('\n')
		try:	
			p = [x for x in out if x and BIN_PATH in x]
		except PopenError,e:
			p = []
		for redis_process in p:
			for port in PORTS_RANGE:
				conf_name = redis_service.get_redis_conf_basename(port)
				if conf_name not in redis_process:
					free_ports.append(port)
		return free_ports
			

	def _launch(self, ports=[], passwords=[], op=None):
		is_replication_master = self.is_replication_master
		primary_ip = self.get_primary_ip()
		assert primary_ip is not None
		
		for port,password in zip(ports, passwords or [None for port in ports]):
			if op:
				op.step('Launch Redis %s on port %s' ('Master' if is_replication_master else 'Slave', port))
			try:
				if op:
					op.__enter__()

				if port not in self.ports:
					self.create_redis_conf_copy(port)
					redis_process = redis_service.Redis(is_replication_master, self.persistence_type, port, password)
					if not redis_process.service.running:
						res = redis_process.init_master(STORAGE_PATH) if is_replication_master else redis_process.init_slave(STORAGE_PATH, primary_ip)
					return res
				
			except:
				if op:
					op.__exit__(sys.exc_info())
				raise
			finally:
				if op:
					op.__exit__()
		
	
	def _shutdown(self, ports, remove_data=False, op=None):
		is_replication_master = self.is_replication_master
		for port in ports:
			if op:
				op.step('Shutdown Redis %s on port %s' ('Master' if is_replication_master else 'Slave', port))
			try:
				if op:
					op.__enter__()
	
				instance = redis_service.Redis(port=port)
				if instance.service.running:
					instance.service.stop()
				if remove_data and os.path.exists(instance.db_path):
					os.remove(instance.db_path)
			except:
				if op:
					op.__exit__(sys.exc_info())
				raise
			finally:
				if op:
					op.__exit__()
					
		
	@property
	def is_replication_master(self):
		value = 0
		if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_REPLICATION_MASTER):
			value = self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)
		return True if int(value) else False
	
	
	@property
	def persistence_type(self):
		value = 'snapshotting'
		if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_PERSISTENCE_TYPE):
			value = self._cnf.rawini.get(CNF_SECTION, OPT_PERSISTENCE_TYPE)
		return value


	def get_primary_ip(self):
		master_host = None
		LOG.info("Requesting master server")
		while not master_host:
			try:
				master_host = list(host 
					for host in self._queryenv.list_roles(self._role_name)[0].hosts 
					if host.replication_master)[0]
			except IndexError:
				LOG.debug("QueryEnv respond with no %s master. " % BEHAVIOUR + 
						"Waiting %d seconds before the next attempt" % 5)
				time.sleep(5)
		host = master_host.internal_ip or master_host.external_ip
		return host

