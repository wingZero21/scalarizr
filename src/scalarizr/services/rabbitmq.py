'''
Created on Sep 8, 2011

@author: Spike
'''

import os
import pwd
import urllib
import shutil
import logging
import subprocess

from .postgresql import lazy
from scalarizr.bus import bus
from scalarizr.libs import metaconf
from scalarizr.util import initdv2, system2, run_detached
from scalarizr.config import BuiltinBehaviours


SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.RABBITMQ
RABBIT_CFG_PATH = '/etc/rabbitmq/rabbitmq.config'
COOKIE_PATH = '/var/lib/rabbitmq/.erlang.cookie'

class NodeTypes:
	RAM = 'ram'
	DISK = 'disk'
	

class RabbitMQInitScript(initdv2.ParametrizedInitScript):
	
	@lazy
	def __new__(cls, *args, **kws):
		obj = super(RabbitMQInitScript, cls).__new__(cls, *args, **kws)
		cls.__init__(obj)
		return obj
	
	def __init__(self):
		initdv2.ParametrizedInitScript.__init__(
				self,
				'rabbitmq',
				'/etc/init.d/rabbitmq-server',
				'/var/run/rabbitmq/pid',
				socks=[initdv2.SockParam(5672, timeout=20)]
				)
		
	def stop(self, reason=None):
		system2(('rabbitmqctl', 'stop'))
	
	def restart(self, reason=None):
		self.stop()
		self.start()

	reload = restart

	def start(self):
		env = {'RABBITMQ_PID_FILE': '/var/run/rabbitmq/pid'}
		run_detached('rabbitmq-server', args=['-detached'], env=env)
		initdv2.wait_sock(self.socks[0])
				
		
	def status(self):
		if self._running:
			return initdv2.Status.RUNNING
		else:
			return initdv2.Status.NOT_RUNNING
		
	@property
	def _running(self):
		rcode = system2(('rabbitmqctl', 'status'), raise_exc=False)[2]
		return False if rcode else True
			
		
initdv2.explore(SERVICE_NAME, RabbitMQInitScript)

	
	
class RabbitMQ(object):
	_instance = None
	
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(RabbitMQ, cls).__new__(
								cls, *args, **kwargs)
		return cls._instance
	

	def __init__(self):
		self._cnf = bus.cnf
		self._logger = logging.getLogger(__name__)
		self.rabbitmq_cnf = metaconf.Configuration('erlang')
		
		for dir in os.listdir('/usr/lib/rabbitmq/lib/'):
			if dir.startswith('rabbitmq_server'):
				self.plugin_dir = os.path.join('/usr/lib/rabbitmq/lib/', dir)
				break
		else:
			raise Exception('RabbitMQ plugin directory not found')
		
		if os.path.exists(RABBIT_CFG_PATH):
			self.rabbitmq_cnf.read(RABBIT_CFG_PATH)
		try:
			self.rabbitmq_cnf.get('./rabbit/cluster_nodes')
		except metaconf.NoPathError:
			self.rabbitmq_cnf.add('./rabbit/cluster_nodes', force=True)
			
		self.service = initdv2.lookup(SERVICE_NAME)

	def set_cookie(self, cookie):
		cookie = self._cnf.rawini.get(CNF_SECTION, 'cookie')
		with open(COOKIE_PATH, 'w') as f:
			f.write(cookie)
		rabbitmq_user = pwd.getpwnam("rabbitmq")
		os.chmod(COOKIE_PATH, 0600)
		os.chown(COOKIE_PATH, rabbitmq_user.pw_uid, rabbitmq_user.pw_gid)


	def install_plugin(self, url):
		filename = os.path.basename(url)
		filepath = os.path.join(self.plugin_dir, filename)
		tmp_path = urllib.urlretrieve(url)[0]
		shutil.copy(tmp_path, filepath)
		os.remove(tmp_path)		
	
	
	def reset(self):
		system2(('rabbitmqctl', 'reset'), logger=self._logger)
	
	
	def stop_app(self):		
		system2(('rabbitmqctl', 'stop_app'), logger=self._logger)
	
	
	def start_app(self):
		system2(('rabbitmqctl', 'start_app'), logger=self._logger)
		
		
	def add_user(self, username, password, is_admin=False):
		system2(('rabbitmqctl', 'add_user', username, password), logger=self._logger)
		if is_admin:
			system2(('rabbitmqctl', 'set_user_tags', username, 'administrator'), logger=self._logger)
	
	
	@property
	def node_type(self):
		return self._cnf.rawini.get(CNF_SECTION, 'node_type')
	
	
	def cluster_with(self, hostnames, do_reset=True):
		nodes = ['rabbit@%s' % host for host in hostnames]
		cmd = ['rabbitmqctl', 'cluster'] + nodes
		self.stop_app()
		if do_reset:
			self.reset()
		system2(cmd, logger=self._logger)
		self.start_app()
	
	
	def _write_cfg(self):
		self.rabbitmq_cnf.write(RABBIT_CFG_PATH) 

rabbitmq = RabbitMQ()




		