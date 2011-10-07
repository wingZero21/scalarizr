'''
Created on Sep 8, 2011

@author: Spike
'''

import os
import pwd
import logging

from .postgresql import lazy
from scalarizr.bus import bus
from scalarizr.libs import metaconf
from scalarizr.util import initdv2, system2
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
				socks=[initdv2.SockParam(5672)])
		
	def stop(self, reason=None):
		initdv2.ParametrizedInitScript.stop(self)
	
	def restart(self, reason=None):
		initdv2.ParametrizedInitScript.restart(self)
	
	def reload(self, reason=None):
		initdv2.ParametrizedInitScript.restart(self)
		
initdv2.explore(SERVICE_NAME, RabbitMQInitScript)


class RabbitMQ(object):
	service = None
	_instance = None
	
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(RabbitMQ, cls).__new__(
								cls, *args, **kwargs)
		return cls._instance
	

	def __init__(self):
		self.service = initdv2.lookup(SERVICE_NAME)
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


	def set_cookie(self, cookie):
		cookie = self._cnf.rawini.get(CNF_SECTION, 'cookie')
		with open(COOKIE_PATH, 'w') as f:
			f.write(cookie)
		rabbitmq_user = pwd.getpwnam("rabbitmq")
		os.chmod(COOKIE_PATH, 0600)
		os.chown(COOKIE_PATH, rabbitmq_user.pw_uid, rabbitmq_user.pw_gid)
	
	
	def change_type(self, type):
		hostname = self._cnf.rawini.get(CNF_SECTION, 'hostname')
		if type == NodeTypes.RAM:
			self.delete_node(hostname)
		else:
			self.add_node(hostname)
				
	
	def add_node(self, hostname):
		self.stop_app()
		self.rabbitmq_cnf.add('./rabbit/cluster_nodes/%s' % hostname)
		self._write_cfg()
		self.start_app()


	def delete_node(self, hostname):
		self.stop_app()
		self.rabbitmq_cnf.remove('./rabbit/cluster_nodes/%s' % hostname)
		self._write_cfg()
		self.start_app()		


	def install_plugin(self, link):
		pass
	
	
	def stop_app(self):
		system2(('rabbitmqctl', 'stop_app'), logger=self._logger)
	
	
	def start_app(self):
		system2(('rabbitmqctl', 'start_app'), logger=self._logger)
	
	
	@property
	def node_type(self):
		return self._cnf.rawini.get(CNF_SECTION, 'node_type')
	
	
	def _write_cfg(self):
		self.rabbitmq_cnf.write(RABBIT_CFG_PATH) 

	
rabbitmq = RabbitMQ()
		