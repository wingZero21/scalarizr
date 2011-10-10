'''
Created on Sep 7, 2011

@author: Spike
'''
import os
import sys
import pwd
import shutil
import logging

from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.services import rabbitmq
from scalarizr import storage
from scalarizr.handlers import HandlerError, ServiceCtlHanler
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.util import fstool, system2
from scalarizr.storage import StorageError



BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.RABBITMQ
OPT_VOLUME_CNF = 'volume_config'
OPT_SNAPSHOT_CNF = 'snapshot_config'
DEFAULT_STORAGE_PATH = '/var/lib/rabbitmq'
STORAGE_PATH = '/mnt/rabbitstorage'
STORAGE_VOLUME_CNF = 'rabbitmq.json'



class RabbitMQMessages:
	RABBITMQ_RECONFIGURE = 'RabbitMQ_Reconfigure'
	RABBITMQ_SETUP_CONTROL_PANEL = 'RabbitMQ_SetupControlPanel'
	RABBITMQ_RECONFIGURE_RESULT = 'RabbitMQ_ReconfigureResult'
	RABBITMQ_SETUP_CONTROL_PANEL_RESULT = 'RabbitMQ_SetupControlPanelResult'


def get_handlers():
	return (RabbitMQHandler(), )

class Hosts:
	
	@classmethod
	def set(cls, addr, hostname):
		hosts = cls.hosts()
		hosts[hostname] = addr
		cls._write(hosts)
		
	@classmethod
	def delete(cls, addr=None, hostname=None):
		hosts = cls.hosts()
		if hostname:
			if hosts.has_key(hostname):
				del hosts[hostname]
		if addr:
			for host, ip  in hosts.iteritems():
				if ip == addr:
					del hosts[host]
		cls._write(hosts)		
	
	@classmethod
	def hosts(cls):
		ret = {}
		with open('/etc/hosts') as f:
			hosts = f.readlines()
			for host in hosts:
				host_line = host.strip()
				if not host_line or host_line.startswith('#'):
					continue
				addr, hostname = host.split(None, 1)
				ret[hostname.strip()] = addr
		return ret
	
	@classmethod
	def _write(cls, hosts):
		with open('/etc/hosts', 'w') as f:
			for hostname, addr in hosts.iteritems():
				f.write('%s\t%s\n' % (addr, hostname))
				
		
class RabbitMQHandler(ServiceCtlHanler):	

	def __init__(self):
		bus.on("init", self.on_init)
		self._logger = logging.getLogger(__name__)
		self.rabbitmq = rabbitmq.rabbitmq
		
		self.on_reload()
		
		if self.cnf.state == ScalarizrState.BOOTSTRAPPING:
			if not os.path.exists('/etc/hosts.safe'):
				shutil.copy2('/etc/hosts', '/etc/hosts.safe')
			
			self.rabbitmq.service.start()
			self.rabbitmq.stop_app()
			self.rabbitmq.reset()
			self.rabbitmq.stop()
			self.rabbitmq.service.stop()
			system2(('ps', 'ax'), logger=self._logger)
				
	def on_init(self):
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		
			
	def on_reload(self):
		self.cnf = bus.cnf
		self.queryenv = bus.queryenv_service
		self.platform = bus.platform
		self._volume_config_path  = self.cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and message.name in (
												Messages.HOST_INIT,
												Messages.HOST_DOWN,
												Messages.UPDATE_SERVICE_CONFIGURATION,
												RabbitMQMessages.RABBITMQ_RECONFIGURE,
												RabbitMQMessages.RABBITMQ_SETUP_CONTROL_PANEL)


	def on_RabbitMQ_SetupControlPanel(self, message):
		pass


	def on_RabbitMQ_Reconfigure(self, message):
		try:
			if not self.cnf.state == ScalarizrState.RUNNING:
				raise HandlerError()
			
			ini = self.cnf.rawini
			if message.node_type != ini.get(CNF_SECTION, 'node_type'):	
				self.rabbitmq.change_type(message.node_type)
				
			msg_body = dict(status='ok', node_type=message.node_type)
		except:
			error = str(sys.exc_info()[1])
			msg_body = dict(status='error', last_error=error)
		finally:
			self.send_message(RabbitMQMessages.RABBITMQ_RECONFIGURE_RESULT, msg_body)


	def on_HostInit(self, message):
		if not BuiltinBehaviours.RABBITMQ in message.behaviour:
			return
		
		if message.local_ip == self.platform.get_private_ip():
			updates = dict(hostname='rabbit-%s' % message.server_index)
			self._update_config(updates)
			Hosts.set('127.0.0.1', 'rabbit-%s' % message.server_index)
			with open('/etc/hostname', 'w') as f:
				f.write('rabbit-%s' % message.server_index)
			system2(('hostname', '-F', '/etc/hostname'))			
		else:
			Hosts.set(message.local_ip, 'rabbit-%s' % message.server_index)		
			self.rabbitmq.add_node(message.local_ip)
			
			
			
	def on_HostDown(self, message):
		if not BuiltinBehaviours.RABBITMQ in message.behaviour:
			return
		Hosts.delete(message.local_ip)
		

	def on_host_init_response(self, message):
		if not message.body.has_key("rabbitmq"):
			raise HandlerError("HostInitResponse message for RabbitMQ behaviour must have 'rabbitmq' property")
		
		dir = os.path.dirname(self._volume_config_path)
		if not os.path.exists(dir):
			os.makedirs(dir)
			
		rabbitmq_data = message.rabbitmq.copy()

		if os.path.exists(self._volume_config_path):
			os.remove(self._volume_config_path)

		if OPT_VOLUME_CNF in rabbitmq_data:
			if rabbitmq_data[OPT_VOLUME_CNF]:
				storage.Storage.backup_config(rabbitmq_data[OPT_VOLUME_CNF], self._volume_config_path)
			del rabbitmq_data[OPT_VOLUME_CNF]

		self._update_config(rabbitmq_data)


	def on_before_host_up(self, message):
		volume_cnf = storage.Storage.restore_config(self._volume_config_path)
		self.storage_vol = self._plug_storage(DEFAULT_STORAGE_PATH, volume_cnf)
		rabbitmq_user = pwd.getpwnam("rabbitmq")
		os.chown(DEFAULT_STORAGE_PATH, rabbitmq_user.pw_uid, rabbitmq_user.pw_gid)
		storage.Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
		#fstool.mount(STORAGE_PATH, DEFAULT_STORAGE_PATH, ['--bind'])

		for role in self.queryenv.list_roles(behaviour = BEHAVIOUR):
			for host in role.hosts:
				self.rabbitmq.add_node('rabbit-%s' % host.index)

		if self.rabbitmq.node_type == rabbitmq.NodeTypes.DISK:
			hostname = self.cnf.rawini.get(CNF_SECTION, 'hostname')
			self.rabbitmq.add(hostname)

		ini = self.cnf.rawini
		cookie = ini.get(CNF_SECTION, 'cookie')
		self._logger.debug('Setting erlang cookie: %s' % cookie)
		self.rabbitmq.set_cookie(cookie)
		system2(('ls', '/var/lib/rabbitmq/mnesia'), logger=self._logger)
		self.rabbitmq.service.start()
		
		# Update message
		msg_data = {}
		msg_data['volume_config'] = self.storage_vol.config()
		msg_data['node_type'] = self.rabbitmq.node_type
		self._logger.debug('Updating HostUp message with %s' % msg_data)
		message.rabbitmq = msg_data


	def _plug_storage(self, mpoint, vol):
		if not isinstance(vol, storage.Volume):
			vol = storage.Storage.create(vol)

		if not os.path.exists(mpoint):
			os.makedirs(mpoint)
		if not vol.mounted():
			try:
				vol.mount(mpoint)
			except StorageError, e:
				if 'you must specify the filesystem type' in str(e):
					vol.mkfs()
					vol.mount(mpoint)
				else:
					raise
		return vol

			
	def _update_config(self, data): 
		updates = dict()
		for k,v in data.items():
			if v: 
				updates[k] = v		
		self.cnf.update_ini(BEHAVIOUR, {CNF_SECTION: updates})