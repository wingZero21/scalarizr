'''
Created on Sep 7, 2011

@author: Spike
'''
import os
import logging

from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.services import rabbitmq
from scalarizr import storage
from scalarizr.handlers import HandlerError, ServiceCtlHanler
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.util import fstool
import sys


BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.RABBITMQ
OPT_VOLUME_CNF = 'volume_config'
OPT_SNAPSHOT_CNF = 'snapshot_config'
DEFAULT_STORAGE_PATH = '/var/lib/rabbitmq'
STORAGE_PATH = '/mnt/rabbitstorage'




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
		hosts[addr] = hostname
		cls._write(hosts)	
		
	@classmethod
	def delete(cls, addr=None, hostname=None):
		hosts = cls.hosts()
		if hosts.has_key(addr):
			del hosts[addr]
		cls._write(hosts)		
	
	@property
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
				ret[addr] = hostname				
		return ret
	
	@classmethod
	def _write(cls, hosts):
		with open('/etc/hosts', 'w') as f:
			for addr, hostname in hosts.iteritems():
				f.write('%s\t%s' % (addr, hostname))
				
		
class RabbitMQHandler(ServiceCtlHanler):	

	def __init__(self):
		bus.on("init", self.on_init)
		self._logger = logging.getLogger(__name__)
		self.rabbitmq = rabbitmq.rabbitmq
		# TODO: bus.define_events()
		self.on_reload()


	def on_init(self):		
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("host_init", self.on_host_init)


	def on_reload(self):
		self.cnf = bus.cnf
		self.queryenv = bus.queryenv_service
		self.platform = bus.platform	

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

		Hosts.add(message.local_ip, 'rabbit.%s' % message.server_index)

		if message.local_ip == self.platform.get_private_ip():
			updates = dict(hostname='rabbit.%s' % message.server_index)
			self._update_config(updates)
			return

		if BuiltinBehaviours.RABBITMQ in message.behaviour:			
			self.rabbitmq.add_node(message.local_ip)
			
			
	def on_HostDown(self, message):
		if not BuiltinBehaviours.RABBITMQ in message.behaviour:
			return
		Hosts.delete(message.local_ip)
		

	def on_host_init_response(self, message):
		if not message.body.has_key("rabbitmq"):
			raise HandlerError("HostInitResponse message for RabbitMQ behaviour must have 'rabbitmq' property")

		rabbitmq_data = message.rabbitmq.copy()

		if os.path.exists(self._volume_config_path):
			os.remove(self._volume_config_path)

		if OPT_VOLUME_CNF in rabbitmq_data:
			if rabbitmq_data[OPT_VOLUME_CNF]:
				storage.Storage.backup_config(rabbitmq_data[OPT_VOLUME_CNF], self._volume_config_path)
			del rabbitmq_data[OPT_VOLUME_CNF]
		
		self._update_config(rabbitmq_data)


	def on_before_host_up(self, message):
		self.rabbitmq.service.stop()
		volume_cnf = storage.Storage.restore_config(self._volume_config_path)
		self.storage_vol = self._plug_storage(volume_cnf)
		storage.Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
		fstool.mount(STORAGE_PATH, DEFAULT_STORAGE_PATH, ['--bind'])

		for role in self.queryenv.list_roles(behaviour = BEHAVIOUR):
			for host in role.hosts:
				self.rabbitmq.add_node('rabbit.%s' % host.index)

		if self.rabbitmq.node_type == rabbitmq.NodeTypes.DISK:
			hostname = self.cnf.rawini.get(CNF_SECTION, 'hostname')
			self.rabbitmq.add(hostname)

		self.rabbitmq.set_cookie()
		self.rabbitmq.service.start()


	def _plug_storage(self, mpoint, vol):
		if not isinstance(vol, storage.Volume):
			vol = storage.Storage.create(vol)

		if not os.path.exists(mpoint):
			os.makedirs(mpoint)
		if not vol.mounted():
			try:
				vol.mount(mpoint)
			except fstool.FstoolError,e:
				if e.code == fstool.FstoolError.NO_FS:
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