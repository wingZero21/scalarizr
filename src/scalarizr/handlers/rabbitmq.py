'''
Created on Sep 7, 2011

@author: Spike
'''
import os
import sys
import pwd
import logging


from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr import storage
from scalarizr.handlers import HandlerError, ServiceCtlHandler
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.util import system2, initdv2, software, dns, cryptotool
from scalarizr.storage import StorageError
import scalarizr.services.rabbitmq as rabbitmq_svc


BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.RABBITMQ
OPT_VOLUME_CNF = 'volume_config'
OPT_SNAPSHOT_CNF = 'snapshot_config'
DEFAULT_STORAGE_PATH = '/var/lib/rabbitmq/mnesia'
STORAGE_PATH = '/mnt/rabbitstorage'
STORAGE_VOLUME_CNF = 'rabbitmq.json'
RABBITMQ_MGMT_PLUGIN_NAME= 'rabbitmq_management' 
RABBITMQ_MGMT_AGENT_PLUGIN_NAME = 'rabbitmq_management_agent'


class RabbitMQMessages:
	RABBITMQ_RECONFIGURE = 'RabbitMq_Reconfigure'
	RABBITMQ_SETUP_CONTROL_PANEL = 'RabbitMq_SetupControlPanel'
	RABBITMQ_RECONFIGURE_RESULT = 'RabbitMq_ReconfigureResult'
	RABBITMQ_SETUP_CONTROL_PANEL_RESULT = 'RabbitMq_SetupControlPanelResult'


def get_handlers():
	return (RabbitMQHandler(), )

		
class RabbitMQHandler(ServiceCtlHandler):	

	def __init__(self):
		if not software.whereis('rabbitmqctl'):
			raise HandlerError("Rabbitmqctl binary was not found. Check your installation.")
		
		bus.on("init", self.on_init)
		
		self._logger = logging.getLogger(__name__)
		self.rabbitmq = rabbitmq_svc.rabbitmq
		self.service = initdv2.lookup(BuiltinBehaviours.RABBITMQ)
		
		self.on_reload()
			
		if 'ec2' == self.platform.name:
			updates = dict(hostname_as_pubdns = '0')
			self.cnf.update_ini('ec2', {'ec2': updates}, private=False)
	
	
	def on_init(self):
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("before_hello", self.on_before_hello)
		bus.on("rebundle_cleanup_image", self.cleanup_hosts_file)

		
		if self.cnf.state == ScalarizrState.BOOTSTRAPPING:
			
			self.cleanup_hosts_file('/')
			self._logger.info('Performing initial cluster reset')
			self.service.start()
			self.rabbitmq.stop_app()
			self.rabbitmq.reset()
			self.service.stop()


		elif self.cnf.state == ScalarizrState.RUNNING:
			
			storage_conf = storage.Storage.restore_config(self._volume_config_path)
			self.storage_vol = storage.Storage.create(storage_conf)
			if not self.storage_vol.mounted():
				self.service.stop()
				self.storage_vol.mount()
				self.service.start()
		
			
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
		
	
	def cleanup_hosts_file(self, rootdir):
		""" Clean /etc/hosts file """
		hosts_path = os.path.join(rootdir, 'etc', 'hosts')
		if os.path.isfile(hosts_path):
			try:
				dns.ScalrHosts.HOSTS_FILE_PATH = hosts_path
				for hostname in dns.ScalrHosts.hosts().keys():
					dns.ScalrHosts.delete(hostname=hostname)
			finally:
				dns.ScalrHosts.HOSTS_FILE_PATH = '/etc/hosts'
					
	
			
	
	def on_before_hello(self, message):
		try:
			rabbit_version = software.rabbitmq_software_info()
		except:
			raise HandlerError("Can't find rabbitmq on this server.")
		
		if rabbit_version.version < (2, 7, 0):
			self._logger.error("Unsupported RabbitMQ version. Assertion failed: %s >= 2.7.0", 
							'.'.join(rabbit_version.version))
			sys.exit(1)


	def on_RabbitMq_SetupControlPanel(self, message):
		try:
			if not self.cnf.state == ScalarizrState.RUNNING:
				raise HandlerError('Server is not in RUNNING state yet')
			try:
				self.service.stop()
				self.rabbitmq.enable_plugin(RABBITMQ_MGMT_PLUGIN_NAME)
			finally:
				self.service.start()
			
			panel_url = 'http://%s:55672/mgmt/' % self.platform.get_public_ip()
			msg_body = dict(status='ok', cpanel_url=panel_url)
		except:
			error = str(sys.exc_info()[1])
			msg_body = dict(status='error', last_error=error)
		finally:
			self.send_message(RabbitMQMessages.RABBITMQ_SETUP_CONTROL_PANEL_RESULT, msg_body)


	def on_RabbitMq_Reconfigure(self, message):
		try:
			if not self.cnf.state == ScalarizrState.RUNNING:
				raise HandlerError('Server is not in RUNNING state yet')
			
			ini = self.cnf.rawini
			if message.node_type != ini.get(CNF_SECTION, 'node_type'):
				self._logger.info('Changing node type to %s' % message.node_type)
				nodes = self._get_cluster_nodes()
				if nodes:
					if message.node_type == rabbitmq_svc.NodeTypes.DISK:
						hostname = self.cnf.rawini.get(CNF_SECTION, 'hostname')
						nodes.append(hostname)
						
					self.rabbitmq.cluster_with(nodes, do_reset=False)
																
				self._update_config(dict(node_type=message.node_type))
			else:
				raise HandlerError('Node type is already %s' % message.node_type)
				
			msg_body = dict(status='ok', node_type=message.node_type)
		except:
			error = str(sys.exc_info()[1])
			msg_body = dict(status='error', last_error=error)
		finally:
			self.send_message(RabbitMQMessages.RABBITMQ_RECONFIGURE_RESULT, msg_body)


	def on_HostInit(self, message):
		if not BuiltinBehaviours.RABBITMQ in message.behaviour:
			return
				
		if message.local_ip != self.platform.get_private_ip():
			hostname = 'rabbit-%s' % message.server_index
			dns.ScalrHosts.set(message.local_ip, hostname)
			
			
	def on_HostDown(self, message):
		if not BuiltinBehaviours.RABBITMQ in message.behaviour:
			return
		dns.ScalrHosts.delete(message.local_ip)
		

	def on_host_init_response(self, message):
		if not message.body.has_key("rabbitmq"):
			raise HandlerError("HostInitResponse message for RabbitMQ behaviour must have 'rabbitmq' property")
		
		path = os.path.dirname(self._volume_config_path)
		if not os.path.exists(path):
			os.makedirs(path)

		rabbitmq_data = message.rabbitmq.copy()
		
		if not rabbitmq_data['password']:
			rabbitmq_data['password'] = cryptotool.pwgen(10)

		if os.path.exists(self._volume_config_path):
			os.remove(self._volume_config_path)

		hostname = 'rabbit-%s' % int(message.server_index)

		dns.ScalrHosts.set('127.0.0.1', hostname)
		with open('/etc/hostname', 'w') as f:
			f.write(hostname)
		system2(('hostname', '-F', '/etc/hostname'))

		if OPT_VOLUME_CNF in rabbitmq_data:
			if rabbitmq_data[OPT_VOLUME_CNF]:
				storage.Storage.backup_config(rabbitmq_data[OPT_VOLUME_CNF], self._volume_config_path)
			del rabbitmq_data[OPT_VOLUME_CNF]

		rabbitmq_data.update(dict(hostname=hostname))
		self._update_config(rabbitmq_data)


	def on_before_host_up(self, message):
		volume_cnf = storage.Storage.restore_config(self._volume_config_path)
		self.storage_vol = self._plug_storage(DEFAULT_STORAGE_PATH, volume_cnf)
		rabbitmq_user = pwd.getpwnam("rabbitmq")
		os.chown(DEFAULT_STORAGE_PATH, rabbitmq_user.pw_uid, rabbitmq_user.pw_gid)
		storage.Storage.backup_config(self.storage_vol.config(), self._volume_config_path)

		nodes = self._get_cluster_nodes()
		
		do_cluster = True if nodes else False
		is_disk_node = self.rabbitmq.node_type == rabbitmq_svc.NodeTypes.DISK
		
		if is_disk_node:
			hostname = self.cnf.rawini.get(CNF_SECTION, 'hostname')
			nodes.append(hostname)
	
		self._logger.debug('Enabling management agent plugin')
		self.rabbitmq.enable_plugin(RABBITMQ_MGMT_AGENT_PLUGIN_NAME)
		
		ini = self.cnf.rawini
		cookie = ini.get(CNF_SECTION, 'cookie')		
		self._logger.debug('Setting erlang cookie: %s' % cookie)
		self.rabbitmq.set_cookie(cookie)
		
		self.service.start()
		
		init_run = 'volume_id' not in volume_cnf.keys()
		if do_cluster and (not is_disk_node or init_run):
			self.rabbitmq.cluster_with(nodes)

		self.rabbitmq.delete_user('guest')
		password = self.cnf.rawini.get(CNF_SECTION, 'password')
		self.rabbitmq.check_scalr_user(password)
		
		# Update message
		msg_data = {}
		msg_data['volume_config'] = self.storage_vol.config()
		msg_data['node_type'] = self.rabbitmq.node_type
		msg_data['password'] = password
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
		
		
	def _get_cluster_nodes(self):
		nodes = []
		for role in self.queryenv.list_roles(behaviour = BEHAVIOUR):
			for host in role.hosts:
				ip = host.internal_ip
				hostname = 'rabbit-%s' % host.index
				dns.ScalrHosts.set(ip, hostname)
				nodes.append(hostname)
		return nodes

		