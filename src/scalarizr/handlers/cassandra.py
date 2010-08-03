'''
Created on Jun 23, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Messages
import logging
import os
from scalarizr.util import configtool, fstool, system, initd
from xml.dom.minidom import parse
from scalarizr.libs.metaconf import *



initd_script = "/etc/init.d/cassandra"
if not os.path.exists(initd_script):
	raise HandlerError("Cannot find Cassandra init script at %s. Make sure that cassandra is installed" % initd_script)

pid_file = '/var/run/cassandra.pid'

logger = logging.getLogger(__name__)
logger.debug("Explore Cassandra service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("cassandra", initd_script, pid_file, tcp_port=7000)

# TODO: rewrite initd to handle service's ip address

class StorageError(BaseException): pass

def get_handlers ():
	return [CassandraHandler()]

class CassandraHandler(Handler):
	_logger = None
	_queryenv = None
	_storage = None
	_storage_path = None
	_storage_conf = None
	_port = None
	_platform = None

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		
		config = bus.config
		self._role_name = config.get(configtool.SECT_GENERAL, configtool.OPT_ROLE_NAME)
		self._storage_path = config.get('behaviour_cassandra','storage_path')
		self._storage_conf = config.get('behaviour_cassandra','storage_conf')
		self.data_file_directory = self._storage_path + "/datafile" 
		self.commit_log_directory = self._storage_path + "/commitlog"

		self._config = Configuration('xml')
		self._config.read(self._storage_conf)
		
		try:
			self._port = self._config.get('.//Storage/StoragePort')
		except PathNotExistsError:
			self._logger.error('Cannot get storage port from config: path not exists')
			self._config.add('.//Storage/StoragePort', '7000')
			self._port = '7000'
			
		"""
		self.xml = parse(self._storage_conf)
		data = self.xml.documentElement
					
		if len(data.childNodes):
			port_entry = data.getElementsByTagName("StoragePort")
	
			if port_entry:
				self._port = port_entry[0].firstChild.nodeValue
			else:
				self._logger.error("Port value not found in cassandra config. Using 7000 for default.")
				self._port = '7000'
		"""
		
		bus.on("init", self.on_init)
		bus.on("before_host_down", self.on_before_host_down)

	def on_init(self):
		bus.on("before_host_up", self.on_before_host_up)
		
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return Behaviours.CASSANDRA in behaviour and \
				(message.name == Messages.HOST_INIT or
				message.name == Messages.HOST_UP or
				message.name == Messages.HOST_DOWN)
	
	def on_HostInit(self, message):
		if message.behaviour == Behaviours.CASSANDRA:
			if not message.local_ip:
				ip = message.remote_ip
			else:
				ip = message.local_ip
			self._add_iptables_rule(ip)

		
	def on_before_host_up(self, message): 
		# Init storage
		role_params = self._queryenv.list_role_params(self._role_name)
		try:
			storage_name = role_params["cassandra_data_storage_engine"]
		except KeyError:
			storage_name = "eph"
				
		self._storage = StorageProvider().new_storage(storage_name)
		self._storage.init(self._storage_path)
		# Update CommitLogDirectory and DataFileDirectory in storage-conf.xml
		if not os.path.exists(self.data_file_directory):
			os.makedirs(self.data_file_directory)
		if not os.path.exists(self.commit_log_directory):
			os.makedirs(self.commit_log_directory)

		self._config.set('.//Storage/CommitLogDirectory', self.commit_log_directory)
		self._config.set('.//Storage/DataFileDirectory', self.data_file_directory)
		
		# TODO: Add farm id to cassandra cluster's name
		self._config.set('.//Storage/ClusterName', 'cassandra-cluster-')

		roles = self._queryenv.list_roles(behaviour = "cassandra")
		seed_list = []
		
		#set list of seed`s IPs
		for role in roles:
			for host in role.hosts:
				if host.internal_ip:
					seed_list.append(host.internal_ip)
				else:
					seed_list.append(host.external_ip)
		
		if '127.0.0.1' in self._config.get_list('.//Storage/Seeds'):
			seed_list.append('127.0.0.1')
		
		self._config.remove('.//Storage/Seeds/Seed')
		
		for seed in seed_list:
			self._config.add('.//Storage/Seeds/Seed', seed)
		
		local_ip = self._platform.get_private_ip()
		self._config.set('.//Storage/ListenAddress', local_ip)
		self._config.set('.//Storage/ThriftAddress', '0.0.0.0')
		self._config.write(open(self._storage_conf, 'w'))
		
		"""
		data = self.xml.documentElement
		
		if len(data.childNodes):
			
			log_entry = data.getElementsByTagName("CommitLogDirectory")
			if log_entry:
				self._logger.debug("Rewriting CommitLogDirectory in cassandra config")
				log_entry[0].firstChild.nodeValue = self.commit_log_directory
			else:
				self._logger.debug("CommitLogDirectory not found in cassandra config")
			
			data_entry = data.getElementsByTagName("DataFileDirectory")
			if data_entry:
				self._logger.debug("Rewriting DataFileDirectory in cassandra config")
				data_entry[0].firstChild.nodeValue = self.data_file_directory
			else:
				self._logger.debug("DataFileDirectory not found in cassandra config")
				
			cluster_name_entry = data.getElementsByTagName("ClusterName")
			if cluster_name_entry:
				self._logger.debug("Rewriting ClusterName in cassandra config")
				cluster_name_entry[0].firstChild.nodeValue = "ololo" # set cassandra-cluster- + farmID
			else:
				self._logger.debug("ClusterName not found in cassandra config")

		for role in roles:
			for host in role.hosts:
				if host.internal_ip:
					seed_list.append(host.internal_ip)
				else:
					seed_list.append(host.external_ip)
			
		seeds_section = data.getElementsByTagName("Seeds")
		
		if seeds_section:
			for seed in seeds_section[0].childNodes:
				if seed.nodeName == "Seed" and seed.firstChild.nodeValue == "127.0.0.1":
						seed_list.append("127.0.0.1")
						
			if seed_list:
				new_section = self.xml.createElement('Seeds')
				
				for seed_ip in seed_list:
					seed_section = self.xml.createElement('Seed')
					text = self.xml.createTextNode(seed_ip)
					seed_section.appendChild(text)
					new_section.appendChild(seed_section)
				
				data.replaceChild(new_section, seeds_section[0])
			
		else:
			self._logger.debug("Seeds section not found in cassandra config")

		listen_entry = data.getElementsByTagName("ListenAddress")
		if listen_entry:
			self._logger.debug("Rewriting ListenAddress in cassandra config")
			listen_entry[0].firstChild.nodeValue = "0.0.0.0"

		thrift_entry = data.getElementsByTagName("ThriftAddress")
		if thrift_entry:
			self._logger.debug("Rewriting ThriftAddress in cassandra config")
			thrift_entry[0].firstChild.nodeValue = "0.0.0.0"
		"""


	
	def on_before_host_down(self):
		try:
			self._logger.info("Stopping Cassandra")
			initd.stop("cassandra")
		except initd.InitdError, e:
			self._logger.error("Cannot stop Cassandra")
			if initd.is_running("cassandra"):
				raise
	

	def on_HostUp(self, message):
		# Update Seed configuration
		pass
	
	def on_HostDown(self, message):
		# Update Seed configuration
		# Update iptables rule
		self._drop_iptable_rules()


	def _add_iptables_rule(self, ip):
		rule = "/sbin/iptables -A INPUT -s %s -p tcp --destination-port %s -j ACCEPT" % (ip, self._port)
		self._logger.debug("Adding rule to iptables: %s", rule)
		returncode = system(rule)[2]
		if returncode :
			self._logger.error("Cannot add rule")
			
	def _drop_iptable_rules(self):
		drop_rule = "/sbin/iptables -A INPUT -p tcp --destination-port %s -j DROP" % (self._port,)
		self._logger.debug("Drop iptables rules on port %s: %s", self._port, drop_rule)
		returncode = system(drop_rule)[2]
		if returncode :
			self._logger.error("Cannot drop rules")
		
class StorageProvider(object):
	
	_providers = None
	_instance = None
	
	def __new__(cls):
		if cls._instance is None:
			o = object.__new__(cls)
			o._providers = dict()
			cls._instance = o
		return cls._instance
	
	def new_storage(self, name, *args, **kwargs):
		if not name in self._providers:
			raise StorageError("Cannot create storage from undefined provider '%s'" % (name,))
		return self._providers[name](*args, **kwargs) 
	
	def register_storage(self, name, cls):
		if name in self._providers:
			raise StorageError("Storage provider '%s' already registered" % (name,))
		self._providers[name] = cls
		
	def unregister_storage(self, name):
		if not name in self._providers:
			raise StorageError("Storage provider '%s' is not registered" % (name,))
		del self._providers[name]
	
class Storage(object):
	def __init__(self):
		pass
	
	def init(self, mpoint, *args, **kwargs):
		pass
	
	def copy_data(self, src, *args, **kwargs):
		pass

class EbsStorage(Storage):
	pass

class EphemeralStorage(Storage):
	_platform = None
	def __init__(self):
		self._platform = bus.platform
		self._logger = logging.getLogger(__name__)
		
	def init(self, mpoint, *args, **kwargs):
		devname = '/dev/' + self._platform.get_block_device_mapping()["ephemeral0"]
		
		try:
			self._logger.debug("Trying to mount device %s and add it to fstab", devname)
			fstool.mount(device = devname, mpoint = mpoint, options = ["-t auto"], auto_mount = True)
		except fstool.FstoolError, e:
			if fstool.FstoolError.NO_FS == e.code:
				self._logger.debug("Trying to create file system on device %s, mount it and add to fstab", devname)
				fstool.mount(device = devname, mpoint = mpoint, options = ["-t auto"], make_fs = True, auto_mount = True)
			else:
				raise
	
	def copy_data(self, src, *args, **kwargs):
		pass
	
StorageProvider().register_storage("eph", EphemeralStorage)	

