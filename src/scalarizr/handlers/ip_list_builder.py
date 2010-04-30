'''
Created on Dec 11, 2009

@author: Dmytro Korsakov
@author: marat
'''

# TODO: add onRebootStart - delete onRebootFinish - restore
import logging
import os
from scalarizr.handlers import Handler
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.util import configtool

def get_handlers ():
	return [IpListBuilder()]

class IpListBuilder(Handler):
	name = "ip_list_builder"
	_logger = None
	_base_path = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		config = bus.config
		self._base_path = config.get(configtool.get_handler_section_name(self.name), "base_path")
		self._base_path = self._base_path.replace('$etc_path', bus.etc_path)     	    
		if self._base_path[-1] != os.sep:
			self._base_path = self._base_path + os.sep  		

	def _create_dir(self, d):
		if not os.path.exists(d):
			try:
				self._logger.debug("Create dir %s", d)
				os.makedirs(d)
			except OSError, x:
				self._logger.exception(x)
	
	def _create_file(self, f):
		try:
			self._logger.debug("Touch file %s", f)
			open(f, 'w').close()
		except OSError, x:
			self._logger.error(x)
	
	def _remove_dir(self, d):
		if not os.listdir(d):
			try:
				self._logger.debug("Remove dir %s", d)
				os.removedirs(d)
			except OSError, x:
				self._logger.error(x)
	
	def _remove_file(self, f):
		try:
			self._logger.debug("Remove file %s", f)
			os.remove(f)
		except OSError, x:
			self._logger.error(x)
			
	def _host_is_replication_master(self, ip,role_name):
		try:
			received_roles = self._queryenv.list_roles(role_name)
		except:
			self._logger.error('Can`t retrieve list of roles from Scalr.')
			raise
				
		for role in received_roles:
			for host in role.hosts:
				if ip == host.internal_ip:
					return host.replication_master
				
		self._logger.warning("Cannot find ip '%s' in roles list", ip)
		return False
		
	def on_HostUp(self, message):
		self._logger.debug("Entering host up...") 
		
		role_alias = message.body["RoleAlias"]
		internal_ip = message.body["InternalIP"]
		role_name = message.body["RoleName"]

		self._logger.info("Add role host (role_name: %s, role_alias: %s, ip: %s)", 
						role_name, role_alias, internal_ip)
		
		# Create %role_name%/xx.xx.xx.xx
		full_path = self._base_path + role_name + os.sep
		print full_path
		self._create_dir(full_path)		
		self._create_file(full_path + internal_ip)
		
		if role_alias == "mysql": 
			suffix = "master" if self._host_is_replication_master(internal_ip, role_name) else "slave"
			
			# Create mysql-(master|slave)/xx.xx.xx.xx
			mysql_path = self._base_path + "mysql-" + suffix + os.sep
			self._create_dir(mysql_path)		
			self._create_file(mysql_path + internal_ip)
		else:
			# Create %role_alias%/xx.xx.xx.xx
			full_path = self._base_path + role_alias + os.sep
			self._create_dir(full_path)
			self._create_file(full_path + internal_ip)
			
	def on_HostDown(self, message):
		self._logger.debug("Entering host down...")
		
		role_alias = message.body["RoleAlias"]
		internal_ip = message.body["InternalIP"]
		role_name = message.body["RoleName"]
		
		self._logger.info("Remove role host (role_name: %s, role_alias: %s, ip: %s)", 
						role_name, role_alias, internal_ip)
		
		# Delete %role_name%/xx.xx.xx.xx
		full_path = self._base_path + role_name + os.sep		
		self._remove_file(full_path + internal_ip)
		self._remove_dir(full_path)
		
		if role_alias == "mysql":	
			suffix = "master" if self._host_is_replication_master(internal_ip, role_name) else "slave"

			# Delete mysql-(master|slave)/xx.xx.xx.xx
			mysql_path = self._base_path + "mysql-" + suffix + os.sep
			self._remove_file(mysql_path + internal_ip)
			self._remove_dir(mysql_path)		
		else:
			# Delete %role_alias%/xx.xx.xx.xx
			full_path = self._base_path + role_alias + os.sep
			self._remove_file(full_path + internal_ip)
			self._remove_dir(full_path)		
			
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.HOST_UP \
			or message.name == Messages.HOST_DOWN
	