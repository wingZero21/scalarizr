'''
Created on Dec 11, 2009

@author: Dmytro Korsakov
@author: marat
'''

# TODO: add onRebootStart - delete onRebootFinish - restore
import logging
import os
from scalarizr.core.handlers import Handler
from scalarizr.core import Bus, BusEntries

def get_handlers ():
	return [PosixIpListBuilder()]

class PosixIpListBuilder(Handler):
	_logger = None
	_base_path = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		
		config = Bus()[BusEntries.CONFIG]
		self._base_path = config.get("handler_posix_ip_list_builder", "base_path")     	    
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
			
	def _host_is_replication_master(self, ip):
		# TODO: use queryenv !
		return True
	
	
	def on_HostUp(self, message):
		self._logger.debug("Entering host up...") 
		
		role_alias = message.body["RoleAlias"]
		internal_ip = message.body["InternalIP"]
		role_name = message.body["RoleName"]

		self._logger.info("Add role host (role_name: %s, role_alias: %s, ip: %s)", 
						role_name, role_alias, internal_ip)
		
		# Create %role_name%/xx.xx.xx.xx
		full_path = self._base_path + role_name + os.sep
		self._create_dir(full_path)		
		self._create_file(full_path + internal_ip)
		
		if role_alias == "mysql": 
			suffix = "master" if self._host_is_replication_master(internal_ip) else "slave"
			
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
			suffix = "master" if self._host_is_replication_master(internal_ip) else "slave"

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
		return message.name == "HostUp" or message.name == "HostDown"
	