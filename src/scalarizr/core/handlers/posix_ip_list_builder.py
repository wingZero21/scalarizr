'''
Created on Dec 11, 2009

@author: Dmytro Korsakov
'''
import logging
import os
from scalarizr.core.handlers import Handler

def get_handlers ():
	return [PosixIpListBuilder()]

class PosixIpListBuilder(Handler):
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__package__ + "." + self.__class__.__name__)

	def _create_dir(self, d):
		if not os.path.exists(d):
			try:
				os.makedirs(d)
			except OSError, x:
				self._logger.error(x)
	
	def _create_file(self, file):
		try:
			f = open(file, 'w')
		except OSError, x:
			self._logger.error(x)
		
		try:
			f.close()
		except OSError, x:
			self._logger.error(x)
	
	def _remove_dir(self, d):
		if not os.listdir(d):
			try:
				os.removedirs(d)
			except OSError, x:
				self._logger.error(x)
	
	def _remove_file(self, f):
		try:
			os.remove(d)
		except OSError, x:
			self._logger.error(x)
			
	def _host_is_replication_master(self, IP):
		return True
	
	
	def on_HostUp(self, message):
		self._logger.info("host up") 
		
		config = Bus()[BusEntries.CONFIG]
		dir = config.get("handler_posix_ip_list_builder", "base_path")     	    

		if dir[-1] != os.sep:
			dir = dir + os.sep  
		# mySQL checking and additional directory&file creation there
		role_alias = message.body["RoleAlias"]
		internal_ip = message.body["InternalIP"]
		role_name = message.body["RoleName"]
		
		if role_name != "mysql":	
			full_path = dir + role_name + os.sep
			self._create_dir(full_path)		
			self._create_file(full_path + internal_ip)
		else :
			suffix = "master" if self._host_is_replication_master(internal_ip) else "slave"
			
			mysql_path = dir + role_alias + "-" + suffix + os.sep
			self._create_dir(mysql_path)		
			self._create_file(mysql_path + internal_ip)
			
			mysql_path2 = dir + "mysql-" + suffix + os.sep
			self._create_dir(mysql_path2)		
			self._createfile(mysql_path2 + internal_ip)
								
								
	def on_HostDown(self, message):
		self._logger.info("host down")
		
		config = Bus()[BusEntries.CONFIG]
		dir = config.get("handler_posix_ip_list_builder", "base_path")     	    

		if dir[-1] != os.sep:
			dir = dir + os.sep  
		# mySQL checking and additional directory&file creation there
		role_alias = message.body["RoleAlias"]
		internal_ip = message.body["InternalIP"]
		role_name = message.body["RoleName"]
		
		if role_name != "mysql":	
			full_path = dir + role_name + os.sep		
			self._remove_file(full_path + internal_ip)
			self._remove_dir(full_path)
		else :
			suffix = "master" if self._host_is_replication_master(internal_ip) else "slave"
			
			mysql_path = dir + role_alias + "-" + suffix + os.sep
			self._remove_file(mysql_path + internal_ip)
			self._remove_dir(mysql_path)		
			
			mysql_path2 = dir + "mysql-" + suffix + os.sep
			self._remove_file(mysql_path2 + internal_ip)
			self._remove_dir(mysql_path2)		
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == "HostUp" or message.name == "HostDown"
	