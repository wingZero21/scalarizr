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

	def createDir(self, d):
		try:
			os.makedirs(d)
		except OSError, x:
			self._logger.error(x)
	
	def createFile(self, file):
		try:
			f = open(file, 'w')
		except OSError, x:
			self._logger.error(x)
		
		try:
			f.close()
		except OSError, x:
			self._logger.error(x)
	
	def removeDir(self, d):
		try:
			os.removedirs(d)
		except OSError, x:
			self._logger.error(x)
	
	def removeFile(self, f):
		try:
			os.remove(d)
		except OSError, x:
			self._logger.error(x)
			
	def HostIsReplicationMaster(self, IP):
		return True
	
	
	def on_HostUp(self, message):
		self._logger.info("host up") 
		
		config = Bus()[BusEntries.CONFIG]
		dir = config.get("handler_posix_ip_list_builder", "base_path")     	    

		if dir[-1] != os.sep:
			dir = dir + os.sep  
		# mySQL checking and additional directory&file creation there
		roleAlias = message.body["RoleAlias"]
		internalIP = message.body["InternalIP"]
		roleName = message.body["RoleName"]
		
		if roleName != "mysql":	
			fullPath = dir + roleName + os.sep
			self.createDir(fullPath)		
			self.createFile(fullPath + internalIP)
		else :
			suffix = "master" if self.HostIsReplicationMaster(internalIP) else "slave"
			
			mysqlPath = dir + roleAlias + "-" + suffix + os.sep
			self.createDir(mysqlPath)		
			self.createFile(mysqlPath + internalIP)
			
			mysqlPath2 = dir + "mysql-" + suffix + os.sep
			self.createDir(mysqlPath2)		
			self.createFile(mysqlPath2 + internalIP)
								
								
	def on_HostDown(self, message):
		self._logger.info("host down")
		
		config = Bus()[BusEntries.CONFIG]
		dir = config.get("handler_posix_ip_list_builder", "base_path")     	    

		if dir[-1] != os.sep:
			dir = dir + os.sep  
		# mySQL checking and additional directory&file creation there
		roleAlias = message.body["RoleAlias"]
		internalIP = message.body["InternalIP"]
		roleName = message.body["RoleName"]
		
		if roleName != "mysql":	
			fullPath = dir + roleName + os.sep		
			self.removeFile(fullPath + internalIP)
			self.removeDir(fullPath)
		else :
			suffix = "master" if self.HostIsReplicationMaster(internalIP) else "slave"
			
			mysqlPath = dir + roleAlias + "-" + suffix + os.sep
			self.removeFile(mysqlPath + internalIP)
			self.removeDir(mysqlPath)		
			
			mysqlPath2 = dir + "mysql-" + suffix + os.sep
			self.removeFile(mysqlPath2 + internalIP)
			self.removeDir(mysqlPath2)		
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == "HostUp" or message.name == "HostDown"
	