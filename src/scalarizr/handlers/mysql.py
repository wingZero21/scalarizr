'''
Created on 14.06.2010

@author: spike
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler
import logging

def get_handlers ():
	return [MysqlHandler()]

class MysqlMessages:
	CREATE_DATA_BUNDLE = "Mysql_CreateDataBundle"
	CREATE_DATA_BUNDLE_RESULT = "Mysql_CreateDataBundleResult"
	CREATE_BACKUP = "Mysql_CreateBackup"
	CREATE_PMA_USER = "Mysql_CreatePmaUser"
	CREATE_PMA_USER_RESULT = "Mysql_CreatePmaUserResult"
	MASTER_UP = "Mysql_MasterUp"


class MysqlHandler(Handler):
	
	_logger = None
	_queryenv = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		bus.on("init", self.on_init)		
				
	def on_init(self):
		bus.on("before_host_up", self.on_before_host_up)
		
	def on_before_host_up(self, message):
		role_name = None # из config.ini
		role_params = self._queryenv.list_role_params(role_name)
		if role_params["mysql_data_storage_engine"]:
			# Poneslas' pizda po ko4kam
			if "master":
				# Attach ebs # boto
				# Mount ebs # fstool.mount()
				pass
			
			elif "slave" or "eph":
				# MOunt
				pass
			
			if not "$MYSQLD_DATA_DIR":
				# Create data dir structure. Template will be in /etc/scalr/public.d/behaviour.mysql
				pass
		
			if "master":
				message.mysql_repl_user = ""
				message.mysql_repl_password = ""
		pass
	