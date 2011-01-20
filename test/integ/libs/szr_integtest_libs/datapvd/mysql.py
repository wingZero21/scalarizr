'''
Created on Dec 27, 2010

@author: spike
'''
from szr_integtest_libs.datapvd import DataProvider, Server, keys_path
from szr_integtest_libs.scalrctl import SshManager

import time
import os
import re
import socket
from scalarizr.util.filetool import read_file
from scalarizr.util import wait_until

class MysqlDataProvider(DataProvider):
	
	_servers = []	
	
	def __init__(self, behaviour=None, role_settings=None, scalr_srv_id=None, dist=None, **kwargs):
		super(MysqlDataProvider, self).__init__('mysql', role_settings, **kwargs)
	
	def slave(self, index=0):
		'''
		@rtype: Server
		'''
		return self.server(index+1)
		
	def master(self):
		'''
		@rtype: Server
		'''
		return self.server(0)
		
	def sync(self):
		self._master = None
		self._slaves = []
		self.scalrctl.exec_cronjob('ScalarizrMessaging')
		servers = self.farmui.get_mysql_servers(self.role_name)
		all_nodes = self.conn.list_nodes()
		for node in all_nodes:
			public_ip = socket.gethostbyname(node.public_ip[0])
			if (servers['master'] and public_ip == servers['master']) or public_ip in servers['slaves']:
				ssh = SshManager(public_ip, self.ssh_config.get('key'))
				if public_ip == servers['master']:
					self._master = Server(node, ssh, role_name = self.role_name)
				else:
					self._slaves.append(Server(node, ssh, role_name = self.role_name))
					
	def terminate_farm(self):
		self.farmui.terminate()
		time.sleep(5)
		self.scalrctl.exec_cronjob('Poller')
		self._master = None
		self._slaves = []