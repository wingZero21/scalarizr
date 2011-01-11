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
	_master = None
	_slaves = []
	
	def __init__(self, behaviour=None, farm_settings=None, scalr_srv_id=None, **kwargs):
		super(MysqlDataProvider, self).__init__('mysql', farm_settings, **kwargs)
	
	def slave(self, index=0):
		'''
		@rtype: Server
		'''
		if len(self._slaves) >= (index + 1):
			return self._slaves[index]
		if not self._master:			
			self.master()
		while not len(self._slaves) >= (index + 1):
			server = self._scale_up()
			self._slaves.append(server)			
		return self._slaves[index]
	
	def master(self):
		if self._master:
			return self._master
		self._master = self._scale_up()
		return self._master
		
	def _scale_up(self):
		def check_szr_port(host):
			try:
				socket.socket().connect((host, 8013))
				return True
			except:
				return False
			
		if not self._master and self.farmui.state == 'terminated':
			self.farmui.use(self.farm_id)
			self.farmui.remove_all_roles()
			self.farmui.add_role(self.role_name, settings=self.farm_settings)
			self.farmui.save()
			self.farmui.launch()
		out = self.scalrctl.exec_cronjob('Scaling')
		result = re.search(self.server_id_re, out)
		if not result:
			raise Exception("Can't create server - farm '%s' hasn't been scaled up." % self.farm_id)
		server_id = result.group('server_id')
		print server_id
		host = self.farmui.get_public_ip(server_id)
		for instance in self.conn.list_nodes():
			if socket.gethostbyname(instance.public_ip[0]) == host:
				node = instance
				break
		else:
			raise Exception("Can't find node with public ip '%s'" % host)
		
		key = os.path.join(keys_path, self.role_name) + '.pem'
		wait_until(check_szr_port, [host], 5, timeout=60)
		time.sleep(5)
		ssh = SshManager(host, key)
		return Server(node, ssh, role_name=self.role_name, scalr_id=server_id)

	server = master
	
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