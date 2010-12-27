'''
Created on Dec 27, 2010

@author: spike
'''
from szr_integtest_libs.providers import DataProvider, Server
from szr_integtest_libs.scalrctl import SshManager
import re

class MysqlDataProvider(DataProvider):
	_master = None
	_slaves = []
	
	def __init__(self, behaviour=None, **kwargs):
		super(DataProvider, self).__init__(self, 'mysql', **kwargs)
	
	def slave(self, index=0):
		'''
		@rtype: Server
		'''
		if len(self.slaves) >= (index + 1):
			return self._slaves[index]
		if not self._master:
			self.master()
		while not len(self.slaves) >= (index + 1):
			server = self._scale_up()
			self._slaves.append(server)			
		return self._slaves[index]
	
	def master(self):
		if self._master:
			return self._master
		self._master = self._scale_up()
		return self._master
		
	def _scale_up(self):
		if not self._master and self.farmui.state == 'terminated':
			self.farmui.remove_all_roles()
			# FIXME: Откуда получать настройки фермы?
			self.farmui.add_role(self.role_name)
			self.farmui.launch()
		out = self.scalr_ctl.exec_cronjob('Scaling')
		result = re.search(self.server_id_re, out)
		if not result:
			raise Exception("Can't create server - farm '%s' hasn't been scaled up." % self.farm_id)
		server_id = result.group('server_id')
		host = self.farmui.get_public_ip(server_id)
		for instance in self.conn.list_nodes():
			if instance.public_ip[0] == host:
				node = instance
				break
			else:
				raise Exception("Can't find node with public ip '%s'" % host)
		key  = self.ssh_config.get('key')
		ssh = SshManager(host, key)
		return Server(node, role_name=self.role_name, ssh=ssh)

	server = master
	
	def sync(self):
		# TODO: 
		pass