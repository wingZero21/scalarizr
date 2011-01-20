'''
Created on Dec 27, 2010

@author: spike
'''
from szr_integtest_libs.datapvd import DataProvider


class MysqlDataProvider(DataProvider):
	
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