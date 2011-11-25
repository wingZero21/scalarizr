'''
Created on Nov 25, 2011

@author: marat
'''

from scalarizr.rpc import service_method


class StorageAPI(object):

	@service_method
	def create(self):
		pass
	
	@service_method
	def snapshot(self):
		pass
	
	@service_method
	def attach(self):
		pass
	
	@service_method
	def detach(self):
		pass
		
	@service_method
	def destroy(self):
		pass