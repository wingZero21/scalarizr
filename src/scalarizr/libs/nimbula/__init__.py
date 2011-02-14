'''
Created on Feb 14th, 2011

@author: Dmytro Korsakov
'''

#env['NIMBULA_USERNAME']
#env['NIMBULA_PASSWORD']

class NimbulaConnection:
	def __init__(self, username=None, password=None):
		pass
		
	def add_machine_image(self, name, file=None, fp=None, attributes=None, account=None):
		''' 
		@param name: base name (ex: apache-deb5-20110217), autocomplete customer/user from self.username 
		@param file: file name
		@param fp: file-like object. One of `file` or `fp` should be provided
		'''
		pass
		
	def get_machine_image(self, name):
		pass
		
	def delete_machine_image(self, name):
		pass
		
	def discover_machine_image(self, container=None):
		pass

	
class NimbulaError(BaseException):