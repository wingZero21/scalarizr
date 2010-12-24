'''
Created on Dec 24, 2010

@author: spike
'''

import unittest
from szr_integtest import DataProvider 

class ImportEc2Test(unittest.TestCase):
	tags = ['ec2', 'import']
	
	def _init_server(self, root_device_type):
		# Init import with params
		dp = DataProvider(behaviour='raw', root_device_type=root_device_type)
		server = dp.server()
		# 
		
		pass

	def test_instance_store(self):
		server = self._init_server('instance_store')
		reader = server.log.head()
		# asserts
		pass
	
	def test_ebs(self):
		server = self._init_server('ebs')
		# asserts
		pass
	
