'''
Created on Oct 06, 2010

@author: shaitanich
'''
import unittest
from szr_integtest import get_selenium, config
from szr_integtest_libs import expect, SshManager, tail_log_channel
from szr_integtest_libs.scalrctl import FarmUI, exec_cronjob
import logging
import re
from szr_integtest.test_mysql_init import RoleHandler
			
class TestApacheInit(unittest.TestCase):
	
	def setUp(self):
		role_name = 'Test_app_2010_10_07_1654'
		self.test_role = RoleHandler(role_name)
	
	def test_init(self):
		sequence = ['HostInitResponse', 'Initializing apache', "Message 'HostUp' delivered"]
		self.test_role.test_init(sequence)
	
	def tearDown(self):
		pass
	
if __name__ == "__main__":
	unittest.main()