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
from szr_integtest.test_mysql_init import RoleInit
			
class TestApacheInit(unittest.TestCase):
	
	def setUp(self):
		role_name = 'Test_app_2010_10_07_1654'
		sequence = ['HostInitResponse', 'Initializing apache', "Message 'HostUp' delivered"]
		self.test_role = RoleInit(role_name, sequence)
	
	def test_init(self):
		self.test_role.test_init()
	
	def tearDown(self):
		pass
	
if __name__ == "__main__":
	unittest.main()