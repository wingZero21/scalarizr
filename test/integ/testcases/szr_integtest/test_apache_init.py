'''
Created on Oct 06, 2010

@author: shaitanich
'''
import unittest
from szr_integtest import get_selenium, config
from szr_integtest_libs import expect, SshManager, tail_log_channel, exec_command
from szr_integtest_libs.scalrctl import FarmUI, ScalrCtl, login
import logging
import re
from szr_integtest.test_mysql_init import RoleHandler

class ApacheRoleHandler(RoleHandler):
	
	def test_configure(self):
		document_root = '/var/www/dima.com'
		channel = self.ssh.get_root_ssh_channel()
		exec_command(channel, 'mkdir %s' % document_root)
		exec_command(channel, 'echo "Test1" > %s/index.html' % document_root)
		exec_command(channel, 'echo "127.0.0.1 dima.com" >> /etc/hosts')
		sel = get_selenium()
		login(sel)
		sel.open('/apache_vhost_add.php')
		
		sel.type('domain_name', 'dima.com')
		sel.type('farm_target', 'dima@us-east-1')
		sel.uncheck('isSslEnabled')
		sel.type('document_root_dir', document_root)
		sel.type('server_admin', 'admin@dima.com')		
		sel.click('button_js')
		
		out = exec_command(channel, 'curl dima.com')
		if not 'Test1' in out:
			raise Exception('Blablabla')
		
		

class TestApacheInit(unittest.TestCase):
	
	def setUp(self):
		role_name = 'Test-app-2010-10-20-1411'
		self.test_role = ApacheRoleHandler(role_name, {})
	
	def test_init(self):
		sequence = ['HostInitResponse', "Hook on 'service_configured'", "Message 'HostUp' delivered"]
		self.test_role.test_init(sequence)
		self.test_role.test_configure()
	
	def tearDown(self):
		pass
	
if __name__ == "__main__":
	unittest.main()