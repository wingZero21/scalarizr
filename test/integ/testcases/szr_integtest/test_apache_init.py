'''
Created on Oct 06, 2010

@author: shaitanich
'''
import unittest
from szr_integtest import get_selenium, config
from szr_integtest_libs import expect, SshManager, tail_log_channel, exec_command
from szr_integtest_libs.scalrctl import FarmUI, ScalrCtl, login
import logging
import os
import re
from szr_integtest.test_mysql_init import RoleHandler

class ApacheRoleHandler(RoleHandler):
	
	def __init__(self, role_name, role_opts):
		self.sel = get_selenium()
		login(self.sel)
		RoleHandler.__init__(self, role_name, role_opts)
	
	def test_configure(self):
		self.domain = 'dima3.com'
		farm_name = 'dima@us-east'
		document_root = os.path.join('/var/www/',self.domain)
		
		channel = self.ssh.get_root_ssh_channel()
		exec_command(channel, 'mkdir %s' % document_root)
		exec_command(channel, 'echo "Test1" > %s/index.html' % document_root)
		exec_command(channel, 'echo "127.0.0.1 www.%s" >> /etc/hosts' % self.domain)

		self.sel.open('/apache_vhost_add.php')		
		self.sel.type('domain_name', self.domain)
		self.sel.type('farm_target', farm_name)
		self.sel.uncheck('isSslEnabled')
		self.sel.type('document_root_dir', document_root)
		self.sel.type('server_admin', 'admin@%s' % self.domain)		
		self.sel.click('button_js')
		
		out = exec_command(channel, 'curl www.%s' % self.domain)
		if not 'Test1' in out:
			raise Exception('%s returned data different from expected: %s' % (self.domain, out))
		
	
	def cleanup(self):		
		if hasattr(self, 'domain'):
			self.sel.open('/apache_vhosts_view.php')
			self.sel.click('//em[text()="%s"]/../../dt[last()]/em/input' % self.domain)	
			self.sel.click('//button[text()="With selected"]')
			self.sel.click('//span[text()="Delete"]')
			self.sel.click('//button[text()="Yes"]')
		self.farm.terminate()
		self.scalr_ctl.exec_cronjob('ScalarizrMessaging')
		
		

class TestApacheInit(unittest.TestCase):
	
	def setUp(self):
		role_name = 'Test-app-2010-10-20-1411'
		self.test_role = ApacheRoleHandler(role_name, {})
	
	def test_init(self):
		sequence = ['HostInitResponse', "Hook on 'service_configured'", "Message 'HostUp' delivered"]
		self.test_role.test_init(sequence)
		self.test_role.scalr_ctl.exec_cronjob('ScalarizrMessaging')
		self.test_role.scalr_ctl.exec_cronjob('ScalarizrMessaging')
		self.test_role.test_configure()
	
	def tearDown(self):
		self.test_role.cleanup()
	
if __name__ == "__main__":
	unittest.main()