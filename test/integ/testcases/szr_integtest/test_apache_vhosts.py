'''
Created on Oct 8th, 2010

@author: shaitanich
'''
import unittest
from szr_integtest import get_selenium, config
from szr_integtest_libs.scalrctl import login, exec_command
from szr_integtest_libs import expect
import os

class TestVhosts(unittest.TestCase):

	def setUp(self):
		self.sel = get_selenium()
		login(self.sel)
		
	def tearDown(self):
		pass

	def _test_configure(self):
		self.domain = 'dima3.com'
		farm_name = 'dima@us-east'
		farm_id = '64'
		role_id = '238'
		document_root = os.path.join('/var/www/',self.domain)
		role_name = 'Test-app-2010-10-20-1411'

		self.sel.open('/apache_vhost_add.php')		
		self.sel.type('domain_name', self.domain)
		self.sel.type('farm_target', farm_id)
		self.sel.type('role_target', role_id)
		self.sel.uncheck('isSslEnabled')
		self.sel.type('document_root_dir', document_root)
		self.sel.type('server_admin', 'admin@%s' % self.domain)		
		self.sel.click('button_js')	
		
	def test_configure_ssl(self):
		farm = 'dima@us-east-1'
		domain = 'ssl.dima2.com'
		farm_id = '64'
		role_id = '238'
		document_root = '/var/www/ssl.dima2.com/'
		ssl_cert = '~/.scalr/apache/server.crt'
		ssl_key = '~/.scalr/apache/server.key'
		ca_cert = '~/.scalr/apache/ca.crt'

		self.sel.open('/apache_vhost_add.php')
		self.sel.type('domain_name', domain)
		self.sel.type('farm_target', farm_id)
		self.sel.type('role_target', role_id)
		self.sel.check('isSslEnabled')
		
		self.sel.type('ssl_cert', ssl_cert)
		self.sel.type('ssl_key', ssl_key)
		self.sel.type('ca_cert', ca_cert)
		
		self.sel.type('document_root_dir', document_root)
		self.sel.type('server_admin', 'admin@%s' % domain)	
		self.sel.click('button_js')


if __name__ == "__main__":
	unittest.main()