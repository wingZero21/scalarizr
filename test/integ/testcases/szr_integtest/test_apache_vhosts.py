'''
Created on Oct 8th, 2010

@author: shaitanich
'''
import unittest
from szr_integtest import get_selenium, config
from szr_integtest_libs.scalrctl import login
import os

#TODO: rewrite as usable class
class TestVhosts(unittest.TestCase):


	def setUp(self):
		self.sel = get_selenium()
		login(self.sel)
		self.sel.open('/apache_vhost_add.php')
		self.instance_ip = ''
		

	def tearDown(self):
		pass


	def test_configure(self):
		farm = 'dima@us-east'
		domain = 'dima.com'
		document_root = '/var/www/dima.com/'
		e_mail = 'admin@dima.com'

		self.sel.type('domain_name', domain)
		self.sel.type('farm_target', farm)
		self.sel.uncheck('isSslEnabled')
		self.sel.type('document_root_dir', document_root)
		self.sel.type('server_admin', e_mail)
		self.sel.click('button_js')
		
	def _test_configure_ssl(self):
		farm = 'dima@us-east-1'
		domain = 'ssl.dima.com'
		document_root = '/var/www/ssl.dima.com/'
		ssl_cert = '~/.scalr/apache/ssl_cert'
		ssl_key = '~/.scalr/apache/ssl_key'
		ca_cert = '~/.scalr/apache/ca_cert'

		self.sel.type('domain_name', domain)
		self.sel.type('farm_target', farm)
		self.sel.check('isSslEnabled')
		
		self.sel.type('ssl_cert', ssl_cert)
		self.sel.type('ssl_key', ssl_key)
		self.sel.type('ca_cert', ca_cert)
		
		self.sel.type('document_root_dir', document_root)
		self.sel.click('button_js')
		#TODO: login to server
		index_file = os.path.join(document_root, 'index.html')
		msg = "hello from %s!" % index_file
		vhost_cmd = 'mkdir %s; echo %s > %s' \
				% (document_root, msg, domain)
		hosts_line = "echo '\n127.0.0.1 %s\n' >> /etc/hosts" % domain
		curl_cmd = "curl http://127.0.0.1"
		#TODO:
		#find ip in scaling optput
		#execute vhost_cmd, hosts_line, curl_cmd
		#search in curl output for msg
		#stop the farm, check before_host_down & before_host_terminate

if __name__ == "__main__":
	unittest.main()