'''
Created on Oct 06, 2010

@author: shaitanich
'''
import unittest
from szr_integtest import get_selenium, config, MutableLogFile
from szr_integtest_libs import exec_command
from szr_integtest_libs.scalrctl import login
import os
import time
from szr_integtest.roleinit import RoleHandler
from scalarizr.util import wait_until

class ApacheRoleHandler(RoleHandler):
	
	def __init__(self, role_id, role_name, role_opts, run_farm = True):
		self.farm_id = config.get('./test-farm/farm_id')
		self.role_id = role_id
		self.domains = []
		RoleHandler.__init__(self, role_name, role_opts)
		self.sel = get_selenium()
		login(self.sel)
		
	def add_vhost(self, domain, document_root=None, ssl=False, apply_vhost=True):
		ssl_cert = '~/.scalr/apache/server.crt'
		ssl_key = '~/.scalr/apache/server.key'
		ca_cert = '~/.scalr/apache/ca.crt'
		if not document_root:
			document_root = os.path.join('/var/www/', domain)
		self._logger.info("Going to apache_vhost_add.php")
		self.sel.open('/apache_vhost_add.php')		
		self.sel.type('domain_name', domain)
		self.sel.type('farm_target', self.farm_id)
		self.sel.type('role_target', self.role_id)
		if ssl:
			self.sel.check('isSslEnabled')
			self.sel.type('ssl_cert', ssl_cert)
			self.sel.type('ssl_key', ssl_key)
			self.sel.type('ca_cert', ca_cert)
		else:
			self.sel.uncheck('isSslEnabled')
		self.sel.type('document_root_dir', document_root)
		self.sel.type('server_admin', 'admin@%s' % domain)	
		self._logger.info("All information filled.")
		if apply_vhost:
			self._logger.info("Sending vhost")
			self.sel.click('button_js')	
		self.domains.append(domain)
			
	def build_site(self, _channel, domain, document_root):
		self._logger.info("making site dir %s and filling index.html" % document_root)
		exec_command(_channel, 'mkdir %s' % document_root)
		exec_command(_channel, 'echo "Test1" > %s/index.html' % document_root)
	
		self._logger.info("adding %s to server`s /etc/hosts" % domain)	
		exec_command(_channel, 'echo "127.0.0.1 www.%s\n" >> /etc/hosts' % domain)
	
	def test_configure(self):
		_channel = self.ssh.get_root_ssh_channel()
		
		domain = 'dima3.com'
		document_root = os.path.join('/var/www/',domain)
		
		self.build_site(_channel, domain, document_root)
		self.add_vhost(domain, document_root, ssl=True)
		
		log = MutableLogFile(_channel)
		reader = log.tail() 
		self._logger.info("Waiting for 'app reloaded'")
		ret = reader.expect('app reloaded', 45)
		self._logger.info("%s appeared in scalarizr.log", ret.group(0))
		
		cmd_channel = self.ssh.get_root_ssh_channel()
		self._logger.info("Getting site content with curl")
		out = exec_command(cmd_channel, 'curl www.%s' % domain)
		if not 'Test1' in out:
			raise Exception('%s returned data different from expected: %s' % (domain, out))
		
		self._logger.info('%s Ok.' % domain)
		
	def test_configure_ssl(self):
		_channel = self.ssh.get_root_ssh_channel()
			
		domain = 'ssl.dima2.com'
		document_root = os.path.join('/var/www/',domain)
		
		self.build_site(_channel, domain, document_root)
		self.add_vhost(domain, document_root)
		
		log = MutableLogFile(_channel)
		reader = log.tail()
		ret = reader.expect('app reloaded', 45)
		self._logger.info("%s appeared in scalarizr.log", ret.group(0))
		
		cmd_channel = self.ssh.get_root_ssh_channel()
		out = exec_command(cmd_channel, 'curl -k www.%s' % domain)
		if not 'Test1' in out:
			raise Exception('%s returned data different from expected: %s' % (self.domain, out))
		self._logger.info('%s Ok.' % domain)
	
	def cleanup(self):
		self.sel.open('/apache_vhosts_view.php')
		self.sel.wait_for_page_to_load(15000)
		for domain in self.domains:
			time.sleep(3)
			self.sel.click('//em[text()="%s"]/../../dt[last()]/em/input' % domain)	
			self.sel.click('//button[text()="With selected"]')
			self.sel.click('//span[text()="Delete"]')
			self.sel.click('//button[text()="Yes"]')
	
	def shutdown(self):	
		self.farm.terminate()
		self.scalr_ctl.exec_cronjob('ScalarizrMessaging')
			

class TestApacheInit(unittest.TestCase):
	
	def setUp(self):
		role_name = 'Test-app-2010-10-22-1656'
		role_id = '255'
		self.test_role = ApacheRoleHandler(role_id, role_name, {})
	
	def test_init(self):
		sequence = ['HostInitResponse', "Hook on 'service_configured'", "Message 'HostUp' delivered"]
		self.test_role.test_init(sequence)
		self.test_role.scalr_ctl.exec_cronjob('ScalarizrMessaging')
		self.test_role.scalr_ctl.exec_cronjob('ScalarizrMessaging')
		
		self.test_role.test_configure()
		self.test_role.cleanup()
		
		self.test_role.test_configure_ssl()
		self.test_role.cleanup()
	
	def tearDown(self):
		self.test_role.shutdown()
		
	
if __name__ == "__main__":
	unittest.main()