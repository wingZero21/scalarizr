'''
Created on Jan 26th 2011

@author: Dmitry Korsakov
'''

import unittest
import string
import logging
import time

from szr_integtest 				 import get_selenium
from szr_integtest_libs.datapvd  import DataProvider
from szr_integtest_libs.scalrctl import FarmUI
from szr_integtest_libs.ssh_tool import execute
from szr_integtest_libs.scalrctl import	ScalrCtl
from szr_integtest.nginx_test	 import VirtualTest, NginxStartupTest, NginxRestartTest, TerminateTest, RebundleTest

from scalarizr.util import system2
from scalarizr.util.filetool import read_file, write_file 



class StartupTest(NginxStartupTest):
	pvd = None
	server = None

	def test_startup(self):
		self.logger.info("Startup Test")
		
		self.check_startup()
		out = self.curl(self.server.public_ip)
		
		if -1 == string.find(out, 'Scalr farm configured succesfully'):
			raise Exception('Apache is not serving dummy page')
		self.logger.info('Apache is serving proper dummy page')
		
		self.logger.info("Startup test is finished.")
			

class RestartTest(NginxRestartTest):
	app_pvd = None
	server = None
	
	def test_restart(self):
		self.logger.info("Restart Test")
		
		log = self.check_restart()

		log.expect('Requesting virtual hosts list')
		log.expect('Virtual hosts list obtained')
		
		self.logger.info('Virtual host list reloaded')
		self.logger.info("Restart test is finished.")		


class HttpTest(VirtualTest):
	app_pvd = None
	server = None
		
	def test_http(self):
		self.logger.info("HTTP Test")
		domain = 'dima4test.com'
		role_name = self.app_pvd.role_name
		
		ssh = self.server.ssh()
		execute(ssh, "mkdir /var/www/%s" % domain, 15)
		execute(ssh, "echo 'test_http' > /var/www/%s/index.html" % domain, 15)
		
		farmui = FarmUI(get_selenium())
		farmui.configure_vhost(domain, role_name)
		upstream_log = self.server.log.head()
		upstream_log.expect("VhostReconfigure")
		self.logger.info('got VhostReconfigure')
		
		#patch /etc/hosts, use domain instead of ip
		hosts_path = '/etc/hosts'
		hosts_orig = read_file(hosts_path)
		write_file(hosts_path, '\n%s %s\n' % (self.server.public_ip, domain), mode='a')
		
		out = system2("curl %s:80" % domain , shell=True)[0]
		print out
		
		#repair /etc/hosts
		write_file(hosts_path, hosts_orig)

		if -1 == string.find(out, 'test_http'):
			raise Exception('Apache is not serving index.html')
		self.logger.info('Apache is serving proper index.html')
		
		self.logger.info("HTTP test is finished.")
	
	
class HttpsTest(VirtualTest):
	app_pvd = None
	server = None
	
	def test_https(self):
		self.logger.info("HTTPS Test")
		domain = 'dima4test.com'
		role_name = self.app_pvd.role_name
		
		farmui = FarmUI(get_selenium())
		farmui.configure_vhost_ssl(domain, role_name)
		
		upstream_log = self.server.log.head()
		upstream_log.expect("VhostReconfigure")
		self.logger.info('got VhostReconfigure')
		
		out = system2('/usr/bin/openssl s_client -connect %s:443' % self.server.public_ip)
		if -1 == string.find(out, '1 s:/'):
			raise Exception('CA file probably ignored or simply does not exist')
		self.logger.info('cert OK.')
		
		self.logger.info("HTTPS test is finished.")
		
		
class ApacheSuite(unittest.TestSuite):
	
	def __init__(self, tests=(), role_name=None):
		unittest.TestSuite.__init__(self, tests)
		self.logger = logging.getLogger(__name__)
		self.run_tests(role_name)
		
	def run_tests(self, role_name=None):
		self.logger.info("Getting servers, configuring farm")
		kwargs = {'behaviour' : 'www', 'arch' : 'x86_64'}
		if role_name:
			kwargs.update({'role_name': role_name})
		app_pvd = DataProvider(**kwargs)
		self.logger.info("Farm configured")
		
		self.logger.info("Starting load balancer")
		server = app_pvd.server()
		self.logger.info("Load balancer started")
		
		startup = StartupTest('test_startup', pvd=app_pvd, server=server)
		restart = RestartTest('test_restart', app_pvd=app_pvd, server=server)
		http = HttpTest('test_http', app_pvd=app_pvd, server=server)
		https = HttpsTest('test_https', app_pvd=app_pvd, server=server)
		# and test from nginx suite
		rebundle = RebundleTest('test_rebundle', app_pvd=app_pvd, server=server, suite = self)
		terminate = TerminateTest('test_terminate', pvd=app_pvd)
		
		self.addTest(startup)
		self.addTest(restart)
		self.addTest(http)
		self.addTest(https)
		self.addTest(rebundle)
		self.addTest(terminate)
		
		self.logger.info("Number of testes: %s. Starting tests." % self.countTestCases())
		
		
if __name__ == "__main__":
	unittest.main()	