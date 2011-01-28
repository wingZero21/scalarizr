'''
Created on Jan 5, 2011

@author: marat
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

from scalarizr.util import ping_socket, wait_until
from scalarizr.util import system2


class VirtualTest(unittest.TestCase):
	
	def __init__(self, methodName='runTest', **kwargs):
		self.logger = logging.getLogger(__name__)
		unittest.TestCase.__init__(self, methodName)
		if kwargs:
			for k, v in kwargs.items():
				setattr(self, k, v)
				
	def runTest(self):
		pass
	
	def check_upstream(self, nginx_ssh, private_hostnames):
		self.logger.info("checking upstream list")
		out = execute(nginx_ssh, 'cat /etc/nginx/app-servers.include', 15)
		self.logger.info(out)
		for ip in private_hostnames:
			if -1 == string.find(out, ip):
				raise Exception('Nginx isn`t streaimg to app instance %s' % ip)
			self.logger.info("%s found in upstream list" % ip)
			

class StartupTest(VirtualTest):

	def test_startup(self):
		self.logger.info("Startup Test")
		
		self.nginx_pvd.wait_for_hostup(self.server)		
		
		ping_socket(self.server.public_ip, 80, exc_str='Nginx is not running on load balancer')
		self.logger.info("Nginx running on 80 port")
		
		self.logger.info("Logging on balancer %s through ssh" % self.server.public_ip)
		self.logger.info("Getting default page from nginx")
		out = system2("curl %s:80" % self.server.public_ip , shell=True)[0]
		print out
		if -1 == string.find(out, 'No running app instances found'):
			raise Exception('Nginx is not serving dummy page')
		self.logger.info('Nginx is serving proper dummy page')
		
		self.logger.info("Startup test is finished.")
		

class RestartTest(VirtualTest):
	nginx_pvd = None
	
	def test_restart(self):
		self.logger.info("Restart Test")
		
		self.nginx_pvd.wait_for_hostup(self.server)
		
		self.logger.info("Logging on balancer through ssh")
		ssh = self.server.ssh()
		
		self.logger.info("Enabling debug log")
		execute(ssh, 'cp /etc/scalr/logging-debug.ini /etc/scalr/logging.ini', 15)
		
		#temporary solution `cause restart triggers "address already in use" error
		self.logger.info("Restarting scalarizr")
		execute(ssh, '/etc/init.d/scalarizr stop', 15)
		time.sleep(10)
		self.logger.info(execute(ssh, 'lsof -i TCP:8013', 15))
		execute(ssh, '/etc/init.d/scalarizr start', 15)
		
		# Check that upstream was reloaded
		self.logger.info("getting log from server")
		log = self.server.log.tail()
		
		log.expect('Scalarizr terminated')
		self.logger.info('Scalarizr terminated')
		
		log.expect('Starting scalarizr')
		self.logger.info('Scalarizr started')
		
		log.expect('Upstream servers:') 
		self.logger.info('upstream reloaded')
		self.logger.info("Restart test is finished.")


class UpstreamTest(VirtualTest):
	app1_pvd = None
	app2_pvd = None	
	server = None

	def test_upstream(self):
		self.logger.info("Upstream Test")

		app1_server = self.app1_pvd.server()
		self.app1_pvd.wait_for_hostup(app1_server)
		self.logger.info('app1_server instance public IP: %s' % app1_server.public_ip)
		
		app2_server = self.app2_pvd.server()
		self.app2_pvd.wait_for_hostup(app2_server)
		self.logger.info('app2_server instance public IP: %s' % app2_server.public_ip)	
		
		log = self.server.log.head()
		log.expect(app1_server.public_ip) 
		log.expect(app2_server.public_ip)

		self.logger.info("Logging on balancer through ssh")
		nginx_ssh = self.server.ssh()
		
		self.logger.info("Getting private IPs")
		app1_private_ip = self.app1_pvd.farmui.get_private_ip(app1_server.scalr_id)
		self.logger.info("App1 private IP: %s" % app1_private_ip)
		app2_private_ip = self.app2_pvd.farmui.get_private_ip(app2_server.scalr_id)
		self.logger.info("App1 private IP: %s" % app1_private_ip)
		
		self.check_upstream(nginx_ssh, [app1_private_ip, app2_private_ip])
		
		self.logger.info("SingleRoleTest")
		
		self.logger.info("changing parameter upstream_app_role")
		sed = 'sed -i "s/upstream_app_role = /upstream_app_role = %s/" /etc/scalr/public.d/www.ini' % self.app1_pvd.role_name
		out = execute(nginx_ssh, sed, 15)
		self.logger.info(out)
		
		execute(nginx_ssh, '/etc/init.d/scalarizr restart', 15)
		
		self.logger.info('Getting server log')
		upstream_log = self.server.log.tail()
		
		self.logger.info('Waiting for upstream reloaded')
		upstream_log.expect("Upstream servers:")
		upstream_log.expect(app1_server.public_ip) 
		upstream_log.expect("Write new /etc/nginx/app-servers.include")
		upstream_log.expect("Normal start")
		self.check_upstream(nginx_ssh, [app1_private_ip])
		
		out = execute(nginx_ssh, 'cat /etc/nginx/app-servers.include', 15)
		
		if -1 != string.find(out, app2_private_ip):
			raise Exception('Nginx is streaimg to wrong app instance %s' % app2_private_ip)

		self.logger.info("Upstream test is finished.")


class HttpsTest(VirtualTest):
	def test_https(self):
		self.logger.info("HTTPS Test")
		domain = 'dima4test.com'
		role_name = self.app1_pvd.role_name
		farmui = FarmUI(get_selenium())
		farmui.configure_vhost_ssl(domain, role_name)
		upstream_log = self.server.log.head()
		upstream_log.expect("VhostReconfigure")
		self.logger.info('got VhostReconfigure')
		out = system2('/usr/bin/openssl s_client -connect %s:443' % self.server.public_ip, shell=True)
		if -1 == string.find(out, '1 s:/'):
			raise Exception('CA file probably ignored or simply does not exist')
		self.logger.info('cert OK.')
		self.logger.info("HTTPS test is finished.")

		
class RebundleTest(VirtualTest):
	server = None
	app_pvd = None
	
	def _is_bundle_process_complete(self):
		self.scalrctl.exec_cronjob('BundleTasksManager', server_id=self.server.scalr_id)
		status = self.pvd.farmui.get_bundle_status(self.server.scalr_id)
		if status == 'failed':
			raise BaseException('Bundle task failed')
		return  status == 'success'
	
	def test_rebundle(self):
		self.logger.info("Rebundle Test")
		self.logger.info("Waiting for HostUp")
		self.pvd.wait_for_hostup(self.server)
		
		self.logger.info("getting log from server")
		reader = self.server.log.tail()
		farmui = self.pvd.farmui
		self.logger.info("Starting bundle process")
		new_role_name = farmui.run_bundle(self.server.scalr_id)
		
		self.scalrctl.exec_cronjob('BundleTasksManager', server_id=self.server.scalr_id)
		
		self.logger.info("Waiting for message 'Rebundle'")
		reader.expect("Received message 'Rebundle'", 60)
		self.logger.info("Received message 'Rebundle'")
		
		reader.expect("Message 'RebundleResult' delivered", 360)
		self.logger.info("Received message 'RebundleResult'")
		
		rebundle_res = self.server.get_message(message_name='RebundleResult')
		self.assertTrue('<status>ok' in rebundle_res)
		
		self.scalrctl.exec_cronjob('ScalarizrMessaging')
		self.scalrctl.exec_cronjob('BundleTasksManager', server_id=self.server.scalr_id)
		wait_until(self._is_bundle_process_complete, None, sleep=15, timeout=180)
		
		'''
		self.server.terminate()
		
		self.logger.info("Running all tests agaen")
		self.suite._tests.remove(self)
		self.suite.run_tests(new_role_name)
		'''
		
		self.logger.info("Rebundle test is finished.")


class TerminateTest(VirtualTest):
	def _hostup_received(self):
			out = self.pvd.scalrctl.exec_cronjob('ScalarizrMessaging')
			return False if -1 == string.find(out, 'HostTerminate') else True
				
	def test_terminate(self):
		self.server.terminate()
		wait_until(self._hostup_received, None, sleep=5, timeout=60)
		

class NginxSuite(unittest.TestSuite):
	def __init__(self, tests=(), role_name=None):
		unittest.TestSuite.__init__(self, tests)
		self.logger = logging.getLogger(__name__)
		self.run_tests(role_name)
		
	def run_tests(self, role_name=None):
		self.logger.info("Getting servers, configuring farm")
		kwargs = {'behaviour' : 'www', 'arch' : 'x86_64'}
		if role_name:
			kwargs.update({'role_name': role_name})
		nginx_pvd = DataProvider(**kwargs)
		self.logger.info("Balancer role name: %s" % nginx_pvd.role_name)
		self.logger.info("Farm configured")
		
		self.logger.info("Starting load balancer")
		server = nginx_pvd.server()
		self.logger.info("Load balancer started")
		
		self.logger.info("Adding app role to farm")
		app1_pvd = DataProvider('app', arch='x86_64', dist='centos5')
		self.logger.info("App role added")
		
		self.logger.info("Adding second role to farm")
		app2_pvd = DataProvider('app', arch='x86_64', dist='ubuntu1004')
		self.logger.info("Second app role added")
		
		appctl=ScalrCtl(nginx_pvd.farm_id)
		
		startup = StartupTest('test_startup', nginx_pvd=nginx_pvd, server=server)
		restart = RestartTest('test_restart', nginx_pvd=nginx_pvd, server=server)
		upstream = UpstreamTest('test_upstream', app1_pvd=app1_pvd, app2_pvd=app2_pvd, server=server)
		https = HttpsTest('test_https', app1_pvd=app1_pvd, nginx_pvd=nginx_pvd, server=server)
		rebundle = RebundleTest('test_rebundle', pvd=nginx_pvd, server=server, scalrctl=appctl, suite = self)
		terminate = TerminateTest('test_terminate', pvd=nginx_pvd, server=server)

		self.addTest(startup)
		self.addTest(restart)
		self.addTest(upstream)
		self.addTest(https)
		self.addTest(rebundle)
		self.addTest(terminate)
		
		self.logger.info("Number of testes: %s. Starting tests." % self.countTestCases())
		
		
if __name__ == "__main__":
	unittest.main()