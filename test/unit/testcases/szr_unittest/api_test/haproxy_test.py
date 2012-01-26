'''
Created on 26.01.2012

@author: sam
'''
import unittest
import os

from scalarizr.api import haproxy
from scalarizr.services import haproxy as hap_serv

class TestHAProxyAPI(unittest.TestCase):

	def setUp(self):
		self.api = haproxy.HAProxyAPI(os.path.dirname(__file__) + \
			'/../../../resources/etc/haproxy.cfg')

	def test_create_listener(self):
		protocol='tcp'
		port=1154
		server_port=2254
		backend='role:1234'

		self.api.create_listener(protocol=protocol, port=port, server_port=server_port, 
			backend=backend)
		ln = hap_serv.naming('listener', protocol, port)
		bnd = hap_serv.naming('backend', protocol, port, backend=backend)

		self.assertEqual(self.api.cfg.listener[ln]['balance'], 'roundrobin')
		self.assertEqual(self.api.cfg.listener[ln]['mode'], 'tcp')

	def test_configure_healthcheck(self):
		pass

	def test_add_server(self):
		self.api.add_server('248.64.125.458', 'role:4321')
		
		self.assertEqual(self.api.cfg.backend['scalr:backend:role:4321:tcp:2254']['server']\
			['248-64-125-458'], {'address': '248.64.125.458', 'port': 2254, 'check':True})
		
	def test_get_servers_health(self):
		pass

	def test_delete_listener(self):
		pass

	def test_reset_healthcheck(self):
		pass

	def test_remove_server(self):
		pass

	def test_list_listeners(self):
		pass

	def test_list_servers(self):
		pass


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()