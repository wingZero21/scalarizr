'''
Created on 26.01.2012

@author: sam
'''
import unittest
import os
import logging

from scalarizr.api import haproxy
from scalarizr.services import haproxy as hap_serv

LOG = logging.getLogger(__name__) 

class TestHAProxyAPI(unittest.TestCase):

	def setUp(self):
		self.api = haproxy.HAProxyAPI(os.path.dirname(__file__) + \
			'/../../../resources/etc/haproxy_api.cfg')

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
		
		self.api.cfg.reload()
		
		self.assertEqual(self.api.cfg.listener[ln]['balance'], 'roundrobin')
		LOG.info(self.api.cfg.listener[ln]['balance'])
		LOG.info(self.api.cfg.listener[ln].xpath)
		self.assertEqual(self.api.cfg.listener[ln]['mode'], 'tcp')

	def test_configure_healthcheck(self):
		pass

	def test_add_server(self):
		ipaddr='248.64.125.458'
		backend='role:1234'
		self.api.add_server(ipaddr, backend)
		
		self.assertEqual(self.api.cfg.backend['scalr:backend:%s:tcp:2254' % backend]['server']\
			[ipaddr.replace('.', '-')], {'address': ipaddr, 'port': 2254, 'check':True})
		
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