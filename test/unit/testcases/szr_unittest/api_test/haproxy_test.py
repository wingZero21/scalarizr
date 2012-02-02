'''
Created on 26.01.2012

@author: sam
'''
import unittest
import os
import logging

from scalarizr.api import haproxy
from scalarizr.services import haproxy as hap_serv
import shutil

LOG = logging.getLogger(__name__)

TEMP_PATH = '/tmp/haproxy_api.cfg'

class TestHAProxyAPI(unittest.TestCase):

	def __init__(self, methodName='runTest'):
		unittest.TestCase.__init__(self, methodName=methodName)
		#if not os.path.exists(TEMP_PATH):

	def setUp(self):
		shutil.copy(os.path.dirname(__file__) + '/../../../resources/etc/haproxy_api.cfg.backup',
			TEMP_PATH)
		self.api = haproxy.HAProxyAPI(TEMP_PATH)
		self.api.svs.start()
	
	def tearDown(self):
		if os.path.exists(TEMP_PATH):
			self.api.svs.stop()
			os.remove(TEMP_PATH)
	
	def test_create_listener(self):
		protocol='http'
		port=1154
		server_port=2254
		backend='role:1234'
		
		self.api.create_listener(protocol=protocol, port=port, server_port=server_port, 
			backend=backend)
		
		self.assertEqual(self.api.svs.status(), 0)
		
		for el in self.api.cfg.sections('backend:role:1234:http:1154'):
			LOG.debug('backend=`%s`', el)
			path = self.api.cfg.backends[el].xpath
			LOG.debug('children=`%s`, path=`%s`\n\n',
				self.api.cfg.conf.children(path), path)
		
		ln = hap_serv.naming('listener', protocol, port)
		bnd = hap_serv.naming('backend', protocol, port, backend=backend)

		LOG.debug('------listener=`%s`; backend=`%s`------', ln, bnd)

		self.assertEqual(self.api.cfg.backends[bnd]['timeout']['check'], '3s')
		self.assertEqual(self.api.cfg.listener[ln]['mode'], protocol)
		
		self.api.cfg.reload()
		
		self.assertEqual(self.api.cfg.backends[bnd]['timeout']['check'], '3s')
		self.assertEqual(self.api.cfg.listener[ln]['mode'], protocol)

	def test_add_server(self):
		protocol='http'
		port=1154
		server_port=2254
		backend='role:12345'

		self.api.create_listener(protocol=protocol, port=port, server_port=server_port, 
			backend=backend)
		
		ipaddr='248.64.125.158'
		backend='role:12345'

		self.api.add_server(ipaddr=ipaddr, backend=backend)
		
		self.api.cfg.reload()
		
		self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (backend, 
			protocol, port)]['server'][ipaddr.replace('.', '-')], 
			{'address': ipaddr, 'port': str(port), 'check':True})

	def test_remove_server(self):
		protocol='http'
		port=1154
		server_port=2254
		backend='role:12345'
		ipaddr='248.64.125.158'
		backend='role:12345'
		
		self.api.create_listener(protocol=protocol, port=port, server_port=server_port, 
			backend=backend)
		self.api.add_server(ipaddr=ipaddr, backend=backend)
		
		self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (backend, 
			protocol, port)]['server'][ipaddr.replace('.', '-')], 
			{'address': ipaddr, 'port': str(port), 'check':True})

		self.api.remove_server(ipaddr=ipaddr, backend=backend)
		
		try:
			server = self.api.cfg.backends['scalr:backend:%s:%s:%s' % (backend, 
			protocol, port)]['server'][ipaddr.replace('.', '-')]
		except:
			server = None
		self.assertIsNone(server)
	
	def test_delete_listener(self):
		pass

	'''
	def test_configure_healthcheck(self):
		pass

	def test_get_servers_health(self):
		pass

	def test_reset_healthcheck(self):
		pass

	def test_list_listeners(self):
		pass

	def test_list_servers(self):
		pass
	'''

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()