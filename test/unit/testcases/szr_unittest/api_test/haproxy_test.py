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
		
		self.protocol='http'
		self.port=1154
		self.server_port=2254
		self.backend='role:1234'
		self.ipaddr='248.64.125.158'		
		
	def setUp(self):
		shutil.copy(os.path.dirname(__file__) + '/../../../resources/etc/haproxy_api.cfg.backup',
			TEMP_PATH)
		self.api = haproxy.HAProxyAPI(TEMP_PATH)
		self.api.svs.start()
	
	def tearDown(self):
		if os.path.exists(TEMP_PATH):
			self.api.svs.stop()
			#os.remove(TEMP_PATH)
	
	def test_create_listener(self):
		self.api.create_listener(protocol=self.protocol, port=self.port,
								 server_port=self.server_port, backend=self.backend)
		
		self.assertEqual(self.api.svs.status(), 0)
		
		ln = hap_serv.naming('listen', self.protocol, self.port)
		bnd = hap_serv.naming('backend', self.protocol, self.port, backend=self.backend)

		self.assertEqual(self.api.cfg.backends[bnd]['timeout']['check'], '3s')
		self.assertEqual(self.api.cfg.listener[ln]['mode'], self.protocol)
		
		self.api.cfg.reload()
		
		self.assertEqual(self.api.cfg.backends[bnd]['timeout']['check'], '3s')
		self.assertEqual(self.api.cfg.listener[ln]['mode'], self.protocol)
		#TODO: add some server with another default-server and wait for exception

	def test_delete_listener(self):
		
		self.api.create_listener(protocol=self.protocol, port=self.port, 
				server_port=self.server_port, backend=self.backend)
		self.assertEqual(self.api.cfg.listener['scalr:listen:%s:%s' %\
				(self.protocol, self.port)]['mode'], self.protocol)
		self.assertEqual(self.api.cfg.sections('scalr:listen:%s:%s' % (self.protocol,
				 self.port)), ['scalr:listen:%s:%s' % (self.protocol, self.port)])
		self.assertEqual(self.api.cfg.listener['scalr:listen:%s:%s' % (self.protocol, self.port)]\
				['default_backend'], 'scalr:backend:%s:%s:%s' % (self.backend, self.protocol, self.port))

		self.api.delete_listener(port=self.port, protocol=self.protocol)

		self.assertEqual(self.api.cfg.sections('scalr:listen:%s:%s' % (self.protocol, self.port)), [])
		self.assertEqual(self.api.cfg.sections('scalr:backend:%s:%s:%s' %\
						(self.backend, self.protocol, self.port)), [])

	def test_add_server(self):
		self.api.create_listener(protocol=self.protocol, port=self.port, server_port=self.server_port, 
			backend=self.backend)

		self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)

		self.api.cfg.reload()
		self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend, 
			self.protocol, self.port)]['server'][self.ipaddr.replace('.', '-')], 
			{'address': self.ipaddr, 'port': str(self.port), 'check':True})

	def test_remove_server(self):
		self.api.create_listener(protocol=self.protocol, port=self.port, server_port=self.server_port, 
			backend=self.backend)
		self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)

		self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend, 
			self.protocol, self.port)]['server'][self.ipaddr.replace('.', '-')], 
			{'address': self.ipaddr, 'port': str(self.port), 'check':True})

		self.api.remove_server(ipaddr=self.ipaddr, backend=self.backend)

		try:
			server = self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend, 
			self.protocol, self.port)]['server'][self.ipaddr.replace('.', '-')]
		except:
			server = None
		self.assertIsNone(server)

	def test_configure_healthcheck(self):
		flag = True
		try:
			self.api.configure_healthcheck(target='http:14080', 
										interval='5s', 
										timeout={'check': '3s'}, 
										fall_threshold=2, 
        								rise_threshold=10)
			flag = False
		except Exception, e:
			LOG.debug('Backend not exist. Details: %s', e)
			self.assertTrue(flag)

		self.api.create_listener(protocol=self.protocol, port=self.port, server_port=self.server_port, 
			backend=self.backend)
		
		self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)
		self.api.add_server(ipaddr='218.124.68.210', backend=self.backend)
		
		self.api.configure_healthcheck(target='%s:%s' % (self.protocol, self.port), 
										interval='5s', 
										timeout={'check': '5s'}, 
										fall_threshold=20, 
        								rise_threshold=100)
		
		self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend, 
			self.protocol, self.port)]['default-server'], {'inter': '5s', 'rise': '100', 'fall': '20'})
		self.assertTrue(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend, 
			self.protocol, self.port)]['server'][self.ipaddr.replace('.','-')]['check'])

	def test_reset_healthcheck(self):
		self.api.create_listener(protocol=self.protocol, port=self.port, server_port=self.server_port, 
			backend=self.backend)
		
		self.api.add_server(ipaddr=self.ipaddr, backend=self.backend)
		self.api.add_server(ipaddr='218.124.68.210', backend=self.backend)
		
		self.api.configure_healthcheck(target='%s:%s' % (self.protocol, self.port), 
										interval='5s', 
										timeout={'check': '5s'}, 
										fall_threshold=20, 
        								rise_threshold=100)
		
		self.api.reset_healthcheck(target='%s:%s' % (self.protocol, self.port))
		
		self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend, 
			self.protocol, self.port)]['default-server'], {'inter': '30s', 'rise': '10', 'fall': '2'})
		self.assertEqual(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend, 
			self.protocol, self.port)]['timeout']['check'], '3s')
		#self.assertTrue(self.api.cfg.backends['scalr:backend:%s:%s:%s' % (self.backend, 
		#	self.protocol, self.port)]['server'][self.ipaddr.replace('.','-')]['check'])


	'''
	def test_get_servers_health(self):
		pass

	def test_list_servers(self):
		pass
	
	def test_list_listeners(self):
		pass
	'''

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()