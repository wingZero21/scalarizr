'''
Created on Jan 10, 2012

@author: marat
'''

import os
import unittest
import logging
import shutil

from scalarizr.services import haproxy

LOG = logging.getLogger(__name__)
TEMP_PATH = '/tmp/haproxy.cfg'

class TestHAProxyCfg(unittest.TestCase):
	
	def __init__(self, methodName='runTest'):
		unittest.TestCase.__init__(self, methodName=methodName)
		shutil.copy(os.path.dirname(__file__) + '/../../../resources/etc/haproxy.cfg',
					TEMP_PATH)
	def setUp(self):
		self.conf = haproxy.HAProxyCfg(TEMP_PATH)
	
	#def tearDown(self):
	#TODO: remove temp file after all tests
	#	os.remove(TEMP_PATH)
	
	def test_global(self):
		
		self.assertEqual(self.conf['global']['chroot'], '/var/lib/haproxy')
		self.assertTrue('daemon' in self.conf['global'])
	
	def test_global_log(self):
		self.assertEqual(self.conf['global']['log']['127.0.0.1'], 'local2')

	def test_global_stats(self):
		self.assertEqual(self.conf['global']['stats']['socket'], '/var/lib/haproxy/stats')

	def test_defaults(self):
		self.assertTrue(self.conf['defaults']['option']['httplog'])
		
	def test_frontend_main(self):
		self.assertEqual(self.conf['frontend']['main']['bind'], '*:5000')
		self.assertEqual(self.conf['frontend']['main']['default_backend'], 'app')


	def test_backend_app_server(self):
		self.assertEqual(self.conf['backend']['app']['server']['app1'], {'address': '127.0.0.1', 'check': True, 'port': '5001'})
	
	
	def test_backend_server(self):
		temp = []
		for el in self.conf['backend']['app']['server']:
			temp.append(el)
		self.assertEqual(temp, ['app1', 'app2', 'app3', 'app4'])
		
	
	def test_set_backend_app_server_appN(self):
		self.conf['backend']['app']['server']['app2'] = {'address': '127.0.0.1', 'check': True, 'port': '522'}	
		self.assertEqual(self.conf['backend']['app']['server']['app2'],
			{'address': '127.0.0.1', 'check': True, 'port': '522'})


	'''
	def test_set_newsection(self):
		self.conf['backend'] = '192.168.0.1:8080'
		res = []
		for el in self.conf['backend']:
			res.append(el) 
		self.assertTrue('192.168.0.1:8080' in res)
	'''
	
	
	def test_set_listen_name(self):
		temp = {
			'bind': '*:12345', 
			'mode': 'tcp',
			'balance': 'roundrobin',
			'option': {'tcplog': True, 'some_param': True},
			'server' : {'app9': {'address': '127.0.0.1', 'check': True, 'port': '5009'},
						'app10': {'address': '127.0.0.1', 'check': True, 'port': 5010}},
			'default_backend': 'scalr:backend:port:1234',
			'timeout': {'check': '300s'},
			'default-server': {'rise': 100, 'inter': '300s', 'fall': 200}
			}
		
		self.conf['backend']['192.168.0.1:8080'] = temp	
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['bind'], '*:12345')
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['balance'], 'roundrobin')
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['server']['app9'], {'address': '127.0.0.1', 'check': True, 'port': '5009'})
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['option']['tcplog'], True)
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['timeout']['check'], '300s')
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['default-server']['rise'],
						 '100')

	def test_set_backend_app_server(self):
		self.conf['backend']['app']['server'] = {'app_7':{'address': '127.0.0.1', 'check': True, 'port': '5007'}, 
									'app_8':{'address': '127.0.0.1', 'check': True, 'port': '5008'}}
		self.assertEqual(self.conf['backend']['app']['server']['app_7'], {'address': '127.0.0.1', 'check': True, 'port': '5007'})
		self.assertEqual(self.conf['backend']['app']['server']['app_8'], {'address': '127.0.0.1', 'check': True, 'port': '5008'})


	def test_set_global(self):
		temp = {
			'bind': '*:12345', 
			'mode': 'tcp',
			'balance': 'roundrobin',
			'option': {'tcplog': True, 'param': ['value1', 'value2']},
			'default_backend': 'scalr:backend:port:1234'
			}
		self.conf['global'] = temp
		self.assertEqual(self.conf['global']['bind'], '*:12345')


	def test_set_option(self):
		self.conf['global']['opt1'] = ['127.0.0.1', 'test', 'test', 'test']
		self.assertEqual(self.conf['global']['opt1'], ['127.0.0.1', 'test', 'test', 'test'])


	def test_set_backend(self):
		tmp = {
			'bind': '*:12345',
			'mode': 'tcp',
			'balance': 'roundrobin',
			'server': {'app_7':{'address': '127.0.0.1', 'check': True, 'port': '5007'}, 
						'app_8':{'address': '127.0.0.1', 'check': True, 'port': '5008'}},
			'option': {'tcplog': True, 'same_option': 'some value'},
			'default_backend': 'scalr:backend:port:1234'
			}

		self.conf['backend']['app'] = tmp	
		self.assertEqual(self.conf['backend']['app']['mode'], 'tcp')
		self.assertEqual(self.conf['backend']['app']['server']['app_7'],
			{'address': '127.0.0.1', 'check': True, 'port': '5007'})
		self.assertTrue(self.conf['backend']['app']['option']['tcplog'])


	def test_set_backend_server(self):
		tmp = {'app_7':{'address': '127.0.0.1', 'check': True, 'port': '5007'}, 
		 	'app_8':{'address': '127.0.0.1', 'check': True, 'port': '5008'}}
		self.conf['backend']['app']['server'] = tmp
		self.assertEqual(self.conf['backend']['app']['server']['app_7'],
			{'address': '127.0.0.1', 'check': True, 'port': '5007'})


	def test_set_getattr_globals(self):
		temp = {
			'bind': '*:12345', 
			'mode': 'tcp',
			'balance': 'roundrobin',
			'option': {'tcplog': True, 'param': ['value1', 'value2']},
			'default_backend': 'scalr:backend:port:1234'
			}

		self.conf.globals = temp 
		self.assertEqual(self.conf.globals['bind'], '*:12345')


	def test_sections(self):
		list_found = self.conf.sections('scalr:backend:tcp:2254')
		self.assertEqual(list_found[0], 'scalr:backend:role:1234:tcp:2254')
		self.assertEqual(self.conf.backends[list_found[0]]['bind'], '*:2254')
		
		list_found = self.conf.sections('scalr:backend')
		self.assertEqual(list_found, ['scalr:backend:role:1234:tcp:2254'])

	
	def test_save_config(self):
		temp = {
			'bind': '*:12345', 
			'mode': 'tcp',
			'balance': 'roundrobin',
			'option': {'tcplog': True, 'some_param': True},
			'server' : {'app9': {'address': '127.0.0.1', 'check': True, 'port': '5009'},
						'app10': {'address': '127.0.0.1', 'check': True, 'port': 5010}},
			'default_backend': 'scalr:backend:port:1234'
			}
		self.conf['backend']['192.168.0.1:8080'] = temp
		
		#LOG.debug("	cnf['backend']['192.168.0.1:8080']['bind']=`%s`",
		#	self.conf['backend']['192.168.0.1:8080']['bind'])
		#LOG.debug("	bind xpath=`%s`", self.conf['backend']['192.168.0.1:8080'].xpath)
		
		self.conf.save()
		
		self.conf.reload()
		
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['bind'], '*:12345')
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['balance'], 'roundrobin')
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['server']['app9'], {'address': '127.0.0.1', 'check': True, 'port': '5009'})
		self.assertEqual(self.conf['backend']['192.168.0.1:8080']['option']['tcplog'], True)
		
	
	def test_defaults_timeout(self):
		self.assertEqual(self.conf['defaults']['timeout']['http-keep-alive'], '10s')
	
		res = []
		for elem in self.conf['defaults']['timeout']:
			res.append(elem)
		self.assertTrue('server' in res[4])


	def test_set_timeout(self):

		temp = {
			'server': {'app9': {'address': '127.0.0.1', 'check': True, 'port': '5009'}},
			'timeout': {'':'2s','check': '22s', 'client': '5s'},
			}

		self.conf['backend']['scalr:backend:12345'] = temp	

		res = []
		for el in self.conf['backend']['scalr:backend:12345']['timeout']:
			res.append(el)
		self.assertEqual(res, ['', 'client', 'check'])
		#TODO: look at  option_group.__iter__
		self.assertEqual(self.conf['backend']['scalr:backend:12345']['timeout']['check'], '22s')
		
		self.assertEqual(self.conf['backend']['scalr:backend:12345']['timeout'][''], '2s')

		self.conf['backend']['scalr:backend:12345']['timeout'][''] = '12s'
		self.conf['backend']['scalr:backend:12345']['timeout']['check'] = '2s'

		self.assertEqual(self.conf['backend']['scalr:backend:12345']['timeout']['check'], '2s')
		self.assertEqual(self.conf['backend']['scalr:backend:12345']['timeout'][''], '12s')

		#self.conf['backend']['scalr:backend:12345']['timeout'] = '8s'
		#self.assertEqual(self.conf['backend']['scalr:backend:12345']['timeout'][''], '8s')
		
class _TestHAProxyInitScript(unittest.TestCase):

	def test_start(self):
		hap_is = haproxy.HAProxyInitScript()
		if hap_is.status() == 0:
			hap_is.stop()

		hap_is.start()

		pid = hap_is.pid()
		self.assertEqual(True, os.path.exists('/proc/%s'%pid), "Process directory /proc/%s doesn't exist"%pid)
		self.assertEqual(0, hap_is.status(), "Status HAProxy service 'Not running'")


	def test_stop(self):
		hap_is = haproxy.HAProxyInitScript()
		pid = self._get_pid(hap_is)

		hap_is.stop()

		self.assertEqual(os.path.exists(hap_is.pid_file), False, 'pid file %s still exist' % hap_is.pid_file)
		self.assertEqual(os.path.exists('/proc/%s' % pid), False, 'pid directory /proc/%s still exist' % pid)


	def test_restart(self):
		hap_is = haproxy.HAProxyInitScript()
		pid = self._get_pid(hap_is)

		hap_is.restart()

		new_pid = hap_is.pid()

		self.assertNotEqual(pid, new_pid, "old pid value equal current => service doesn't restarted")
		self.assertEqual(os.path.exists('/proc/%s'%new_pid), True,
			'Process /proc/%s after restart not created'%new_pid)


	def test_reload(self):
		hap_is = haproxy.HAProxyInitScript()

		if hap_is.status() != 0:
			hap_is.start()

		pid = hap_is.pid()

		hap_is.reload()

		new_pid = hap_is.pid()

		if new_pid:
			self.assertEqual(os.path.exists('/proc/%s'%new_pid), True, 'Process /proc/%s'\
				' after reload not exist'%new_pid)
			self.assertNotEqual(pid, new_pid, "Service reload not correct. Pid of process"\
				" before reloading not equal it after reloading")
		else:
			raise LookupError('Process HAProxy not started after reload. details:')


	def _get_pid(self, hap_is):
		try:
			pid = hap_is.pid()
		except:
			if hap_is.status() != 0:
				hap_is.start()
			pid = hap_is.pid()
		finally:
			return pid

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()