'''
Created on Jan 10, 2012

@author: marat
'''

import os
import unittest

from scalarizr.services import haproxy


class TestHAProxyCfg2(unittest.TestCase):
	def setUp(self):
		self.conf = haproxy.HAProxyCfg2(os.path.dirname(__file__) + '/../../../resources/etc/haproxy.cfg')
	

	def test_global(self):
		self.assertTrue('daemon' in self.conf['global'])
		self.assertEqual(self.conf['global']['chroot'], '/var/lib/haproxy')
		self.assertEqual(self.conf['global']['log'], ['127.0.0.1', 'local2'])
		
	def test_defaults(self):
		self.assertEqual(self.conf['defaults']['timeout']['http-keep-alive'], '10s')
		self.assertTrue('server' in self.conf['defaults']['timeout'])
		self.assertTrue(self.conf['defaults']['option']['httplog'])
		
	def test_frontend(self):
		self.assertEqual(self.conf['frontend']['main']['bind'], '*:5000')
		self.assertEqual(self.conf['frontend']['main']['default_backend'], 'app')
		
	def test_backend(self):
		self.assertEqual(self.conf['backend']['app']['server']['app1'], ['127.0.0.1:5001', 'check'])
		
		
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
		self.assertEqual(os.path.exists('/proc/%s'%new_pid), True, 'Process /proc/%s after restart not created'%new_pid)


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
