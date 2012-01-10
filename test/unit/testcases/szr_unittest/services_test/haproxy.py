'''
Created on Dec 19, 2011

@author: sam
'''
import unittest
import os, sys

from scalarizr.services import haproxy

class TestHAProxyInitScript(unittest.TestCase):

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


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()