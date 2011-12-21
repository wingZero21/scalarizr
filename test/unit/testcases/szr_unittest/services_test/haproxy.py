'''
Created on Dec 19, 2011

@author: sam
'''
import unittest
import os, sys
import re, signal

from scalarizr.util.initdv2 import Status
from scalarizr.util import system2
from scalarizr.util import wait_until

from scalarizr.services import haproxy

class TestHAProxyInitScript(unittest.TestCase):

	def test_start(self):
		hap_is = haproxy.HAProxyInitScript()
		'''
		try:
			pid = haproxy.get_pid(hap_is.pid_file)
			os.kill(pid, signal.SIGTERM)
			wait_until(lambda: not os.path.exists('/proc/%s' % pid), timeout=self.timeout,
					sleep=0.2, error_text='Trying to start haproxy service.')
			os.kill(pid, signal.SIGKILL)
			os.remove(hap_is.pid_file)
			self.assertEqual(os.path.exists(hap_is.pid_file), False, "HAProxy is still running, not killed.")
			raise
		except:'''
		if hap_is.status() == 0:
			hap_is.stop()
		
		hap_is.start()

		pid = haproxy.get_pid(hap_is.pid_file)
		'''if pid:
				self.assertEqual(os.path.exists('/proc/%s' % pid), True, "Process %s"
					" doesn't exist: %s"%'/proc/%s' % pid)
				if os.path.exists('/proc/%s' % pid):
					try:
						fp = open('/proc/%s/status' % pid)
						status = fp.read()
					except:
						return Status.NOT_RUNNING
					finally:
						fp.close()
					if status:
						pid_state = re.search('State:\s+(?P<state>\w)', status).group('state')
						if pid_state in ('T', 'Z'):
							res = Status.NOT_RUNNING
						else:
							res = Status.RUNNING
				else:
					res = Status.NOT_RUNNING
				
				self.assertEqual(res, 0, "Process status is 'NOT_RUNNING': %s" % res)'''
		self.assertEqual(True, os.path.exists('/proc/%s'%pid), "Process directory /proc/%s doesn't exist"%pid)
		self.assertEqual(0, hap_is.status(), "Status HAProxy service 'Not running'")


	def test_stop(self):
		hap_is = haproxy.HAProxyInitScript()
		pid = _get_pid(hap_is)

		hap_is.stop()

		self.assertEqual(os.path.exists(hap_is.pid_file), False, 'pid file %s still exist' % hap_is.pid_file)
		self.assertEqual(os.path.exists('/proc/%s' % pid), False, 'pid directory /proc/%s still exist' % pid)


	def test_restart(self):
		hap_is = haproxy.HAProxyInitScript()
		pid = _get_pid(hap_is)

		hap_is.restart()

		new_pid = haproxy.get_pid(hap_is.pid_file)

		self.assertNotEqual(pid, new_pid, "old pid value equal current => service doesn't restarted")
		self.assertEqual(os.path.exists('/proc/%s'%new_pid), True, 'Process /proc/%s after restart not created'%new_pid)


	def test_reload(self):
		hap_is = haproxy.HAProxyInitScript()

		if hap_is.status() != 0:
			hap_is.start()
			
		pid = haproxy.get_pid(hap_is.pid_file)

		hap_is.reload()

		new_pid = haproxy.get_pid(hap_is.pid_file)

		if new_pid:
			self.assertEqual(os.path.exists('/proc/%s'%new_pid), True, 'Process /proc/%s after reload not exist'%new_pid)
			self.assertNotEqual(pid, new_pid, "Service reload not correct. Pid of process before reloading not equal it after reloading")
		else:
			raise LookupError('Process HAProxy not started after reload.')


def _get_pid(hap_is):
	try:
		pid = haproxy.get_pid(hap_is.pid_file)
	except:
		if hap_is.status() != 0:
			hap_is.start()
		'''
		system2([hap_is._haproxy, '-f', hap_is._config, '-p', hap_is.pid_file, '-D'], )
		wait_until(lambda: os.path.exists(hap_is.pid_file), timeout=hap_is.timeout,
			sleep=0.2, error_text="HAProxy pid file %s does'not exist" %
			hap_is.pid_file)'''
		pid = haproxy.get_pid(hap_is.pid_file)
	finally:
		return pid


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()