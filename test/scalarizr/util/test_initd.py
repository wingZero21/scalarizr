'''
Created on Jun 17, 2010

@author: marat
'''

import unittest
from scalarizr.util import init_tests, initd, disttool, system
from scalarizr.util.initd import InitdError
import socket
import threading
import time
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer


if disttool.is_redhat_based():
	prog_name = "httpd"
	initd_script = "/etc/init.d/httpd"
	pid_file = "/var/run/httpd/httpd.pid"
else:
	prog_name = "apache"	
	initd_script = "/etc/init.d/apache2"
	pid_file = None

srv_name = "apache"	
initd.explore(srv_name, initd_script, pid_file)

class Test(unittest.TestCase):

	def prog_running(self, prog):
		out = system("ps aux | grep " + prog, shell=True)[0].strip().split("\n")
		print out
		return len(out) > 2

	def _test_start_stop_reload(self):
		if self.prog_running(prog_name):
			initd.stop(srv_name)
			self.assertFalse(self.prog_running(prog_name))
			self.assertFalse(initd.is_running(srv_name))
		
		initd.start(srv_name)
		self.assertTrue(self.prog_running(prog_name))
		self.assertTrue(initd.is_running(srv_name))
		
		try:
			initd.reload(srv_name)
			self.assertTrue(True)
		except InitdError, e:
			self.fail("%s. %s" % (e, e.output))
			
		initd.stop(srv_name)
		self.assertFalse(self.prog_running(prog_name))
		self.assertFalse(initd.is_running(srv_name))

class TestFds(unittest.TestCase):
	def test_start_service(self):
		t = threading.Thread(target=self.open_sockets)
		t.start()
		#initd.start(srv_name)
		system(["/etc/init.d/httpd", "start"], shell=False)
		time.sleep(5)
		
	def open_sockets(self):
		server = HTTPServer(('localhost', 9999), BaseHTTPRequestHandler)
		server.serve_forever()
		
		"""
		# open socket
		sock = socket.socket(socket.AF_INET)
		sock.bind(('0.0.0.0', 9999))
		sock.listen(1)
		while sock.accept():
			pass
		"""
		

if __name__ == "__main__":
	init_tests()
	unittest.main()