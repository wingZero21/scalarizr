'''
Created on Jan 5, 2011

@author: marat
'''

import unittest

from szr_integtest import get_selenium
from szr_integtest_libs.datapvd import DataProvider
from szr_integtest_libs.scalrctl import FarmUI
from szr_integtest_libs.szrctl import Scalarizr

class StartupTest(unittest.TestCase):
	farmui = None
	nginx_pvd = None
	
	def __init__(self, methodName='runTest', **kwargs):
		unittest.TestCase.__init__(self, methodName)
		if kwargs:
			for k, v in kwargs.items():
				setattr(self, k, v)
	
	def test(self):
		pass
	
class UpstreamTest(unittest.TestCase):
	farmui = None
	nginx_pvd = None
	app1_pvd = None
	app2_pvd = None

	def test(self):
		pass

class RestartTest(unittest.TestCase):
	nginx_pvd = None
	
	def test(self):
		server = self.nginx_pvd.server()
		log = server.log.tail()		
	
		# Restart scalarizr
		szr = Scalarizr()		
		szr.use(server.ssh())
		szr.restart()
		
		# Check that upstream was reloaded
		log.expect('[pid: \d+] Scalarizr terminated')
		log.expect('[pid: \d+] Starting scalarizr')
		log.expect('Upstream servers:')
		

class NginxSuite(unittest.TestSuite):
	def __init__(self, tests=()):
		nginx_pvd = DataProvider('www')
		app1_pvd = DataProvider('app', arch='i386')
		app2_pvd = DataProvider('app', arch='x86_64')
		farmui = FarmUI(get_selenium())
		
		startup = StartupTest(farmui=farmui, nginx_pvd=nginx_pvd)
		upstream = UpstreamTest(farmui=farmui, app1_pvd=app1_pvd, app2_pvd=app2_pvd)
		restart = RestartTest(nginx_pvd=nginx_pvd)
		
		unittest.TestSuite((
			startup, upstream, restart
		))
