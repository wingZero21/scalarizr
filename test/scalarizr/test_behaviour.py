'''
@author: Dmytro Korsakov
'''
import os
import sys
import unittest
import ConfigParser
from scalarizr.bus import bus
from scalarizr import behaviour
from scalarizr.util import init_tests


class TestBehaviour(unittest.TestCase):
	
	def setUp(self):
		bus.etc_path = os.path.realpath(os.path.dirname(__file__) + "/../resources/etc")
		self.answers = os.path.dirname(__file__) + "/../resources/answers_behaviour.txt"
		sys.stdin = file(self.answers)

	def test_AppConfigurator(self):
		A = behaviour.AppConfigurator()
		A.configure(_interactive=True, vhosts_path = "/etc/httpd/scalr-vhosts")
		config = ConfigParser.ConfigParser()
		config.read(bus.etc_path+'/public.d/behaviour.app.ini')
		vhosts_path = config.get('behaviour_app','vhosts_path')
		self.assertEquals(vhosts_path,'/etc/httpd/scalr-vhosts')
		httpd_conf_path = config.get('behaviour_app','httpd_conf_path')
		self.assertEquals(httpd_conf_path,'/etc/apache2/apache2.conf')
		
	def test_WwwConfigurator(self):
		W = behaviour.WwwConfigurator()
		W.configure(_interactive=True)
		config = ConfigParser.ConfigParser()
		config.read(bus.etc_path+'/public.d/behaviour.www.ini')
		app_include_path = config.get('behaviour_www','app_include_path')
		binary_path = config.get('behaviour_www','binary_path')
		https_include_path = config.get('behaviour_www','https_include_path')
		app_port = config.get('behaviour_www','app_port')
		self.assertEquals(app_include_path,'/etc/nginx/app-servers.include')
		self.assertEquals(binary_path,'/usr/sbin/nginx')
		self.assertEquals(https_include_path,'/etc/nginx/https.include')
		self.assertEquals(app_port,'80')
		

if __name__ == "__main__":
	init_tests()
	
	unittest.main()