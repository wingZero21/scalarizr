'''
Created on 19.01.2010

@author: Dmytro Korsakov
'''
import unittest
from scalarizr.bus import bus
from scalarizr.handlers import nginx
from scalarizr.util import init_tests
import os
from scalarizr.behaviour import Behaviours
from scalarizr.util import configtool
				
class Test(unittest.TestCase):
	
	def test_creating_vhosts(self):
		config = bus.config
		sect_name = configtool.get_behaviour_section_name(Behaviours.WWW)
		nginx_incl = bus.etc_path + "/nginx/scalr-vhosts"
		config.set(sect_name, "app_include_path",nginx_incl)
		if os.path.exists(nginx_incl):
			os.remove(nginx_incl)
		
		bus.queryenv_service = _EmptyQueryEnv()
		n = nginx.NginxHandler()
		n.nginx_upstream_reload()
		
		self.assertTrue(os.path.exists(nginx_incl))
	
	def test_creating_template(self):
		include_tpl = bus.etc_path + "/public.d/handler.nginx/app-servers.tpl"
		if os.path.exists(include_tpl):
			os.remove(include_tpl)
		
		bus.queryenv_service = _QueryEnv()
		n = nginx.NginxHandler()
		n.nginx_upstream_reload()
		
		self.assertTrue(os.path.exists(include_tpl))

	def _test_nginx_upstream_reload(self):
		pass

class _Bunch(dict):
			__getattr__, __setattr__ = dict.get, dict.__setitem__
			
class _QueryEnv:
	def list_roles(self, behaviour):
		return [_Bunch(
			behaviour = "app",
			name = "nginx",
			hosts = [_Bunch(index='1',replication_master="1",internal_ip="8.8.8.8",external_ip="192.168.1.93")]
			)]

class _EmptyQueryEnv:
	def list_roles(self,behaviour):
		return []
			
if __name__ == "__main__":
	init_tests()
	unittest.main()