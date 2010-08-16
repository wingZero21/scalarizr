'''
Created on 19.01.2010

@author: Dmytro Korsakov
'''
import unittest
from scalarizr.bus import bus
from scalarizr.handlers import nginx
from scalarizr.util import init_tests 
from scalarizr.util.filetool import read_file, write_file
import os
from scalarizr.util import configtool
				
class Test(unittest.TestCase):
	
	def test_reconfigure(self):
		bus.queryenv_service = _QueryEnv()
		
		https_include_path = os.path.realpath(os.path.join(bus.etc_path, "nginx/https.include"))
		https_include = read_file(https_include_path)
		cert_path = configtool.get_key_filename("https.crt", private=True)
		pk_path = configtool.get_key_filename("https.key", private=True)
		cert = read_file(cert_path)
		pk = read_file(pk_path)
		
		print 'Cleaning..'
		for file in (https_include_path, cert_path, pk_path):
			if os.path.exists(file):
				os.remove(file)
				print 'File %s deleted.' % file 
		
		n = nginx.NginxHandler()
		n.on_VhostReconfigure(None)
				
		self.assertTrue(os.path.isfile(https_include_path))
		self.assertTrue(os.path.isfile(cert_path))
		self.assertTrue(os.path.isfile(pk_path))
				
		self.assertEquals(bus.queryenv_service.list_virtual_hosts()[0]['raw']+'\n', https_include)
		self.assertEquals(bus.queryenv_service.get_https_certificate()[0], cert)
		self.assertEquals(bus.queryenv_service.get_https_certificate()[1], pk)
		
		
			
	
	def _test_creating_vhosts(self):
		config = bus.config
		sect_name = configtool.get_behaviour_section_name(nginx.BEHAVIOUR)
		#nginx_incl = bus.etc_path + "/nginx/scalr-vhosts"
		nginx_incl = "/etc/nginx/app-servers.include"
		config.set(sect_name, "app_include_path",nginx_incl)
		if os.path.exists(nginx_incl):
			os.remove(nginx_incl)
		
		bus.queryenv_service = _EmptyQueryEnv()
		n = nginx.NginxHandler()
		n.nginx_upstream_reload()
		
		self.assertTrue(os.path.exists(nginx_incl))
	
	def _test_creating_template(self):
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
		
	def list_virtual_hosts(self, name = None, https=False):
		return [_Bunch(hostname = "test1.net",
						type = "nginx",
						raw= """http {
  server {
    listen          80 default;
    server_name     _;
    access_log      logs/default.access.log main;
 
    server_name_in_redirect  off;
 
    index index.html;
    root  /var/www/default/htdocs;
  }
}""",							
			https = True,
			)]		
		
	def get_https_certificate(self):
		return ("MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN","MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN")
		

class _EmptyQueryEnv:
	def list_roles(self,behaviour):
		return []
			
if __name__ == "__main__":
	init_tests()
	unittest.main()