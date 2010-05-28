'''
Created on 16.02.2010

@author: Dmytro Korsakov
'''

import unittest
import os
from scalarizr.util import init_tests
from scalarizr.bus import bus
from scalarizr.handlers import apache


class Test(unittest.TestCase):
		
	def test_update_vhost(self):
		class _Bunch(dict):
			__getattr__, __setattr__ = dict.get, dict.__setitem__
			
		class _QueryEnv:
			def list_virtual_hosts(self, name = None, https=False):
				return [_Bunch(hostname = "test-example.scalr.net",
							type = "apache",
							raw = """<VirtualHost *:80> 
DocumentRoot /var/www/1/ 
ServerName test-example.scalr.net 
CustomLog     /var/log/apache3/test-example.scalr.net-access.log1 combined
#  CustomLog     /var/log/apache2/test-example.scalr.net-access.log2 combined
ErrorLog      /var/log/apache3/test-example.scalr.net-error.log3
#ErrorLog      /var/log/apache2/test-example.scalr.net-error.log4#
# ErrorLog      /var/log/apache2/test-example.scalr.net-error.log_5#
#  ErrorLog      /var/log/apache2/test-example.scalr.net-error.log_6_#
# Other directives here 

</VirtualHost> """,
			https = True,
			)]
			def get_https_certificate(self):
				return ("MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN","MIICWjCCAhigAwIBAgIESPX5.....1myoZSPFYXZ3AA9kwc4uOwhN")
			
		bus.queryenv_service = _QueryEnv()
		a = apache.ApacheHandler()
		a.on_VhostReconfigure("")
		
		
if __name__ == "__main__":
	init_tests()
	unittest.main()