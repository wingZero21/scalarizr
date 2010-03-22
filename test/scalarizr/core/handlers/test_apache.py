# -*- coding: latin-1 -*-
'''
Created on 16 марта 2010

@author: shaitanich
'''
import unittest
import os


class Test(unittest.TestCase):


    def test_update_vhost(self):
        from scalarizr.core import Bus, BusEntries
        bus = Bus()
        config = bus[BusEntries.CONFIG]
        config.read( os.path.realpath(os.path.dirname(__file__) + "/../../../../etc/include/handler.apache.ini" ))
        
        class _Bunch(dict):
            __getattr__, __setattr__ = dict.get, dict.__setitem__
            
        class _QueryEnv:
            def list_virtual_hosts(self, name = None, https=None):
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
            
        bus[BusEntries.QUERYENV_SERVICE] = _QueryEnv()
        from scalarizr.core.handlers import apache  
        a = apache.ApacheHandler()
        a.on_VhostReconfigure("")      


if __name__ == "__main__":

    unittest.main()