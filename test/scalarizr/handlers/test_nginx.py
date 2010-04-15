'''
Created on 19.01.2010

@author: Dmytro Korsakov
'''
import unittest
from scalarizr.bus import bus
import sys
import os

class Test(unittest.TestCase):


    def test_nginx_upstream_reload(self):
        config = bus.config  
        config.read( os.path.realpath(os.path.dirname(__file__) + "/../../../../etc/include/behaviour.www.ini"))
        
        #if sys.platform.startswith("freebsd"):
        #    c_parser.set("handler_nginx", "binary_path", "/usr/local/sbin/nginx")
        #    print c_parser.get("handler_nginx","binary_path")       
        #inject_config(config, c_parser)
        
        class _Bunch(dict):
            __getattr__, __setattr__ = dict.get, dict.__setitem__
            
        class _QueryEnv:
            def list_roles(self, behaviour):
                return [_Bunch(
                    behaviour = "app",
                    name = "nginx",
                    hosts = [_Bunch(index='1',replication_master="1",internal_ip="127.0.0.1",external_ip="192.168.1.93")]
                    )]
            
        bus.queryenv_service = _QueryEnv()
        
        from scalarizr.handlers import nginx
        
        n = nginx.NginxHandler()
        n.nginx_upstream_reload()

if __name__ == "__main__":
    unittest.main()