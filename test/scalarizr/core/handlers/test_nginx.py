'''
Created on 19.01.2010

@author: Dmytro Korsakov
'''
import unittest


class Test(unittest.TestCase):


    def test_nginx_upstream_reload(self):
        from scalarizr.core import Bus, BusEntries
        bus = Bus()
        config = bus[BusEntries.CONFIG]
        
        import ConfigParser
        from scalarizr.util import inject_config
        c_parser = ConfigParser.ConfigParser()
        c_parser.read('/home/shaitanich/workspace/scalarizr-trunk/etc/include/handler.nginx.ini')
        
        import sys
        if sys.platform.startswith("freebsd"):
            c_parser.set("handler_nginx", "binary_path", "/usr/local/sbin/nginx")
            print c_parser.get("handler_nginx","binary_path")
            
        inject_config(config, c_parser)
        
        class _Bunch(dict):
            __getattr__, __setattr__ = dict.get, dict.__setitem__
            
        class _QueryEnv:
            def list_roles(self, behaviour):
                return [_Bunch(
                    behaviour = "app",
                    name = "nginx",
                    hosts = [_Bunch(index='1',replication_master="1",internal_ip="127.0.0.1",external_ip="192.168.1.93")]
                    )]
            
        bus[BusEntries.QUERYENV_SERVICE] = _QueryEnv()
        
        from scalarizr.core.handlers import nginx
        
        n = nginx.NginxHandler()
        n.nginx_upstream_reload()

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()