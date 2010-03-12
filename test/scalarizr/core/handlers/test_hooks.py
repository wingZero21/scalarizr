'''
Created on 04.03.2010

@author: shaitanich
'''
import unittest
from scalarizr.core.handlers import hooks
from scalarizr.core import Bus, BusEntries
from ConfigParser import ConfigParser
import os

class Test(unittest.TestCase):
    
    #_bus = None

    def test_create_hook(self):    
        bus = Bus()
        bus[BusEntries.BASE_PATH] = os.path.realpath(os.path.dirname(__file__) + "/../../../..")
        config = ConfigParser()
        config.read(bus[BusEntries.BASE_PATH] + "/etc/config.ini")
        bus[BusEntries.CONFIG] = config
        
        resources_path = os.path.realpath(os.path.dirname(__file__) + "/../../../" + "resources")
        bus[BusEntries.BASE_PATH] = resources_path
        bus.define_events("init", "test")
        handler = hooks.HooksHandler()
        #bus.on("init", self.on_start)
        bus.fire('init')
        
        bus.fire("test", "parameter1", aenv="some_env")
        self.assertTrue(os.path.exists(resources_path + "/hooks/test_hook_executed"))
        self.assertTrue(os.path.exists(resources_path + "/hooks/parameter1"))
        self.assertTrue(os.path.exists(resources_path + "/hooks/some_env"))


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()