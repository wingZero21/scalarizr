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


    def test_create_hook(self):    
        bus = Bus()
        bus[BusEntries.BASE_PATH] = os.path.realpath(os.path.dirname(__file__) + "/../../..")
        config = ConfigParser()
        config.read(bus[BusEntries.BASE_PATH] + "/etc/config.ini")
        bus[BusEntries.CONFIG] = config
        
        handler = hooks.HooksHandler()
        

if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()