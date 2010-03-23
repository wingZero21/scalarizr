# -*- coding: latin-1 -*-
'''
Created on 23 марта 2010

@author: Dmytro Korsakov
'''
import unittest
from scalarizr.util import disttool

class Test(unittest.TestCase):


    def test_DistTool(self):
        A = disttool.DistTool()
        print A.is_debian_based()
        print A.get_version()


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.test']
    unittest.main()