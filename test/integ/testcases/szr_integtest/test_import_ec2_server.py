'''
Created on Oct 2, 2010

@author: spike
'''
import unittest

from szr_integtest_libs.scalrctl import FarmUI
from szr_integtest import get_selenium

class TestImportEc2Server(unittest.TestCase):

	def test_import(self):
		farmui = FarmUI(get_selenium())
		
		pass


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_ ']
	unittest.main()