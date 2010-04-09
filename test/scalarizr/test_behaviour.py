'''
@author: Dmytro Korsakov
'''
import unittest
from scalarizr import behaviour

class TestBehaviour(unittest.TestCase):


	def test_AppConfigurator(self):
		B = behaviour.AppConfigurator()
		B.configure(_interactive=True, vhosts_path = "/etc/httpd/scalr-vhosts")
		A = behaviour.WwwConfigurator()
		A.configure(_interactive=True)

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()