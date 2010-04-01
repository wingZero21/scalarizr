'''
@author: Dmytro Korsakov
'''
import unittest

class Test(unittest.TestCase):


	def test_DistTool(self):
		import scalarizr.util.disttool as dt
		self.assertTrue(dt.is_linux())
		self.assertTrue(dt.is_redhat_based())
		self.assertTrue(dt.is_fedora())

		self.assertFalse(dt.is_win())
		self.assertFalse(dt.is_sun())
		self.assertFalse(dt.is_debian_based())


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test']
	unittest.main()