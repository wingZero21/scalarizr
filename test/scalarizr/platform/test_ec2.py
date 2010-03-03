'''
Created on Dec 24, 2009

@author: marat
'''
import unittest
from scalarizr.platform.ec2 import get_platform

class Test(unittest.TestCase):


	def test_get_metadata(self):
		platform = get_platform()
		platform._meta_url = "http://ec2-meta-local/"

		meta = platform.get_metadata()
		self.assertFalse(meta is None)
		self.assertEqual(meta["farmid"], "115")
		
		self.assertEqual(platform.get_private_ip(), "10.54.23.73");
		self.assertEqual(platform.get_public_ip(), "154.33.12.55")

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_get_metadata']
	unittest.main()
