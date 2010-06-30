'''
Created on June 30, 2010

@author: shaitanich
'''
import unittest
from scalarizr.util import fstool		
import os
		

class Test(unittest.TestCase):

	def test_fstab(self):

		fstab_location= os.path.realpath(os.path.dirname(__file__) + "/../../resources/fstab")
		fstab = fstool.Fstab(fstab_location)
		entries = fstab.list_entries()
		
		self.assertEqual(entries[1].device, "/dev/sda1")
		self.assertEqual(entries[1].mpoint, "/")
		self.assertEqual(entries[1].fstype, "ext4")
		self.assertEqual(entries[1].options, "errors=remount-ro")
		self.assertEqual(entries[1].value, "/dev/sda1       /               ext4    errors=remount-ro 0       1")
		

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()