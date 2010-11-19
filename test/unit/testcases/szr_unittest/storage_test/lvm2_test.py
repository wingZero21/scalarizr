'''
Created on Nov 19, 2010

@author: shaitanich
'''
import unittest
from scalarizr.util import system
from scalarizr.storage.lvm2 import Lvm2
import os

class Test(unittest.TestCase):


	def setUp(self):
		self.group_name = 'vg_test'
		self.loop_devname = '/dev/loop1'
		self.loop_file = 'test.img'
		
		if not os.path.exists(self.loop_file):
			cmd = 'dd if=/dev/zero of=%s bs=1024 count=100000'%self.loop_file
			system(cmd.split(), shell=False)
		
		system('losetup %s %s'% (self.loop_devname, self.loop_file))
		self.lvm = Lvm2(self.group_name)
		

	def tearDown(self):
		pass


	def testLvm2(self):
		self.lvm.add_physical_volumes(self.loop_devname)
		self.assertTrue(self.lvm.get_physical_volumes())
		self.assertTrue(self.loop_devname in self.lvm.get_physical_volumes())
		
		self.lvm.create_volume_group(self.group_name, '16M', self.loop_devname)	
		self.assertTrue(self.group_name in self.lvm.get_volume_groups())	
		
		self.lvm.remove_physical_volume(self.loop_devname)
		self.assertFalse(self.loop_devname in self.lvm.get_physical_volumes())
					
		self.lvm.remove_volume_group()
		self.assertTrue(self.group_name in self.lvm.get_volume_groups())
		

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()