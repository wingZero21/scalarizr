'''
Created on Mar 15, 2010

@author: marat
'''

import unittest
import os

class Test(unittest.TestCase):
	
	
	def test_mtab(self):
		from scalarizr.core.handlers.ec2.rebundle import Mtab
		
		mtab = Mtab(os.path.dirname(__file__) + "/../../../../resources/mtab")
		entries = mtab.list_entries()
		
		self.assertEqual(len(entries), 9)
		
		self.assertEqual(entries[0].device, "/dev/sda1")
		self.assertEqual(entries[0].mpoint, "/")
		self.assertEqual(entries[0].fstype, "ext3")
		self.assertEqual(entries[0].options, "rw")
		self.assertEqual(entries[0].value, "/dev/sda1 / ext3 rw 0 0")
		
		self.assertEqual(entries[3].device, "devpts")
		self.assertEqual(entries[3].mpoint, "/dev/pts")
		self.assertEqual(entries[3].fstype, "devpts")
		self.assertEqual(entries[3].options, "rw,gid=5,mode=620")
		self.assertEqual(entries[3].value, "devpts /dev/pts devpts rw,gid=5,mode=620 0 0")
		
	def test_fstab(self):
		from scalarizr.core.handlers.ec2.rebundle import Fstab
		
		mtab = Fstab(os.path.dirname(__file__) + "/../../../../resources/fstab")
		entries = mtab.list_entries()
		
		self.assertEqual(entries[0].device, "UUID=c5662397-b99a-468c-9a75-bf6cefc260d7")
		self.assertEqual(entries[0].mpoint, "/")
		self.assertEqual(entries[0].fstype, "ext3")
		self.assertEqual(entries[0].options, "defaults")
		self.assertEqual(entries[0].value, "UUID=c5662397-b99a-468c-9a75-bf6cefc260d7 /     ext3    defaults     1 1")



if __name__ == "__main__":
	unittest.main()