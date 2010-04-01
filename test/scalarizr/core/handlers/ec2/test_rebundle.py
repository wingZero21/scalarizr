'''
Created on Mar 15, 2010

@author: marat
'''

import unittest
import os
from scalarizr.core.handlers.ec2.rebundle import FileTool

class Test(unittest.TestCase):
	
	_resources_path = None
	
	def __init__(self, methodName='runTest'):
		unittest.TestCase.__init__(self, methodName)
		self._resources_path = os.path.realpath(os.path.dirname(__file__) + "/../../../../resources")
	
	def test_mtab(self):
		from scalarizr.core.handlers.ec2.rebundle import Mtab
		
		mtab = Mtab(self._resources_path + "/mtab")
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
		
		mtab = Fstab(self._resources_path + "/fstab")
		entries = mtab.list_entries()
		
		self.assertEqual(entries[0].device, "UUID=c5662397-b99a-468c-9a75-bf6cefc260d7")
		self.assertEqual(entries[0].mpoint, "/")
		self.assertEqual(entries[0].fstype, "ext3")
		self.assertEqual(entries[0].options, "defaults")
		self.assertEqual(entries[0].value, "UUID=c5662397-b99a-468c-9a75-bf6cefc260d7 /     ext3    defaults     1 1")

	def test_fileutil(self):
		filename = self._resources_path + "/fileutil.split"
		f = open(filename, "w")
		f.write("1234567")
		f.close()
		
		part_names = FileTool.split(filename, "fileutil.chunk", 3, self._resources_path)
		self.assertEqual(len(part_names), 3)
		self.assertEqual(os.path.getsize(self._resources_path + "/" + part_names[0]), 3)
		self.assertEqual(os.path.getsize(self._resources_path + "/" + part_names[2]), 1)

	def test_manifest(self):
		from scalarizr.core.handlers.ec2.rebundle import Manifest
		m = Manifest(user="2121212245")
		self.assertTrue(m.user is not None)
		self.assertTrue(m.ec2_encrypted_iv is None)

if __name__ == "__main__":
	unittest.main()