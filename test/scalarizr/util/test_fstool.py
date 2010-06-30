'''
Created on Jun 30, 2010

@author: marat
'''

import os, shutil
import unittest
from scalarizr.util import fstool, init_tests

class Test(unittest.TestCase):

	def __init__(self, methodName='runTest'):
		unittest.TestCase.__init__(self, methodName)
		self._resources_path = os.path.realpath(os.path.dirname(__file__) + "/../../resources")
		self._mtab_path = self._resources_path + "/mtab"
		self._fstab_path = self._resources_path + "/fstab"

	def tearDown(self):
		try: os.remove(self._mtab_path)
		except: pass
		
		try: os.remove(self._fstab_path)
		except: pass

	def _new_fstab(self):
		shutil.copy(self._fstab_path + ".orig", self._fstab_path)
		return fstool.Fstab(self._fstab_path)		
	
	def _new_mtab(self):
		shutil.copy(self._mtab_path + ".orig", self._mtab_path)
		return fstool.Mtab(self._mtab_path)

	def test_mtab(self):
		mtab = self._new_mtab()
		entries = mtab.list_entries()
		
		self.assertEqual(len(entries), 9)
		
		self.assertEqual(entries[0].devname, "/dev/sda1")
		self.assertEqual(entries[0].mpoint, "/")
		self.assertEqual(entries[0].fstype, "ext3")
		self.assertEqual(entries[0].options, "rw")
		self.assertEqual(entries[0].value, "/dev/sda1 / ext3 rw 0 0")
		
		self.assertEqual(entries[3].devname, "devpts")
		self.assertEqual(entries[3].mpoint, "/dev/pts")
		self.assertEqual(entries[3].fstype, "devpts")
		self.assertEqual(entries[3].options, "rw,gid=5,mode=620")
		self.assertEqual(entries[3].value, "devpts /dev/pts devpts rw,gid=5,mode=620 0 0")
		
		excludes = list(entry.mpoint for entry in mtab.list_entries()  
					if entry.fstype in fstool.Mtab.LOCAL_FS_TYPES)
		self.assertTrue("/" in excludes)
		self.assertTrue("/dev/shm" in excludes)
		self.assertTrue("/home" in excludes)
		self.assertTrue(len(excludes), 3)

		
	def test_fstab_read(self):
		fstab = self._new_fstab()
		
		entries = fstab.list_entries()
		self.assertEqual(entries[0].devname, "UUID=c5662397-b99a-468c-9a75-bf6cefc260d7")
		self.assertEqual(entries[0].mpoint, "/")
		self.assertEqual(entries[0].fstype, "ext3")
		self.assertEqual(entries[0].options, "defaults")
		self.assertEqual(entries[0].value, "UUID=c5662397-b99a-468c-9a75-bf6cefc260d7 /     ext3    defaults     1 1")
		
		self.assertTrue(fstab.contains("proc"))
		self.assertFalse(fstab.contains("/dev/sdn"))
		self.assertTrue(fstab.contains(mpoint="/"))
		self.assertFalse(fstab.contains(mpoint="/mnt"))
		
		self.assertFalse(fstab.contains(devname="UUID=9cc535dd-9a2c-4504-8f5f-1bd0b91a0086", mpoint="/non-existed"))

	def test_fstab_modify(self):
		fstab = self._new_fstab()
		fstab.append("/dev/sdo", "/mnt/dbstorage", "ext4")
		
		# Check that it was added
		self.assertTrue(fstab.contains("/dev/sdo"))

		# Check that it was written to disk
		fstab2 = fstool.Fstab(self._fstab_path)
		self.assertTrue(fstab2.contains("/dev/sdo"))
		
		
		fstab.remove("UUID=9cc535dd-9a2c-4504-8f5f-1bd0b91a0086")
		# Check that it was removed
		self.assertFalse(fstab.contains("UUID=9cc535dd-9a2c-4504-8f5f-1bd0b91a0086"))
		
		# Check that it was written to disk		
		fstab2 = fstool.Fstab(self._fstab_path)
		self.assertFalse(fstab2.contains("UUID=9cc535dd-9a2c-4504-8f5f-1bd0b91a0086"))
		
		

if __name__ == "__main__":
	init_tests()
	unittest.main()