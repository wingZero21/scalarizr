'''
Created on Mar 15, 2010

@author: marat
'''

import unittest
import os
from scalarizr.handlers.ec2.rebundle import FileTool, Ec2RebundleHandler, Manifest
from scalarizr.util.fstool import Mtab, Fstab
from scalarizr.messaging.p2p import P2pMessage
from scalarizr.util import init_tests


class Test(unittest.TestCase):
	
	_resources_path = None
	
	def __init__(self, methodName='runTest'):
		unittest.TestCase.__init__(self, methodName)
		self._resources_path = os.path.realpath(os.path.dirname(__file__) + "/../../../resources")
	
	def test_mtab(self):
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
		
		excludes = list(entry.mpoint for entry in mtab.list_entries()  
					if entry.fstype in Mtab.LOCAL_FS_TYPES)
		self.assertTrue("/" in excludes)
		self.assertTrue("/dev/shm" in excludes)
		self.assertTrue("/home" in excludes)
		self.assertTrue(len(excludes), 3)

		
	def _test_fstab(self):
		mtab = Fstab(self._resources_path + "/fstab")
		entries = mtab.list_entries()
		
		self.assertEqual(entries[0].device, "UUID=c5662397-b99a-468c-9a75-bf6cefc260d7")
		self.assertEqual(entries[0].mpoint, "/")
		self.assertEqual(entries[0].fstype, "ext3")
		self.assertEqual(entries[0].options, "defaults")
		self.assertEqual(entries[0].value, "UUID=c5662397-b99a-468c-9a75-bf6cefc260d7 /     ext3    defaults     1 1")

	def _test_fileutil(self):
		filename = self._resources_path + "/fileutil.split"
		f = open(filename, "w")
		f.write("1234567")
		f.close()
		
		part_names = FileTool.split(filename, "fileutil.chunk", 3, self._resources_path)
		self.assertEqual(len(part_names), 3)
		self.assertEqual(os.path.getsize(self._resources_path + "/" + part_names[0]), 3)
		self.assertEqual(os.path.getsize(self._resources_path + "/" + part_names[2]), 1)

	def _test_manifest(self):
		m = Manifest(user="2121212245")
		self.assertTrue(m.user is not None)
		self.assertTrue(m.ec2_encrypted_iv is None)
		
	def test_empty_excludes(self):
		xml = """<?xml version="1.0"?><message id="ad851a7b-6512-45db-b0a5-77f915aada32" name="Rebundle"><meta/><body><role_name>scalarizr-centos5.2</role_name><bundle_task_id>45</bundle_task_id><excludes/></body></message>"""
		msg = P2pMessage()
		msg.fromxml(xml)
		self.assertTrue(msg.excludes is None)
		

if __name__ == "__main__":
	init_tests()
	unittest.main()