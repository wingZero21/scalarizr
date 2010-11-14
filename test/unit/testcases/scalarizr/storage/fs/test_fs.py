'''
Created on Nov 14, 2010

@author: spike
'''
import unittest
from scalarizr.util import system
from scalarizr.storage.fs.ext import ExtFileSystem
from scalarizr.storage.fs.jfs import JfsFileSystem
from scalarizr.storage.fs.xfs import XfsFileSystem
import os
import re


class FileSystemTest(unittest.TestCase):

	device = None
	img_file = None
	mpoint = None

	def __init__(self, methodName):
		self.img_file = '/tmp/device'
		self.mpoint   = '/mnt/testdir'
		if not os.path.exists(self.mpoint):
			os.makedirs(self.mpoint)
		return unittest.TestCase.__init__(self, methodName)
	
	def setUp(self):
		system("dd if=/dev/zero of=%s bs=1M count=50" % self.img_file)
		self.device, err, rcode = system('losetup -f --show %s' % self.img_file)
		if rcode:
			raise Exception('Error occured during loop device creation.\n\
			                 Return code: %s. Error: %s' % (rcode, err))
		self.device = self.device.strip()
		
	def tearDown(self):
		if self.device:
			system('umount %s' % self.device)
			system('losetup -d %s' % self.device)
			self.device = None			
		system('rm -f %s' % self.img_file)
		
	def test_ext3(self):
		fs = ExtFileSystem()
		fs.mkfs(self.device)
		self.assertFalse(fs.get_label(self.device))
		fs.set_label(self.device, 'testlabel')
		self.assertEqual('testlabel', fs.get_label(self.device))
		self._mount()
		self.assertEqual(self._get_size(), 49574)
		self._grow_partition()
		self._umount()
		fs.resize(self.device)
		self._mount()
		self.assertEqual(self._get_size(), 99384)		
	
	def _grow_partition(self):
		system('dd if=/dev/zero of=%s bs=1M count=50 seek=50' % self.img_file)
		system('losetup -c %s' % self.device)
		
	def _mount(self):
		out,err,rcode = system('mount %s %s' % (self.device, self.mpoint))
		if rcode:
			raise Exception('Error occured during mount operation.\n>>>Out:\n%s,\n>>>Err:\n%s' % (out, err))
	
	def _umount(self):
		out,err,rcode = system('umount %s' % self.device)
		if rcode:
			raise Exception('Error occured during umount operation.\n>>>Out:\n%s,\n>>>Err:\n%s' % (out, err))
		
	def _get_size(self):
		out = system('df')[0]
		res = re.search('%s\s+(?P<size>\d+)' % self.device, out)
		if not res:
			raise Exception('Mount device before trying to get size of it.')
		
		return int(res.group('size'))
		
if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()