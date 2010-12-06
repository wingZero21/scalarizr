'''
Created on Dec 2, 2010

@author: spike
'''
import os
import unittest
from scalarizr.util import system
from scalarizr.storage import Storage, RaidVolumeProvider

class Test(unittest.TestCase):

	def setUp(self):
		self.vols = []
		for i in range(3):
			system('dd if=/dev/zero of=/tmp/device%s bs=1M count=10' % i)
			self.vols.append(Storage.create(type='loop', file='/tmp/device%s' % i))
		self.snap_vol = self.vols.pop()

	def tearDown(self):
		for vol in self.vols:
			system('losetup -d %s' % vol.devname)
			system('rm -f /tmp/device%s' % vol.devname[-1])
		system('losetup -d %s' % self.snap_vol.devname)
		system('rm -f /tmp/device%s' % self.snap_vol.devname[-1])
		if hasattr(self, 'array'):
			Storage.remove_raid(self.array)
		if hasattr(self, 'new_array'):
			Storage.remove_raid(self.new_array)
			

	def testCreateDestroyRaid(self):
		array = Storage.create_raid(self.vols, 1, 'dbstorage')
		Storage.remove_raid(array)
		
	def testBackupRestoreRaid(self):
		mpoint = '/tmp/mpoint'
		if not os.path.isdir(mpoint):
			os.makedirs(mpoint)
		
		self.array = Storage.create_raid(self.vols, 1, 'dbstorage', snap_pv=self.snap_vol, fstype='ext3')
		self.array.mkfs()
		self.array.mount(mpoint)
		# Create big file
		bigfile_path = os.path.join(mpoint, 'bigfile')
		system('dd if=/dev/random of=%s bs=1M count=5' % bigfile_path)
		md5sum = system(('/usr/bin/md5sum %s' % bigfile_path))[0].strip().split(' ')[0]
		
		array_snap = self.array.snapshot()
		
		self.new_array = Storage.create_from_snapshot(array_snap.id)
		
		new_mpoint = '/tmp/mpoint2'
		if not os.path.isdir(new_mpoint):
			os.makedirs(new_mpoint)
			
		self.new_array.mount(new_mpoint)
		bigfile_path2 = os.path.join(new_mpoint, 'bigfile')
		md5sum2 = system(('/usr/bin/md5sum %s' % bigfile_path2))[0].strip().split(' ')[0]
		self.assertEqual(md5sum, md5sum2)
		
		
		

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()