'''
Created on Dec 2, 2010

@author: spike
'''
import os
import unittest
from scalarizr.util import system
from scalarizr.bus import bus
from szr_unittest import init_platform
from scalarizr.storage import Storage, RaidVolumeProvider
from scalarizr.platform.ec2.storage import EbsVolumeProvider

class Test(unittest.TestCase):

	def setUp(self):
		self.vols = []
		for i in range(3):
			#system('dd if=/dev/zero of=/tmp/device%s bs=1M count=10' % i)
			#self.vols.append(Storage.create(type='loop', file='/tmp/device%s' % i))
			self.vols.append(Storage.create(type='ebs', size=1, zone='us-east-1a'))
		self.snap_vol = self.vols.pop()
		
	def tearDown(self):
		if hasattr(self, 'array') and self.array.devname:
			self.array.destroy(remove_disks=True)
		if self.snap_vol.devname:
			self.snap_vol.destroy()

	def _testCreateDestroyRaid(self):
		self.array = Storage.create(type='raid', disks=self.vols, level=1, vg='dbstorage')
		self.assertTrue(os.path.exists(self.array.raid_pv))
		self.array.destroy(remove_disks=True)
		
	def _testBackupRestoreRaid(self):
		mpoint = '/tmp/mpoint'
		if not os.path.isdir(mpoint):
			os.makedirs(mpoint)
		
		self.array = Storage.create(type='raid', disks=self.vols, level=1, vg='dbstorage', snap_pv=self.snap_vol, fstype='ext3')

		self.array.mkfs()
		self.array.mount(mpoint)
		# Create big file
		bigfile_path = os.path.join(mpoint, 'bigfile')
		system('dd if=/dev/random of=%s bs=1M count=5' % bigfile_path)
		md5sum = system(('/usr/bin/md5sum %s' % bigfile_path))[0].strip().split(' ')[0]

		array_snap = self.array.snapshot()
		self.array.destroy(remove_disks=True)
		
		self.array = Storage.create(snapshot=array_snap)
		
		new_mpoint = '/tmp/mpoint2'
		if not os.path.isdir(new_mpoint):
			os.makedirs(new_mpoint)
			
		self.array.mount(new_mpoint)
		bigfile_path2 = os.path.join(new_mpoint, 'bigfile')
		md5sum2 = system(('/usr/bin/md5sum %s' % bigfile_path2))[0].strip().split(' ')[0]
		self.assertEqual(md5sum, md5sum2)
		self.array.destroy(remove_disks=True)
		
	def _testDetachAttachRaid(self):
		mpoint = '/tmp/mpoint'
		if not os.path.isdir(mpoint):
			os.makedirs(mpoint)
			
		self.array = Storage.create(type='raid', disks=self.vols, level=1, vg='dbstorage', snap_pv=self.snap_vol, fstype='ext3')
		self.array.mkfs()
		self.array.mount(mpoint)
		
		bigfile_path = os.path.join(mpoint, 'bigfile')
		system('dd if=/dev/random of=%s bs=1M count=5' % bigfile_path)
		md5sum = system(('/usr/bin/md5sum %s' % bigfile_path))[0].strip().split(' ')[0]
		self.assertTrue(os.path.ismount(mpoint))
		config = self.array.detach(force=True)
		self.assertFalse(os.path.ismount(mpoint))
		self.assertEqual(self.array.devname, None)
		
		self.array = Storage.create(**config)
		self.array.mount(mpoint)
		md5sum2 = system(('/usr/bin/md5sum %s' % bigfile_path))[0].strip().split(' ')[0]
		self.assertEqual(md5sum, md5sum2)	
		
if __name__ == "__main__":
	init_platform('ec2')
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()