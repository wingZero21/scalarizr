'''
Created on Nov 25, 2010

@author: marat
'''
import unittest

from scalarizr.util import system2 as system
from scalarizr.storage import mkloop, ResourceMgr, IEphSnapshotBackend, Volume,\
	Snapshot, Storage, IEphSnapshotProvider, EphVolume, StorageError
from scalarizr.storage.fs import FileSystem

import os
import time
from random import randint
import shutil


class TestMkloop(unittest.TestCase):
	filename = None
	loop = None
	SIZE = 100
	
	def setUp(self):
		self.loop = None
		self.filename = '/tmp/loop%s' % randint(11, 99)
	
	def tearDown(self):
		if self.loop:
			system('/sbin/losetup -d %s' % self.loop, shell=True)
		if os.path.exists(self.filename):
			os.remove(self.filename)

	def test_create_quick(self):
		t1 = time.time()
		self.loop = mkloop(self.filename, size=self.SIZE, quick=True)
		t2 = time.time()
		self.assertTrue(t2 - t1 < 0.1)
		self.assert_size()
		
	def test_create(self):
		t1 = time.time()
		self.loop = mkloop(self.filename, size=self.SIZE, quick=False)
		t2 = time.time()
		self.assertTrue(t2 - t1 > 0.1)
		self.assert_size()
		
	def test_create_on_exiting_file(self):
		system('/bin/dd if=/dev/zero of=%s bs=1M seek=%d count=1' % (self.filename, self.SIZE-1), shell=True)
		self.loop = mkloop(self.filename)
		self.assert_size()
		
	def assert_size(self):
		self.assertEqual(os.path.getsize(self.filename), self.SIZE * 1024 * 1024)
		
class TestResourceMgr(unittest.TestCase):
	mgr = None
	def setUp(self):
		self.mgr = ResourceMgr
		self.mgr.reset()
	
	def tearDown(self):
		pass
	
	def test_singleton_backend(self):
		my = 'my'
		class MySnapBackend(IEphSnapshotBackend):
			scheme = my
		self.mgr.explore_snapshot_backend(my, MySnapBackend)
		o1 = self.mgr.lookup_snapshot_backend(my)
		self.assertTrue(isinstance(o1, MySnapBackend))
		self.assertEqual(o1, self.mgr.lookup_snapshot_backend(my))

	def test_singleton_fs(self):
		myfs = 'myfs'
		class FSDriver(FileSystem):
			name = myfs
		self.mgr.explore_filesystem(myfs, FSDriver)
		o1 = self.mgr.lookup_filesystem(myfs)
		self.assertTrue(isinstance(o1, FSDriver))
		self.assertEqual(o1, self.mgr.lookup_filesystem(myfs))

	def test_lookup_std_fs(self):
		fs = self.mgr.lookup_filesystem('ext4')
		self.assertEqual(fs.name, 'ext4')
	
		
class TestVolume(unittest.TestCase):
	def setUp(self):
		self.device = mkloop('/tmp/testvolume', size=100)
		self.mpoint = '/mnt/storage'
		if not os.path.exists(self.mpoint):
			os.makedirs(self.mpoint)
	
	def tearDown(self):
		if self.device:
			system(('/sbin/losetup', '-d', self.device))
		if os.path.exists(self.mpoint):
			os.removedirs(self.mpoint)
	
	def test(self):
		vol = Volume(self.device, self.mpoint, 'ext3')
		
		# When trying to do filesystem operations without having a fs on volume ValueError raises
		self.assertRaises(StorageError, vol.freeze)
		self.assertRaises(StorageError, vol.unfreeze)
		
		# After creating a filesystem ValueError dissapears
		vol.mkfs()
		self.assertTrue(vol.fstype, 'ext3')
		vol.freeze()
		vol.unfreeze()
		
		# When volume is mounted without mpoint specify, it gets from volume mpoint property
		self.assertFalse(vol.mounted())
		vol.mount()
		self.assertTrue(vol.mpoint, self.mpoint)
		self.assertTrue(vol.mounted())
				
		# When volume is unmounted, volume mpoint property remains
		vol.umount()
		self.assertEqual(vol.mpoint, self.mpoint)
		self.assertFalse(vol.mounted())
		
		snap = vol.snapshot(description='test snap')
		self.assertTrue(isinstance(snap, Snapshot))
		self.assertTrue(snap.id is None)
		self.assertEqual(snap.description, 'test snap')



class TestEphStorageCreate(unittest.TestCase):
	filename = None
	device = None
	vol = None

	def setUp(self):
		self.filename = '/tmp/pv%s' % randint(11, 99)
		self.device = mkloop(self.filename, size=200)
		self.mpoint = '/mnt/ephstorage'
		if not os.path.exists(self.mpoint):
			os.makedirs(self.mpoint)

	def tearDown(self):
		if self.vol:
			Storage.remove_ephs(self.vol)
		if self.device:
			system(('/sbin/losetup', '-d', self.device))
		if os.path.exists(self.filename):
			os.remove(self.filename)
		os.removedirs(self.mpoint)
	
	def test_1(self):
		class SnapProvider(IEphSnapshotProvider):
			tc = self
			def create(self, snapshot, volume, tranzit_path):
				self.tc.assertTrue(isinstance(snapshot, Snapshot))
				self.tc.assertTrue(isinstance(volume, EphVolume))
				self.tc.assertTrue(os.access(tranzit_path, os.W_OK | os.F_OK))
			
			def restore(self, volume, tranzit_path):
				self.tc.assertTrue(isinstance(volume, Volume))
				self.tc.assertTrue(os.access(tranzit_path, os.W_OK | os.F_OK))
				
		class SnapBackend(IEphSnapshotBackend):
			scheme = 'my'
			def upload(self, snapshot, tranzit_path):
				snapshot.id = 'my://bucket/path/to/snap'
				
		ResourceMgr.explore_snapshot_backend('my', SnapBackend)
		
		self.vol = Storage.create_ephs(self.device, 'dbstorage', 
								snap_pvd=SnapProvider(), snap_backend=SnapBackend())
		snap = self.vol.snapshot()
		self.vol.restore(snap)
		

class TestEphStorageSnapshotRestore(unittest.TestCase):
	
	def setUp(self):
		self.filenames = []
		self.devices = []
		self.mpoints = ('/mnt/storage', '/mnt/snapshot', '/mnt/storage2')
		self.vols = [None, None, None]		
		for i in range(3):
			self.filenames.append('/tmp/pv%s' % randint(11, 99))
			self.devices.append(mkloop(self.filenames[i], size=200, quick=True))
			if not os.path.exists(self.mpoints[i]):
				os.makedirs(self.mpoints[i])

	def tearDown(self):
		for vol in self.vols:
			if vol:
				vol.umount()
				if isinstance(vol, EphVolume):
					Storage.remove_ephs(vol)
		for device in self.devices:
			system(('/sbin/losetup', '-d', device))
		for file in self.filenames:
			os.remove(file)
		for mpoint in self.mpoints:
			os.removedirs(mpoint)

	def test_1(self):
		class SnapBackend(IEphSnapshotBackend):
			scheme = 'file'
			def __init__(self, dest_path=None):
				self.dest_path = dest_path
				
			def upload(self, snapshot, tranzit_path):
				#manifest = 
				
				for file in os.listdir(tranzit_path):
					shutil.copy(os.path.join(tranzit_path, file), self.dest_path)
				snapshot.id = self.scheme + '://' + self.dest_path
				
			def download(self, id, tranzit_path):
				src_path = id[len(self.scheme + '://'):]
				for file in os.listdir(src_path):
					shutil.copy(file, tranzit_path)
		
		self.vols[1] = Volume(self.devices[1], self.mpoints[1], 'ext3')
		self.vols[1].mkfs()
		self.vols[1].mount()
		backend = SnapBackend(self.mpoints[1])

		bigfile = os.path.join(self.mpoints[0], 'bigfile')
		
		# Create and mount storage
		self.vols[0] = Storage.create_ephs(self.devices[0], 'casstorage', snap_backend=backend)
		self.vols[0].mkfs('ext3')
		self.vols[0].mount(self.mpoints[0])
		
		# Create big file
		system(('dd', 'if=/dev/urandom', 'of=%s' % bigfile, 'bs=1M', 'count=50'))
		bigsize = os.path.getsize(bigfile)
		self.assertTrue(bigsize > 0)
		md5sum = system(('/usr/bin/md5sum', bigfile))[0].strip().split(' ')[0]		
		
		# Snapshot storage
		snap = self.vols[0].snapshot()
		system(('ls', '-la', '/mnt/snapshot'))
		
		'''
		# Restore snapshot on storage 2
		self.vols[2] = Storage.create_ephs(self.devices[2], 'casstorage2')
		self.vols[2].restore(snap)
		self.vols[2].mount(self.mpoints[2])
		bigfile2 = os.path.join(self.mpoints[2], 'bigfile')
		
		self.assertTrue(os.path.exists(bigfile2))

		md5sum2 = system(('/usr/bin/md5sum', bigfile2))[0].strip().split(' ')[0]
		self.assertEqual(md5sum, md5sum2)
		'''
		
if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()