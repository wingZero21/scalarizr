'''
Created on Nov 25, 2010

@author: marat
'''
import unittest

from scalarizr.util import system2 as system
from scalarizr.storage import mkloop, Volume, Snapshot, Storage, StorageError, VolumeProvider,\
	EphSnapshot
from scalarizr.storage.fs import FileSystem

import os
import time
from random import randint


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
		
class TestResourceManagement(unittest.TestCase):
	mgr = None
	def setUp(self):
		self.mgr = Storage
		self.mgr._fs_drivers = {}
	
	def tearDown(self):
		pass
	
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
		vol = Volume(self.device, self.mpoint)
		
		# When trying to do filesystem operations without having a fs on volume ValueError raises
		self.assertRaises(StorageError, vol.freeze)
		self.assertRaises(StorageError, vol.unfreeze)
		
		# After creating a filesystem ValueError dissapears
		vol.mkfs('ext3')
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
		self.assertEqual(snap.description, 'test snap')

class TestStorageProviders(unittest.TestCase):
	_save_snap_pvd = None
	_save_vol_pvd = None
	_save_pvds = None

	class MyPvd(VolumeProvider):
		type = 'myvol'
	
	def setUp(self):
		self._save_pvds = Storage.providers.copy()
		Storage.providers.clear()
		self._save_snap_pvd, Storage.default_snap_provider = Storage.default_snap_provider, None
		self._save_vol_pvd, Storage.default_vol_provider = Storage.default_vol_provider, None

	def tearDown(self):
		Storage.providers = self._save_pvds
		Storage.default_snap_provider = self._save_snap_pvd
		Storage.default_vol_provider = self._save_vol_pvd
	
	def test_explore_provider(self):
		Storage.explore_provider(self.MyPvd)
		self.assertFalse(Storage.default_snap_provider)
		self.assertFalse(Storage.default_vol_provider)
		self.assertTrue(isinstance(Storage.lookup_provider(self.MyPvd.type), self.MyPvd))
	
	def test_explore_default_provider(self):
		Storage.explore_provider(self.MyPvd, True)
		self.assertFalse(Storage.default_snap_provider)
		self.assertEqual(Storage.default_vol_provider, self.MyPvd.type)
		
		self.assertTrue(isinstance(Storage.lookup_provider(self.MyPvd.type), self.MyPvd))
		self.assertTrue(isinstance(Storage.lookup_provider(), self.MyPvd))
		
	def test_explore_default_provider2(self):
		Storage.explore_provider(self.MyPvd, default_for_snap=True)
		self.assertEqual(Storage.default_snap_provider, self.MyPvd.type)
		self.assertFalse(Storage.default_vol_provider)
		
		self.assertTrue(isinstance(Storage.lookup_provider(self.MyPvd.type), self.MyPvd))
		self.assertTrue(isinstance(Storage.lookup_provider(None, True), self.MyPvd))
	
	def test_snapshot_factory(self):
		Storage.providers = self._save_pvds		
		pvd = Storage.lookup_provider('eph')
		snap = pvd.snapshot_factory('hom')
		self.assertEqual(snap.type, 'eph')
		self.assertEqual(snap.description, 'hom')
		self.assertTrue(isinstance(snap, EphSnapshot))
	
	
class TestStorageCreate(unittest.TestCase):
	class Vol(Volume):
		def __init__(self, *args, **kwargs):
			if kwargs:
				for k, v in kwargs.items():
					setattr(self, k, v)
			Volume.__init__(self, *args, **kwargs)
			
	class VolPvd(VolumeProvider):
		type = 'myvol'
	
	def setUp(self):
		self.VolPvd.vol_class = self.Vol
		Storage.explore_provider(self.VolPvd)

	def tearDown(self):
		pass
	
	def test_create_by_string_args(self):
		vol = Storage.create('/dev/sdb')
		self.assertEqual(vol.devname, '/dev/sdb')

	def test_create_over_disk(self):
		vol = Storage.create(type='myvol', device='/dev/lvolume', disk='/dev/sdb')
		self.assertEqual(vol.disk.devname, '/dev/sdb')
		
		vol = Storage.create(
			type='myvol', 
			device='/dev/ldevice2',
			disk=dict(
				type='myvol',
				device='/dev/sdb',
				param1='value1'
			)
		)
		self.assertEqual(vol.disk.devname, '/dev/sdb')
		self.assertEqual(vol.disk.param1, 'value1')
		
	def test_create_vol_container(self):
		vol = Storage.create(
			type='myvol',
			device='/dev/gp0',
			disks=('/dev/sdb', dict(type='myvol', device='/dev/sdd'))
		)
		self.assertEqual(len(vol.disks), 2)
		self.assertEqual(vol.disks[0].devname, '/dev/sdb')
		self.assertEqual(vol.disks[1].devname, '/dev/sdd')
		self.assertEqual(vol.disks[1].type, 'myvol')
	
	def test_create_from_snapshot(self):
		vol = Storage.create(
			snapshot=dict(
				type='base',
				device='/dev/sdb',
				mpoint='/mnt/dbstorage',
				fstype='xfs'
			)
		)
		self.assertEqual(vol.devname, '/dev/sdb')
		self.assertEqual(vol.mpoint, '/mnt/dbstorage')
		
		vol = Storage.create(
			device='/dev/sdd',
			snapshot=dict(
				type='myvol',
				device='/dev/lvol',
				param1='value1',
				param2='value2'
			)
		)
		self.assertEqual(vol.devname, '/dev/sdd')
		self.assertEqual(vol.type, 'myvol')
		self.assertEqual(vol.param1, 'value1')

		
if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()