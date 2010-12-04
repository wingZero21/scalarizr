'''
Created on Dec 3, 2010

@author: marat
'''
import unittest

from scalarizr.util import system2 as system
from scalarizr.storage import mkloop, IEphSnapshotBackend, IEphSnapshotProvider, \
 	Snapshot, Volume, Storage, EphVolume, ResourceMgr

import os
from random import randint
import shutil
from scalarizr.libs.metaconf import Configuration




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
			self.devices.append(mkloop(self.filenames[i], size=100, quick=True))
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
			tc = None
			def __init__(self, dest_path=None):
				self.dest_path = dest_path
				
			def upload(self, snapshot, tranzit_path):
				manifest = Configuration('ini')
				manifest.read(snapshot.id)				
				
				self.tc.assertEqual(manifest.get('snapshot/description'), 'snapall')
				self.tc.assertTrue(manifest.get('snapshot/created_at'))
				self.tc.assertTrue(manifest.get('snapshot/pack_method'))
				self.tc.assertTrue(len(manifest.get_dict('chunks')) > 0)
				
				for chunk, md5sum in manifest.items('chunks'):
					shutil.copy(os.path.join(tranzit_path, chunk), self.dest_path)
				shutil.copy(snapshot.id, self.dest_path)
				
				snapshot.id = self.scheme + '://' + self.dest_path
			
				
			def download(self, id, tranzit_path):
				src_path = id[len(self.scheme + '://'):]
				for file in os.listdir(src_path):
					if not 'lost+found' in file:
						shutil.copy(file, tranzit_path)
		
		self.vols[1] = Volume(self.devices[1], self.mpoints[1], 'ext3')
		self.vols[1].mkfs()
		self.vols[1].mount()
		backend = SnapBackend(self.mpoints[1])
		backend.tc = self

		bigfile = os.path.join(self.mpoints[0], 'bigfile')
		
		# Create and mount storage
		self.vols[0] = Storage.create_ephs(self.devices[0], 'casstorage', snap_backend=backend)
		self.vols[0].mkfs('ext3')
		self.vols[0].mount(self.mpoints[0])
		
		# Create big file
		system(('dd', 'if=/dev/urandom', 'of=%s' % bigfile, 'bs=1M', 'count=30'))
		bigsize = os.path.getsize(bigfile)
		self.assertTrue(bigsize > 0)
		md5sum = system(('/usr/bin/md5sum', bigfile))[0].strip().split(' ')[0]		
		
		# Snapshot storage
		snap = self.vols[0].snapshot('snapall')

		# Restore snapshot on storage 2
		self.vols[2] = Storage.create_ephs(self.devices[2], 'casstorage2')
		self.vols[2].restore(snap)
		self.vols[2].mount(self.mpoints[2])
		bigfile2 = os.path.join(self.mpoints[2], 'bigfile')
		
		self.assertTrue(os.path.exists(bigfile2))

		md5sum2 = system(('/usr/bin/md5sum', bigfile2))[0].strip().split(' ')[0]
		self.assertEqual(md5sum, md5sum2)

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()