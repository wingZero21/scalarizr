from random import randint
import subprocess 
import hashlib
import logging
import os

from scalarizr.util import system
from scalarizr.storage.fs.ext import Ext3FileSystem
from scalarizr.libs.metaconf import Configuration

# ebs-raid0-lvm-ext3
# ebs-raid0-xfs
# eph-lvm-ext3
# eph-xfs


'''
# ebs-raid0-lvm-ext3

vol = mgr.create_raid(devices=('/dev/ebs1', '/dev/ebs2'), level='striping')
vol = mgr.lvm_wrapper(vol, buffer_device='/dev/loop0')
vol.mkfs('ext3')
snap = vol.snapshot()

# ebs-raid0-xfs
vol = mgr.create_raid(devices=('/dev/ebs1', '/dev/ebs2'), level='striping')
vol.mkfs('xfs')
snap = vol.snapshot()


# eph-lvm-ext3
vol = mgr.create_reliable_volume(device='/dev/sdb', shadow='/dev/sdd', 
		snapshot_backend=CloudfilesSnapshotBackend('/container/key-basename')
vol = mgr.lvm_wrapper(vol, buffer_device='/dev/loop0')
vol.mkfs('ext3')
snap = vol.snapshot()


# eph-xfs
vol = mgr.create_reliable_volume(device='/dev/loop0', shadow='/dev/loop1', 
		snapshot_backend=CloudfilesSnapshotBackend('/container/key-basename')
vol.mkfs('xfs')
snap = vol.snapshot()
'''


def mkloop(size):
	pass


class StorageMgr:
	def create_raid(self, devices, level):
		pass
	
	def create_reliable_volume(self, device, shadow, snapshot_backend):
		pass
	
	def lvm_wrapper(self, device, buffer_device=None, create_buffer_cb=None, release_buffer_cb=None):
		pass
	
		
class Volume:
	devname = None
	mpoint = None
	fs = None
	fstype = None

	snapable = False

	def __init__(self, devname, mpoint, fstype=None):
		pass
	
	@property
	def mounted(self):
		pass

	def mount(self, mpoint, **options):
		pass
	
	def umount(self):
		pass
	
	def snapshot(self):
		pass



class FileSystemProvider:
	@classmethod
	def explore(fstype, ImplClass):
		pass
	
	@classmethod
	def lookup(fstype=None, devname=None):
		pass



class RaidVolume(Volume):
	level = None
	devices = None
	
	def __init__(self, devname, mpoint, raid_level, raid_devices):
		pass
	
	@property
	def snapable(self):
		return all(lambda vol: vol.snapable(), self.devices)


class FreezableVolume(Volume):
	physical_vol = None
	vol_group = None
	
	def freeze(self):
		pass
	
	def unfreeze(self):
		pass
	
	def snapshot(self):
		# create lvm snapshot
		pass


class Snapshot:
	name = None
	id = None
	def __init__(self, id=None, name=None):
		self.id = id
		self.name = name
	
class ReliableSnapshot(Snapshot):
	path = None
	
class ReliableSnapshotBackend:
	def save(self, snapshot):
		# save snapshot to 
		pass
	def load(self, snapshot):
		pass
	
class ReliableVolume(Volume):
	def snapshot(self):
		pass
	pass
	
class Registry:
	@classmethod
	def lookup_snapshot_backend(self, schema):
		pass
	
	@classmethod
	def explore_snapshot_backend(self, schema, BackendClass):
		pass
	
	@classmethod
	def lookup_filesystem(self, fstype):
		pass

	@classmethod
	def explore_filesystem(self, fstype, FSClass):
		pass

	
class EphSnapshotMgr:

	backup_devname = None
	backend = None
	fstype = None
	chunk_size = None
	PREFIX = 'snapshot'
	
	def __init__(self, backup_devname, backend, fstype='ext3', chunk_size=10):
		self.backup_devname = backup_devname
		self.backend = backend
		self.fstype = fstype
		self.chunk_size = chunk_size
		
		self._logger = logging.getLogger(__name__)
		self._config = Configuration('ini')
		
	def _make_unique_dir(self, directory):
		while os.path.exists(directory):
			directory += str(randint(1000000, 9999999))
		os.makedirs(directory)
		return directory
	
	def create(self, volume, snap_name=None):
		#prepare backup volume
		fs = FileSystemProvider.lookup(self.fstype)
		fs.mkfs(self.backup_devname)
		backup_dir = self._make_unique_dir('/mnt/backup')
		backup_volume = Volume(self.backup_devname, backup_dir, self.fstype)
		backup_volume.mount(backup_dir)
		# create lvm snapshot ; calculate buf size first
		cmd1 = ['dd', 'if=%s' % volume.devname]
		cmd2 = ['gzip']
		cmd3 = ['split', '-a','3', '-b', '%s'%self.chunk_size, '-', '%s/%s.gz.' 
			% (backup_dir, self.PREFIX)]
		p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
		p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE)
		p3 = subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE)
		self._logger.info('Making shadow copy')
		output = p3.communicate()[0]
		#unfreeze - delete lvm snapshot
		if output:
			self._logger.debug(output)
		
		self._config.add('./%s/%s'%('snapshot_name', 'name'), snap_name, force=True)
		for chunk in os.listdir(self.backup_dir):
			full_path = os.path.join(self.backup_dir, chunk)
			self._config.add('./%s/%s'%('chunks', chunk), self._md5(full_path), force=True)
		self._config.write(os.path.join(self.backup_dir, 'manifest.ini'))
		
		self.backend.save(self.backup_dir)
		backup_volume.umount()
		os.rmdir(self.backup_dir)
		
		return Snapshot(name=snap_name)#why do we need to return this and where to find ID?
	
	def _md5(self, file, block_size=4096):
		md5 = hashlib.md5()
		while True:
			data = file.read(block_size)
			if not data:
				break
			md5.update(data)
		return md5.digest()

	def restore(self, snapshot, volume):
		# backend: load snapshot into `dir`
		# restore chunks from `dir` into volume.devname
		 
		#find temporary place for chunks
		#mount with rw 
		#download [which prefix?!]
		
		
		# foreach chunk
		#    ungzip
		#    untar
		#    write to volume.devname
		pass


def _system(cmd, error):
	out,rcode = system(cmd + ' 2>&1', True)[0::2]
	if rcode:
		raise Exception(error+"\n" + "Return code: %s. Error: %s" % (rcode, out))
	return out
