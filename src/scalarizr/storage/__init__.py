from scalarizr.util import system

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
	
class EphSnapshotMgr:
	# ???
	def __init__(self, devname, backend, fstype='ext3', chunk_size=10):
		# create fs on devname
		pass
	
	def create(self, volume):
		# create 
		# while cat volume.devname:
		# tar
		# gzip
		# if chunk_size >= self.chunk_size:
		#     write chunk 
		pass

	def restore(self, volume):
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