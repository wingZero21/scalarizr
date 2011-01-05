
from .lvm2 import Lvm2, Lvm2Error
from .raid import Mdadm
from .fs import MOUNT_EXEC, UMOUNT_EXEC, SYNC_EXEC
from .transfer import Transfer

from scalarizr.util import system2, PopenError, firstmatched, wait_until
from scalarizr.util.filetool import read_file, write_file
from scalarizr.libs.metaconf import Configuration

import urlparse
import subprocess
from random import randint
import hashlib
import logging
import os
import re
import time
import glob
import signal
import binascii
import uuid
import shutil
import json
import cStringIO



logger = logging.getLogger(__name__)


def system(*popenargs, **kwargs):
	kwargs['logger'] = logger
	kwargs['exc_class'] = StorageError
	return system2(*popenargs, **kwargs)

class StorageError(PopenError):
	pass


def mkloop(filename, size=None, quick=False):
	''' Create loop device '''
	if size:
		cmd = ['dd', 'if=/dev/zero', 'of=%s' % filename, 'bs=1M']
		if quick:
			cmd.extend(['seek=%d' % (size - 1,), 'count=1'])
		else:
			cmd.extend(['count=%d' % size])
		system(cmd)
	devname = system(('/sbin/losetup', '-f'))[0].strip()
	system(('/sbin/losetup', devname, filename))
	return devname

def listloop():
	ret = {}
	loop_lines = system(('/sbin/losetup', '-a'))[0].strip().splitlines()
	for loop_line in loop_lines:
		words = loop_line.split()
		ret[words[0][:-1]] = words[-1][1:-1]
	return ret
	

def rmloop(device):
	system(('/sbin/losetup', '-d', device))


class Storage:
	_lvm = None
	_mdadm = None
	_fs_drivers = {}
	
	@staticmethod
	def _init_lvm():
		self = Storage
		if not self._lvm:
			self._lvm = Lvm2()
	
	@staticmethod
	def _init_mdadm():
		self = Storage
		if not self._mdadm:
			self._mdadm = Mdadm()
	
	@staticmethod
	def lookup_filesystem(fstype):
		self = Storage
		if fstype in self._fs_drivers:
			return self._fs_drivers[fstype]
		try:
			mod = __import__('scalarizr.storage.fs.%s' % fstype, globals(), locals(), ('__filesystem__',), -1)
			self._fs_drivers[fstype] = mod.__filesystem__()
			return self.lookup_filesystem(fstype)
		except ImportError:
			raise LookupError('Unknown filesystem %s' % fstype)

	@staticmethod
	def explore_filesystem(fstype, FileSystemClass):
		self = Storage
		self._fs_drivers[fstype] = FileSystemClass()	
	
	@staticmethod
	def explore_provider(PvdClass, default_for_vol=False, default_for_snap=False):
		self = Storage
		type = PvdClass.type
		self.providers[type] = PvdClass
		if default_for_vol:
			self.default_vol_provider = type
		if default_for_snap:
			self.default_snap_provider = type
	
	providers = {}
	default_vol_provider = None
	default_snap_provider = None

	@staticmethod
	def lookup_provider(pvd_type=None, for_snap=False):
		'''
		XXX: for_snap confuse
		'''
		self = Storage
		
		if not pvd_type:
			pvd_type = self.default_snap_provider if for_snap else self.default_vol_provider		
		try:
			pvd = self.providers[pvd_type]
		except KeyError:
			raise LookupError('Unknown volume provider "%s"' % (pvd_type,))
		if hasattr(pvd, '__bases__'):
			pvd = pvd()
			self.providers[pvd_type] = pvd
		return pvd

	@staticmethod
	def create(*args, **kwargs):
		'''
		@raise LookupError: When volume provider cannot be resolved
		@raise StorageError: General error for all cases		
		'''
		self = Storage
		args = args or list()
		kwargs = kwargs or dict()
		from_snap = False
		
		if 'snapshot' in kwargs:
			# Save original kwargs
			from_snap = True			
			orig_kwargs = kwargs.copy()
			
			# Set kwargs to snapshot kwargs
			kwargs = kwargs['snapshot'].config() if isinstance(kwargs['snapshot'], Snapshot) else kwargs['snapshot']
			if not isinstance(kwargs, dict):
				args = None
				kwargs = dict(device=kwargs)
				
			# Update kwargs with original one
			del orig_kwargs['snapshot']
			kwargs.update(orig_kwargs)
			
		if 'disks' in kwargs:
			disks = []
			for item in kwargs['disks']:
				if isinstance (item, Volume):
					disks.append(item)
					continue
				disk = self.create(**item) if isinstance(item, dict) else self.create(item)
				disks.append(disk)
			kwargs['disks'] = disks
			
		if 'disk' in kwargs:
			disk = kwargs['disk']
			if not isinstance(disk, Volume):
				kwargs['disk'] = self.create(**disk) if isinstance(disk, dict) else self.create(disk)
		
		if args:
			kwargs['device'] = args[0]
			
		# Find provider	
		pvd = self.lookup_provider(kwargs.get('type'), from_snap)
		return getattr(pvd, 'create_from_snapshot' if from_snap else 'create').__call__(**kwargs)
	
	@staticmethod
	def snapshot(vol, description):
		pass

	@staticmethod	
	def detach(vol, force=False):
		pvd = Storage.lookup_provider(vol.type)
		ret = pvd.detach(vol, force)
		vol.detached = True
		return ret
	
	@staticmethod
	def destroy(vol, force=False, **kwargs):
		pvd = Storage.lookup_provider(vol.type)
		pvd.destroy(vol, force, **kwargs)
		
	@staticmethod
	def backup_config(cnf, filename):
		fp = open(filename, 'w+')
		try:
			fp.write(json.dumps(cnf))
		finally:
			fp.close()
	
	@staticmethod
	def restore_config(filename):
		fp = open(filename, 'r')
		try:
			return json.load(fp)
		finally:
			fp.close()
	
	
class VolumeConfig(object):
	type = 'base'
	device = None
	mpoint = None
	fstype = None
	id = None
	
	def config(self, as_snapshot=False):
		base = tuple(base for base in self.__class__.__bases__ if base.__name__.endswith('Config'))[0]
		attrs = tuple(attr for attr in dir(base) if not attr.startswith('_'))
		ret = dict()
		for attr in attrs:
			if attr == 'config':
				continue
			elif attr == 'disks':
				ret['disks'] = tuple(disk.config() for disk in self.disks)
				if as_snapshot:
					ret['disks'] = tuple(dict(snapshot=disk) for disk in ret['disks'])
			elif attr == 'disk':
				ret['disk'] = self.disk.config() if self.disk else self.disk
			else:
				ret[attr] = getattr(self, attr)
		return ret
	
def _fs_should_be_set(f):
	def d(*args):
		if args[0]._fs is None:
			raise StorageError('Filesystem is not set')
		return f(*args)
	return d

def _lvm_group_b64(vg):
	vgfile = '/etc/lvm/backup/%s' % os.path.basename(vg)
	if os.path.exists(vgfile):
		return binascii.b2a_base64(read_file(vgfile))

class Volume(VolumeConfig):
	detached = False
	
	_logger = None
	_fs = None

	def __init__(self, device=None, mpoint=None, fstype=None, type=None, *args, **kwargs):
		self._logger = logging.getLogger(__name__)
		if not device:
			raise ValueError('device name should be non-empty')
		self.device = device
		self.mpoint = mpoint
		if fstype:
			self.fstype = fstype
		if type:
			self.type = type
		for k, v in kwargs.items():
			if hasattr(self, k):
				setattr(self, k, v)

	@property	
	def devname(self):
		return self.device

	def _fstype_setter(self, fstype):
		self._fs = Storage.lookup_filesystem(fstype)

	def _fstype_getter(self):
		return self._fs.name if self._fs else None

	fstype = property(_fstype_getter, _fstype_setter)

	def mkfs(self, fstype=None):
		fstype = fstype or self.fstype
		if not fstype:
			raise ValueError('Filesystem cannot be None')
		fs = Storage.lookup_filesystem(fstype) 
		fs.mkfs(self.devname)
		self.fstype = fstype
		self._fs = fs
	
	@_fs_should_be_set
	def resize(self, size=None, **fsargs):
		fsargs = fsargs or dict()
		return self._fs.resize(self.devname, **fsargs)
	
	@_fs_should_be_set
	def _get_label(self):
		return self._fs.get_label(self.devname)
	
	@_fs_should_be_set
	def _set_label(self, lbl):
		self._fs.set_label(self.devname, lbl)
		
	label = property(_get_label, _set_label)
	
	@_fs_should_be_set
	def freeze(self):
		return self._fs.freeze(self.devname)
	
	@_fs_should_be_set
	def unfreeze(self):
		return self._fs.unfreeze(self.devname)
	
	def mounted(self):
		res = re.search('%s\s+on\s+(?P<mpoint>.+)\s+type' % self.devname, system(MOUNT_EXEC)[0])
		return bool(res)
	
	def mount(self, mpoint=None):
		mpoint = mpoint or self.mpoint
		cmd = (MOUNT_EXEC, self.devname, mpoint)
		system(cmd, error_text='Cannot mount device %s' % self.devname)
		self.mpoint = mpoint
	
	def umount(self, lazy=False):
		cmd = (UMOUNT_EXEC, '-l' if lazy else '-f' , self.devname)
		try:
			system(cmd, error_text='Cannot umount device %s' % self.devname)
		except (Exception, BaseException), e:
			if not 'not mounted' in str(e):
				raise
	
	def snapshot(self, description=None):
		# Freeze filesystem
		if self._fs:
			system(SYNC_EXEC)
			self.freeze()

		# Create snapshot
		pvd = Storage.lookup_provider(self.type)
		snap = pvd.snapshot_factory(description)		
		pvd.create_snapshot(self, snap)
		
		# Unfreeze filesystem
		if self._fs:
			self.unfreeze()
			
		# Save snapshot
		return pvd.save_snapshot(self, snap)

	def detach(self, force=False):
		return Storage.detach(self, force)
	
	def destroy(self, force=False, **kwargs):
		Storage.destroy(self, force, **kwargs)

	def __str__(self):
		fmt = '[volume:%s] %s\n' + '%-10s : %s\n'*3
		return fmt % (
			self.type, self.devname,
			'id', self.id or ''
			'mpoint', self.mpoint or '',
			'fstype', self.fstype or ''
		)
		
		
class Snapshot(VolumeConfig):
	version = '0.7'
	type = None
	description = None

	_id = None
		
	def __init__(self, type=None, description=None, **kwargs):
		self.type = type
		self.description = description
		for k, v in kwargs.items():
			if hasattr(self, k):
				setattr(self, k, v)
	
	def _id_setter(self, id):
		self._id = id
		
	def _id_getter(self):
		if not self._id:
			self._id = '%s-snap-%s' % (self.type, uuid.uuid4().hex[0:8])
		return self._id
	
	id = property(_id_getter, _id_setter)
	
	def __str__(self):
		fmt = '[snapshot(v%s):%s] %s\n%s'
		return str(fmt % (self.version, self.type, self.id, self.description or '')).strip()
	
	def config(self):
		cnf = VolumeConfig.config(self, as_snapshot=True)
		cnf['description'] = self.description
		return cnf
	

class VolumeProvider(object):
	type = 'base'
	vol_class = Volume
	snap_class = Snapshot
	
	def create(self, **kwargs):
		device = kwargs['device']
		del kwargs['device']
		if not kwargs.get('type'):
			kwargs['type'] = self.type
		return self.vol_class(device, **kwargs)
	
	def create_from_snapshot(self, **kwargs):
		return self.create(**kwargs)
	
	def snapshot_factory(self, description=None, **kwargs):
		kwargs['description'] = description
		kwargs['type'] = self.type
		return self.snap_class(**kwargs)
	
	def create_snapshot(self, vol, snap):
		return snap
	
	def save_snapshot(self, vol, snap):
		return snap

	def destroy(self, vol, force=False, **kwargs):
		if not vol.devname and not vol.detached:
			raise StorageError("Can't destroy volume: device name is empty.")
		if not vol.detached:
			self._umount(vol, force)		
	
	def detach(self, vol, force=False):
		if not vol.detached:
			self._umount(vol, force)
		
	def _umount(self, vol, force=False):
		try:
			vol.umount()
		except:
			if force:
				vol.umount(lazy=True)
			else:
				raise

		
Storage.explore_provider(VolumeProvider, default_for_vol=True, default_for_snap=True)

class LoopConfig(VolumeConfig):
	type = 'loop'
	file = None
	size = None
	zerofill = None

class LoopVolume(Volume, LoopConfig):
	pass
		
class LoopSnapshot(Snapshot, LoopConfig):
	pass

	
class LoopVolumeProvider(VolumeProvider):
	type = 'loop'
	vol_class = LoopVolume
	snap_class = LoopSnapshot
	
	def create(self, **kwargs):
		
		'''
		@param file: Filename for loop device
		@type file: basestring
		
		@param size: Size in MB or % of root device
		@type size: int | str
		
		@param zerofill: Fill device with zero bytes. Takes more time, but greater GZip compression
		@type zerofill: bool
		'''
		size = kwargs.get('size')
		file = kwargs.get('file')
		
		if (not size and (not file or not os.path.exists(file))):
			raise StorageError('You must specify size of new loop device or existing file.')
		
		if not file:
			file = '/mnt/loopdev' + time.strftime('%Y%m%d%H%M%S')			
		try:
			size = int(size)
		except ValueError:
			if type(size) == str and '%free' in size.lower():
				# Getting size in percents
				try:
					size_pct = int(size.lower().replace('%free', ''))
				except:
					raise StorageError('Incorrect size format: %s' % size)
				# Retrieveing root device size and usage 
				root_size, used_pct = (system(('df', '-B', '1024', '/'))[0].splitlines()[1].split()[x] for x in (1,4))
				root_size = int(root_size) / 1024
				used_pct = int(used_pct[:-1])
				
				if size_pct > (100 - used_pct):
					raise StorageError('No enough free space left on device.')
				# Calculating loop device size in Mb
				size = (root_size / 100) * size_pct
			else:
				raise StorageError('Incorrect size format: %s' % size)
		
		kwargs['size']	= size
		kwargs['file']	= file
		kwargs['device'] = mkloop(file, size, not kwargs.get('zerofill'))
		return super(LoopVolumeProvider, self).create(**kwargs)
	

	def create_from_snapshot(self, **kwargs):
		return self.create(**kwargs)
	
	def create_snapshot(self, vol, snap):
		backup_filename = vol.file + '.%s.bak' % time.strftime('%d-%m-%Y_%H:%M')
		shutil.copy(vol.file, backup_filename)
		snap.file = backup_filename
		return snap
	
	def save_snapshot(self, vol, snap):
		return snap

	def detach(self, vol, force=False):
		super(LoopVolumeProvider, self).detach(vol, force)
		rmloop(vol.devname)
		vol.device = None
		vol.detached = True
		return vol.config()

	def destroy(self, vol, force=False, **kwargs):		
		super(LoopVolumeProvider, self).destroy(vol, force, **kwargs)
		rmloop(vol.devname)
		vol.device = None
		
Storage.explore_provider(LoopVolumeProvider)


class RaidConfig(VolumeConfig):
	type	= 'raid'
	level	= None
	raid_pv = None
	snap_pv = None
	vg		= None
	lvm_group_cfg = None
	disks	= None
	
	def config(self, as_snapshot=False):
		cnf = VolumeConfig.config(self, as_snapshot)
		if isinstance(cnf['raid_pv'], Volume):
			cnf['raid_pv'] = cnf['raid_pv'].config()
		if isinstance(cnf['snap_pv'], Volume):
			cnf['snap_pv'] = cnf['snap_pv'].config()
		return cnf

class RaidVolume(Volume, RaidConfig):
	pass
	
class RaidSnapshot(Snapshot, RaidConfig):
	pass

def _devname_not_empty(f):
	def d(*args, **kwargs):
		if not args[1].devname:
			raise StorageError('Device name is empty.')
		return f(*args, **kwargs)
	return d

class RaidVolumeProvider(VolumeProvider):
	type = 'raid'
	vol_class = RaidVolume
	snap_class = RaidSnapshot

	_mdadm = None
	_lvm = None
	_logger = None
	
	def __init__(self):
		self._mdadm = Mdadm()
		self._lvm = Lvm2()
		self._logger = logging.getLogger(__name__)
		self._lvm_backup_filename = '/tmp/lvm_backup'
		
	
	def create(self, **kwargs):
		'''
		@param disks: Raid disks
		@type disks: list(Volume)
		
		@param level: Raid level 0, 1, 5 - are valid values
		@type level: int
		
		@param vg: Volume group over RAID PV
		@type vg: str|dict
		
		@param snap_pv: Physical volume for LVM snapshot
		@type snap_pv: str|dict|Volume
		'''
		if kwargs.get('lvm_group_cfg'):
			volume = self.create_from_snapshot(**kwargs)
		else:
			raid_pv = self._mdadm.create(list(vol.devname for vol in kwargs['disks']), kwargs['level'])
			if not isinstance(kwargs['vg'], dict):
				kwargs['vg'] = dict(name=kwargs['vg'])
			vg_name = kwargs['vg']['name']
			del kwargs['vg']['name']
			vg_options = kwargs['vg']
			self._lvm.create_pv(raid_pv)		
			kwargs['vg'] = self._lvm.create_vg(vg_name, (raid_pv,), **vg_options)
			kwargs['device'] = self._lvm.create_lv(vg_name, extents='100%FREE')
			kwargs['raid_pv'] = raid_pv
			volume = super(RaidVolumeProvider, self).create(**kwargs)
		return volume
	
	def create_from_snapshot(self, **kwargs):
		'''
		@param level: Raid level 0, 1, 5 - are valid values
		@param vg: Volume group name to restore
		@param lvm_group_cfg: Base64 encoded RAID volume group configuration
		@param disks: Volumes
		@param snap_pv: Physical volume for future LVM snapshot creation
		'''

		vg				= kwargs['vg']
		raw_vg			= os.path.basename(vg)
		raid_pv			= self._mdadm.assemble([vol.devname for vol in kwargs['disks']])
		lvm_raw_backup	= binascii.a2b_base64(kwargs['lvm_group_cfg'])
		
		write_file(self._lvm_backup_filename, lvm_raw_backup, logger=logger)
		
		if 'pv_uuid' in kwargs:
			system(('pvcreate', '--uuid', kwargs['pv_uuid'], raid_pv))
					
		try:
			self._lvm.restore_vg(vg, self._lvm_backup_filename)
		finally:
			os.unlink(self._lvm_backup_filename)
		
		lvinfo = firstmatched(lambda lvinfo: lvinfo.vg_name == raw_vg, self._lvm.lv_status())
		if not lvinfo:
			raise StorageError('Volume group %s does not contain any logical volume.' % raw_vg)
		self._lvm.change_vg(raw_vg, available=True)
		
		return RaidVolume(	lvinfo.lv_path,
							raid_pv	= raid_pv,
						 	vg		= vg,
							disks	= kwargs['disks'],
							level	= kwargs['level'],
							snap_pv	= kwargs['snap_pv'])

	
	@_devname_not_empty
	def create_snapshot(self, vol, snap):
		if not vol.snap_pv:
			raise ValueError('Volume should have non-empty snap_pv attribute')
		if isinstance(vol.snap_pv, Volume):
			snap_pv = vol.snap_pv
		elif isinstance(vol.snap_pv, dict):
			snap_pv = Storage.create(**vol.snap_pv)
		else:
			snap_pv = Storage.create(vol.snap_pv)

		# Extend RAID volume group with snapshot disk
		self._lvm.create_pv(snap_pv.devname)
		if not self._lvm.pv_info(snap_pv.devname).vg == vol.vg:
			self._lvm.extend_vg(vol.vg, snap_pv.devname)
			
		# Create RAID LVM snapshot
		snap_lv = self._lvm.create_lv_snapshot(vol.devname, 'snap', '100%FREE')
		try:
			# Creating RAID members snapshots
			snap.level		= vol.level
			snap.vg			= vol.vg
			snap.tmp_snaps	= []
			snap.disks		= []
			snap.snap_pv	= vol.snap_pv.config() if isinstance(vol.snap_pv, Volume) else vol.snap_pv

			for _vol, i in zip(vol.disks, range(0, len(vol.disks))):
				pvd = Storage.lookup_provider(_vol.type)
				_snap = pvd.snapshot_factory('RAID%s disk #%d - %s' % (vol.level, i, snap.description))
				snap.tmp_snaps.append((_vol, pvd.create_snapshot(_vol, _snap)))
		except (Exception, BaseException), e:
			raise StorageError("Error occured during snapshot creation: '%s'" % e)
					
		finally: 
			self._lvm.remove_lv(snap_lv)
			self._lvm.remove_pv(snap_pv.devname)
			# Destroy or detach Snap PV.
			snap_pv.detach() if isinstance(vol.snap_pv, Volume) else snap_pv.destroy()
		
		return snap
	
	@_devname_not_empty
	def save_snapshot(self, vol, snap):
		raw_vg = os.path.basename(vol.vg)
		lvmgroupcfg = read_file('/etc/lvm/backup/%s' % raw_vg)
		if lvmgroupcfg is None:
			raise StorageError('Backup file for volume group "%s" does not exists' % raw_vg)
		snap.lvm_group_cfg = binascii.b2a_base64(lvmgroupcfg)
			
		# Saving RAID members snapshots
		for _vol, _snap in snap.tmp_snaps:
			pvd = Storage.lookup_provider(_vol.type)
			snap.disks.append(pvd.save_snapshot(_vol, _snap))
		del snap.tmp_snaps
		
		return snap

	def destroy(self, vol, force=False, **kwargs):
		super(RaidVolumeProvider, self).destroy(vol, force, **kwargs)
		
		remove_disks=kwargs.get('remove_disks') 
		if not vol.detached:
			self._remove_lvm(vol, force)
			# Check if sleeping is necessary
			time.sleep(1)
			self._mdadm.delete(vol.raid_pv)
		if remove_disks:
			if getattr(vol.disks, '__iter__', False):
				for disk in vol.disks:
					disk.destroy(force=force)
			if vol.snap_pv and isinstance(vol.snap_pv, Volume):
				vol.snap_pv.destroy(force=force)
				


	@_devname_not_empty			
	def detach(self, vol, force=False):
		self._logger.debug('Detaching volume %s' % vol.devname)
		super(RaidVolumeProvider, self).detach(vol, force)
		pv_uuid = system(('pvs', '-o', 'pv_uuid', vol.raid_pv))[0].splitlines()[1].strip()
		vol.lvm_group_cfg = _lvm_group_b64(vol.vg)
		self._remove_lvm(vol)
		self._mdadm.delete(vol.raid_pv, zero_superblock=False)
		for disk in vol.disks:
			disk.detach(force)
		ret = vol.config()
		ret['pv_uuid'] = pv_uuid
		vol.detached = True
		return ret

	def _remove_lvm(self, vol, force=False):
		self._lvm.remove_vg(vol.vg)
		self._lvm.remove_pv(vol.raid_pv)
		vol.device = None
	
Storage.explore_provider(RaidVolumeProvider)

class EphConfig(VolumeConfig):
	type = 'eph'
	vg = None
	lvm_group_cfg = None
	disk = None
	size = None
	path = None
	snap_backend = None

class EphVolume(Volume, EphConfig):
	tranzit_vol = None

class EphSnapshot(Snapshot, EphConfig):
	pass

class EphVolumeProvider(VolumeProvider):
	type = 'eph'
	vol_class = EphVolume
	snap_class = EphSnapshot
	
	_lvm = None
	_snap_pvd = None
	
	def __init__(self):
		self._lvm = Lvm2()
		self._snap_pvd = EphSnapshotProvider()
	
	def _create_layout(self, pv, vg, size):
		''' 
		Creates LV layout
		      [Disk]
		        |
		       [VG]
		      /   \ 
		  [Data] [Tranzit]
		'''

		# Create PV
		self._lvm.create_pv(pv)		

		# Create VG
		if not isinstance(vg, dict):
			vg = dict(name=vg)
		vg_name = vg['name']
		del vg['name']
		vg = self._lvm.create_vg(vg_name, [pv], **vg)
		
		# Create data volume
		lv_kwargs = dict()
		
		size = size or '40%'
		size = str(size)
		if size[-1] == '%':
			lv_kwargs['extents'] = '%sVG' % size
		else:
			lv_kwargs['size'] = int(size)

		data_lv = self._lvm.create_lv(vg, 'data', **lv_kwargs)

		# Create tranzit volume (should be 5% bigger then data vol)
		lvi = self._lvm.lv_info(data_lv)
		size_in_KB = int(read_file('/sys/block/dm-%s/size' % lvi.lv_kernel_minor)) / 2
		tranzit_lv = self._lvm.create_lv(vg, 'tranzit', size='%dK' % (size_in_KB*1.05,))

		return (vg, data_lv, tranzit_lv, size)

	def _destroy_layout(self, vg, data_lv, tranzit_lv):
		# Find PV 
		pv = None
		pvi = firstmatched(lambda pvi: vg in pvi.vg, self._lvm.pv_status())
		if pvi:
			pv = pvi.pv
			
		# Remove storage VG
		self._lvm.change_lv(data_lv, available=False)
		self._lvm.change_lv(tranzit_lv, available=False)
		self._lvm.remove_vg(vg)
		
		if pv:
			# Remove PV if it doesn't belongs to any other VG
			pvi = self._lvm.pv_info(pv)
			if not pvi.vg:
				self._lvm.remove_pv(pv)		
	
	def create(self, **kwargs):
		'''
		@param disk: Physical volume
		@param vg: Uniting volume group
		@param size: Useful storage size (in % of physican volume or MB)
		@param snap_backend: Snapshot backend
		
		Example: 
		Storage.create({
			'type': 'eph',
			'disk': '/dev/sdb',
			'size': '40%',
			'vg': {
				'name': 'mysql_data',
				'ph_extent_size': 10
			},
			'snap_backend': 'cf://mysql_backups/cloudsound/production'
		})
		'''
		if kwargs['lvm_group_cfg']:
			self._lvm.restore_vg(kwargs['vg'], cStringIO.StringIO(kwargs['lvm_group_cfg']))
		else:
			# Create LV layout
			kwargs['vg'], kwargs['device'], tranzit_lv, kwargs['size'] = self._create_layout(
					kwargs['disk'].devname, vg=kwargs.get('vg'), size=kwargs.get('size'))
		
		# Initialize tranzit volume
		kwargs['tranzit_vol'] = Volume(tranzit_lv, '/tmp/sntz' + str(randint(100, 999)), 'ext3', 'base')

		# Accept snapshot backend
		if not isinstance(kwargs['snap_backend'], dict):
			kwargs['snap_backend'] = dict(path=kwargs['snap_backend'])
		
		return super(EphVolumeProvider, self).create(**kwargs)

	def create_from_snapshot(self, **kwargs):
		'''
		...
		@param path: Path to snapshot manifest on remote storage
		
		Example: 
		Storage.create(**{
			'disk' : {
				'type' : 'loop',
				'file' : '/media/storage',
				'size' : 1000
			}
			'snapshot': {
				'type': 'eph',
				'description': 'Last winter mysql backup',
				'path': 'cf://mysql_backups/cloudsound/production/snap-14a356de.manifest.ini'
				'size': '40%',
				'vg': 'mysql_data'
			}
		})
		
		
		'''
		_kwargs = kwargs.copy()
		if not 'snap_backend' in _kwargs:
			_kwargs['snap_backend'] = os.path.dirname(_kwargs['path'])
		vol = self.create(**_kwargs)

		snap = self.snapshot_factory(**kwargs)
		try:
			self._prepare_tranzit_vol(vol.tranzit_vol)
			self._snap_pvd.download(vol, snap, vol.tranzit_vol.mpoint)
			self._snap_pvd.restore(vol, snap, vol.tranzit_vol.mpoint)			
		finally:
			self._cleanup_tranzit_vol(vol.tranzit_vol)
	
		return vol

	def create_snapshot(self, vol, snap):
		try:
			self._prepare_tranzit_vol(vol.tranzit_vol)
			self._snap_pvd.create(vol, snap, vol.tranzit_vol.mpoint)
			return snap
		except:
			self._cleanup_tranzit_vol(vol.tranzit_vol)
			raise
	
	def save_snapshot(self, vol, snap):
		try:
			self._snap_pvd.upload(vol, snap, vol.tranzit_vol.mpoint)
			return snap
		finally:
			self._cleanup_tranzit_vol(vol.tranzit_vol)


	def _prepare_tranzit_vol(self, vol):
		os.makedirs(vol.mpoint)
		vol.mkfs()
		vol.mount()
		
	def _cleanup_tranzit_vol(self, vol):
		vol.umount()
		if os.path.exists(vol.mpoint):
			os.rmdir(vol.mpoint)

	def detach(self, vol, force=False):
		'''
		@type vol: EphVolume
		'''
		super(EphVolumeProvider, self).detach(vol, force)
		if vol.vg:
			vol.lvm_group_cfg = _lvm_group_b64(vol.vg)
			self._destroy_layout(vol.vg, vol.devname, vol.tranzit_vol.devname)
			vol.tranzit_vol = None
		vol.disk.detach(force)
		return vol.config()

	def destroy(self, vol, force=False, **kwargs):
		super(EphVolumeProvider, self).destroy(vol, force, **kwargs)

		# Umount tranzit volume
		self._cleanup_tranzit_vol(vol.tranzit_vol)
		
		# Find PV 
		pv = None
		pvi = firstmatched(lambda pvi: vol.vg in pvi.vg, self._lvm.pv_status())
		if pvi:
			pv = pvi.pv
			
		# Remove storage VG
		self._lvm.change_lv(vol.devname, available=False)
		self._lvm.change_lv(vol.tranzit_vol.devname, available=False)
		self._lvm.remove_vg(vol.vg)
		
		if pv:
			# Remove PV if it doesn't belongs to any other VG
			pvi = self._lvm.pv_info(pv)
			if not pvi.vg:
				self._lvm.remove_pv(pv)		

Storage.explore_provider(EphVolumeProvider)


class EphSnapshotProvider(object):

	MANIFEST_NAME 		= 'manifest.ini'
	SNAPSHOT_LV_NAME 	= 'snap'	
	
	chunk_size = None
	'''	Data chunk size in Mb '''

	_logger = None	
	_transfer = None
	_lvm = None
	
	def __init__(self, chunk_size=10):
		self.chunk_size = chunk_size		
		self._logger = logging.getLogger(__name__)
		self._transfer = Transfer()
		self._lvm = Lvm2()
	
	def create(self, volume, snapshot, tranzit_path):
		# Create LVM snapshot
		snap_lv = None
		chunk_prefix = '%s.data' % snapshot.id
		try:
			snap_lv = self._lvm.create_lv_snapshot(volume.devname, self.SNAPSHOT_LV_NAME, extents='100%FREE')
			
			# Copy|gzip|split snapshot into tranzit volume directory
			self._logger.info('Packing volume %s -> %s', volume.devname, tranzit_path) 
			cmd1 = ['dd', 'if=%s' % snap_lv]
			cmd2 = ['gzip', '-1']
			cmd3 = ['split', '-a','3', '-d', '-b', '%sM' % self.chunk_size, '-', '%s/%s.gz.' % 
					(tranzit_path, chunk_prefix)]
			p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			p3 = subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			out, err = p3.communicate()

			if p3.returncode:
				p1.stdout.close()
				p2.stdout.close()				
				p1.wait()
				p2.wait()
				raise StorageError('Error during coping LVM snapshot device (code: %d) <out>: %s <err>: %s' % 
						(p3.returncode, out, err))
		finally:
			# Remove LVM snapshot			
			if snap_lv:
				self._lvm.remove_lv(snap_lv)			
					
		# Make snapshot manifest
		config = Configuration('ini')
		config.add('snapshot/description', snapshot.description, force=True)
		config.add('snapshot/created_at', time.strftime("%Y-%m-%d %H:%M:%S"))
		config.add('snapshot/pack_method', 'gzip') # Not used yet
		for chunk in glob.glob(os.path.join(tranzit_path, chunk_prefix + '*')):
			config.add('chunks/%s' % os.path.basename(chunk), self._md5sum(chunk), force=True)
		
		manifest_path = os.path.join(tranzit_path, '%s.%s' % (snapshot.id, self.MANIFEST_NAME))
		config.write(manifest_path)

		snapshot.path = manifest_path
		snapshot.vg = os.path.basename(volume.vg)
		snapshot.size = volume.size			
		
		return snapshot

	
	def restore(self, volume, snapshot, tranzit_path):
		# Load manifest
		mnf = Configuration('ini')
		mnf.read(os.path.join(tranzit_path, os.path.basename(snapshot.path)))
		
		# Checksum
		for chunk, md5sum_o in mnf.items('chunks'):
			chunkpath = os.path.join(tranzit_path, chunk)
			md5sum_a = self._md5sum(chunkpath)
			if md5sum_a != md5sum_o:
				raise StorageError(
						'Chunk file %s checksum mismatch. Actual md5sum %s != %s defined in snapshot manifest', 
						chunkpath, md5sum_a, md5sum_o)

		# Restore chunks 
		self._logger.info('Unpacking snapshot from %s -> %s', tranzit_path, volume.devname)
		cat = ['cat']
		catargs = list(os.path.join(tranzit_path, chunk) for chunk in mnf.options('chunks'))
		catargs.sort()
		cat.extend(catargs)
		gunzip = ['gunzip']
		dest = open(volume.devname, 'w')
		#Todo: find out where to extract file
		p1 = subprocess.Popen(cat, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		p2 = subprocess.Popen(gunzip, stdin=p1.stdout, stdout=dest, stderr=subprocess.PIPE)
		out, err = p2.communicate()
		dest.close()
		if p2.returncode:
			p1.stdout.close()
			p1.wait()
			raise StorageError('Error during snapshot restoring (code: %d) <out>: %s <err>: %s' % 
					(p2.returncode, out, err))

	def upload(self, volume, snapshot, tranzit_path):
		mnf = Configuration('ini')
		mnf.read(snapshot.path)
		
		files = [snapshot.path]
		files += [os.path.join(tranzit_path, chunk) for chunk in mnf.options('chunks')]
		
		snapshot.path = self._transfer.upload(files, volume.snap_backend['path'])[0]
		return snapshot

	def download(self, volume, snapshot, tranzit_path):
		# Load manifest
		mnf_path = self._transfer.download(snapshot.path, tranzit_path)[0]
		mnf = Configuration('ini')
		mnf.read(mnf_path)
		
		# Load files
		remote_path = os.path.dirname(snapshot.path)
		files = tuple(os.path.join(remote_path, chunk) for chunk in mnf.options('chunks'))
		self._transfer.download(files, tranzit_path)

	def _md5sum(self, file, block_size=4096):
		fp = open(file, 'rb')
		try:
			md5 = hashlib.md5()
			while True:
				data = fp.read(block_size)
				if not data:
					break
				md5.update(data)
			return binascii.hexlify(md5.digest())
		finally:
			fp.close()

