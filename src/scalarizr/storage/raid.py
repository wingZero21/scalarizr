'''
Created on Jan 6, 2011

@author: marat
'''

from . import VolumeConfig, Volume, Snapshot, VolumeProvider, Storage, \
		StorageError, system, devname_not_empty
from .util.mdadm import Mdadm
from .util.lvm2 import Lvm2, lvm_group_b64

from scalarizr.util import firstmatched
from scalarizr.util.filetool import write_file, read_file

import logging
import os
import time
import binascii

logger = logging.getLogger(__name__)



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

	
	@devname_not_empty
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
	
	@devname_not_empty
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
				


	@devname_not_empty			
	def detach(self, vol, force=False):
		self._logger.debug('Detaching volume %s' % vol.devname)
		super(RaidVolumeProvider, self).detach(vol, force)
		pv_uuid = system(('pvs', '-o', 'pv_uuid', vol.raid_pv))[0].splitlines()[1].strip()
		vol.lvm_group_cfg = lvm_group_b64(vol.vg)
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
