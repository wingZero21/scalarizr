from __future__ import with_statement

__author__ = 'Nick Demyanchuk'

import os
import re
import base64
import logging
import tempfile

from scalarizr import storage2, util
from scalarizr.linux import mdadm, lvm2, coreutils
from scalarizr.storage2.volumes import base


LOG = logging.getLogger(__name__)


class RaidVolume(base.Volume):


	lv_re = re.compile(r'Logical volume "([^\"]+)" created')

	
	def __init__(self, 
				disks=None, raid_pv=None, level=None, lvm_group_cfg=None, 
				vg=None, pv_uuid=None, **kwds):
		'''
		:type disks: list
		:param disks: Raid disks

		:type raid_pv: string
		:param raid_pv: Raid device name (e.g: /dev/md0)

		:type level: int
		:param level: Raid level. Valid values are
			* 0
			* 1
			* 5
			* 10

		:type lvm_group_cfg: string
		:param lvm_group_cfg: LVM volume group configuration (base64 encoded)

		:type vg: string
		:param vg: LVM volume group name

		:type pv_uuid: string
		:param pv_uuid: Mdadm device physical volume id
		'''
		super(RaidVolume, self).__init__(disks=disks or [],
				raid_pv=raid_pv, level=level and int(level), 
				lvm_group_cfg=lvm_group_cfg,
				vg=vg, pv_uuid=pv_uuid, **kwds)


	def _ensure(self):
		if self.snap:
			disks = []
			try:
				for disk_snap in self.snap['disks']:
					snap = storage2.snapshot(disk_snap)
					disks.append(snap.restore())
			except:
				for disk in disks:
					disk.destroy()
				raise

			self.disks = disks

			self.vg = self.snap['vg']
			self.level = int(self.snap['level'])
			self.pv_uuid = self.snap['pv_uuid']
			self.lvm_group_cfg = self.snap['lvm_group_cfg']

			self.snap = None

		self._check_attr('level')
		self._check_attr('vg')
		self._check_attr('disks')

		assert int(self.level) in (0,1,5,10),\
									'Unknown raid level: %s' % self.level

		disks = []
		for disk in self.disks:
			disk = storage2.volume(disk)
			disk.ensure()
			disks.append(disk)
		self.disks = disks

		disks_devices = [disk.device for disk in self.disks]
		vg_name = os.path.basename(self.vg)

		if self.lvm_group_cfg:
			try:
				raid_device = mdadm.mdfind(*disks_devices)
			except storage2.StorageError:
				raid_device = mdadm.findname()
				if self.level in (1, 10):
					for disk in disks_devices:
						mdadm.mdadm('misc', None, disk,
									zero_superblock=True, force=True)

					kwargs = dict(force=True, metadata='default',
								  level=self.level, assume_clean=True,
								  raid_devices=len(disks_devices))
					mdadm.mdadm('create', raid_device, *disks_devices, **kwargs)
				else:
					mdadm.mdadm('assemble', raid_device, *disks_devices)

				mdadm.mdadm('misc', raid_device, wait=True)

			try:
				lvm2.pvs(raid_device)
			except:
				lvm2.pvcreate(raid_device, uuid=self.pv_uuid)

			# Restore vg
			tmpfile = tempfile.mktemp()
			try:
				with open(tmpfile, 'w') as f:
					f.write(base64.b64decode(self.lvm_group_cfg))
				lvm2.vgcfgrestore(vg_name, file=tmpfile)
			finally:
				os.remove(tmpfile)

			# Check that logical volume exists
			lv_infos = lvm2.lvs(self.vg)
			if not lv_infos:
				raise storage2.StorageError(
					'No logical volumes found in %s vol. group')
			lv_name = lv_infos.popitem()[1].lv_name
			self.device = lvm2.lvpath(self.vg, lv_name)

			# Activate volume group
			lvm2.vgchange(vg_name, available='y')

			# Wait for logical volume device file
			util.wait_until(lambda: os.path.exists(self.device),
						timeout=120, logger=LOG,
						error_text='Logical volume %s not found' % self.device)

		else:
			raid_device = mdadm.findname()
			mdadm.mdadm('create', raid_device, *disks_devices,
						force=True, level=self.level, assume_clean=True,
						raid_devices=len(disks_devices), metadata='default')
			mdadm.mdadm('misc', raid_device, wait=True)

			lvm2.pvcreate(raid_device, force=True)
			self.pv_uuid = lvm2.pvs(raid_device)[raid_device].pv_uuid

			lvm2.vgcreate(vg_name, raid_device)

			out, err = lvm2.lvcreate(vg_name, extents='100%FREE')[:2]
			try:
				clean_out = out.strip().split('\n')[-1].strip()
				vol = re.match(self.lv_re, clean_out).group(1)
				self.device = lvm2.lvpath(vg_name, vol)
			except:
				e = 'Logical volume creation failed: %s\n%s' % (out, err)
				raise Exception(e)

			self.lvm_group_cfg = lvm2.backup_vg_config(vg_name)

		self.raid_pv = raid_device


	def _detach(self, force, **kwds):
		self.lvm_group_cfg = lvm2.backup_vg_config(self.vg)
		lvm2.vgremove(self.vg, force=True)
		self.device = None
		lvm2.pvremove(self.raid_pv, force=True)

		mdadm.mdadm('misc', None, self.raid_pv, stop=True, force=True)
		try:
			mdadm.mdadm('manage', None, self.raid_pv, remove=True, force=True)
		except (Exception, BaseException), e:
			if not 'No such file or directory' in str(e):
				raise

		try:
			os.remove(self.raid_pv)
		except:
			pass

		self.raid_pv = None

		for disk in self.disks:
			disk.detach(force=force)


	def _snapshot(self, description, tags, **kwds):
		coreutils.sync()
		lvm2.dmsetup('suspend', self.device)
		try:
			descr = 'Raid%s disk ${index}. %s' % (self.level, description or '')
			disks_snaps = storage2.concurrent_snapshot(
				volumes=self.disks,
				description=descr,
				tags=tags, **kwds
			)

			return storage2.snapshot(
				type='raid',
				disks=disks_snaps,
				lvm_group_cfg=lvm2.backup_vg_config(self.vg),
				level=self.level,
				pv_uuid=self.pv_uuid,
				vg=self.vg
			)
		finally:
			lvm2.dmsetup('resume', self.device)


	def _destroy(self, force, **kwds):
		remove_disks = kwds.get('remove_disks')
		if remove_disks:
			for disk in self.disks:
				disk.destroy(force=force)
			self.disks = []


class RaidSnapshot(base.Snapshot):

	def __init__(self, **kwds):
		super(RaidSnapshot, self).__init__(**kwds)
		self.disks = map(storage2.snapshot, self.disks)


	def _destroy(self):
		for disk in self.disks:
			disk.destroy()


	def _status(self):
		if all((snap.status() == self.COMPLETED for snap in self.disks)):
			return self.COMPLETED
		elif any((snap.status() == self.FAILED for snap in self.disks)):
			return self.FAILED
		elif any((snap.status() == self.IN_PROGRESS for snap in self.disks)):
			return self.IN_PROGRESS
		return self.UNKNOWN


storage2.volume_types['raid'] = RaidVolume
storage2.snapshot_types['raid'] = RaidSnapshot
