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

	features = base.Volume.features.copy()
	features.update({
		'restore': True
	})

	default_config = base.Volume.default_config.copy()
	default_config.update({
		#Raid disks
		'disks': [],

		#Raid device name, e.g: /dev/md0
		'raid_pv': None,

		#Raid level 0|1|5|10
		'level': None,

		#LVM volume group configuration (base64 encoded)
		'lvm_group_cfg': None,

		#LVM volume group name
		'vg': None,

		#Mdadm device physical volume id
		'pv_uuid': None
	})

	lv_re = re.compile(r'Logical volume "([^\"]+)" created')

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

			self.vg = self.snap.vg
			self.level = int(self.snap.level)
			self.pv_uuid = self.snap.pv_uuid
			self.lvm_group_cfg = self.snap.lvm_group_cfg

			# Here?
			self.snap = None

		disks_devices = [disk.device for disk in self.disks]
		vg_name = os.path.basename(self.vg)

		if self.lvm_group_cfg:
			self._check_attr('device')
			raid_device = mdadm.mdfind(*disks_devices)
			if not raid_device:
				raid_device = mdadm.findname()
				if self.level in (1, 10):
					for disk in disks_devices:
						mdadm.mdadm('misc', disk,
									zero_superblock=True, force=True)
					mdadm.mdadm('create', raid_device, *disks_devices,
								force=True,
								level=self.level,
								assume_clean=True,
								raid_devices=len(disks_devices))
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
					f.write(base64.b64encode(self.lvm_group_cfg))
				lvm2.vgcfgrestore(vg_name, file=tmpfile)
			finally:
				os.remove(tmpfile)

			# Check that logical volume exists
			lvm2.lvs(self.device)

			# Activate volume group
			lvm2.vgchange(vg_name, available='y')

			# Wait for logical volume device file
			util.wait_until(lambda: os.path.exists(self.device),
						timeout=120, logger=LOG,
						error_text='Logical volume %s not found' % self.device)

		else:
			raid_device = mdadm.findname()
			mdadm.mdadm('create', raid_device, *disks_devices,
						force=True, level=self.level, assume_clean=True)

			lvm2.pvcreate(raid_device)
			self.pv_uuid = lvm2.pvs(raid_device)[raid_device].pv_uuid

			lvm2.vgcreate(vg_name, raid_device)

			out, err = lvm2.lvcreate()[:2]
			try:
				vol = re.match(self.lv_re, out.split('\n')[-1].strip()).group(1)
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

		mdadm.mdadm('misc', self.raid_pv, stop=True, force=True)
		mdadm.mdadm('manage', self.raid_pv, remove=True, force=True)

		self.raid_pv = None

		for disk in self.disks:
			disk.detach()


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
		for disk in self.disks:
			disk.detach(force=force)

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

	# STATUS?
storage2.volume_types['raid'] = RaidVolume
storage2.snapshot_types['raid'] = RaidSnapshot