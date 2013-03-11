from __future__ import with_statement
from __future__ import with_statement

import os
import sys
import logging
import tempfile
import urlparse
import threading

from scalarizr import storage2
from scalarizr.libs import metaconf
from scalarizr.linux import coreutils
from scalarizr.storage2 import cloudfs
from scalarizr.storage2.volumes import base


LOG = logging.getLogger(__name__)


class EphVolume(base.Volume):
	"""
	Represents LVM layout over volumes of any type.
	It differs from lvm volume in the way of making snapshot.
	Ephemeral snapshot freezes lvm layout (creates pure LVM snapshot), then it uploads
	all the data on this logical volume to cloud storage provider (whereas lvm volume
	snapshots underlying disks)
	"""

	
	def __init__(self, vg=None, disk=None, size=None, cloudfs_dir=None,	**kwds):
		# Compatibility with 1.0
		snap_backend = kwds.pop('snap_backend', None)
		if snap_backend:
			cloudfs_dir = snap_backend['path'] \
						if isinstance(snap_backend, dict) \
						else snap_backend
			if not cloudfs_dir.endswith('/'):
				cloudfs_dir += '/'
		kwds.pop('lvm_group_cfg', None)

		super(EphVolume, self).__init__(vg=vg, disk=disk, size=size,
												cloudfs_dir=cloudfs_dir, **kwds)

		self._lvm_volume = None


	def _ensure(self):
		# snap should be applied after layout: download and extract data.
		# this could be done on already ensured volume. 
		# Example: resync slave data

		if not self._lvm_volume:
			# First of all, merge self config and snapshot config
			self.snap = storage2.snapshot(self.snap) if self.snap else None

			for attr in ('disk', 'fstype', 'size', 'vg', 'mpoint'):
				if not getattr(self, attr, None):
					if not self.snap or not getattr(self.snap, attr, None):
						raise storage2.StorageError('Missing ephemeral volume'
															' attribute "%s"' % attr)
					setattr(self, attr, getattr(self.snap, attr))

			self.disk = storage2.volume(self.disk)
			if self.disk.device and self.disk.device.startswith('/dev/sd'):
				self.disk = storage2.volume(type='ec2_ephemeral', name='ephemeral0')

			self._lvm_volume = storage2.volume(
					type='lvm',
					pvs=[self.disk],
					size=self.size + 'VG',
					vg=self.vg,
					name='data')

		self._lvm_volume.ensure()
		self.device = self._lvm_volume.device

		if self.snap:
			self.snap = storage2.snapshot(self.snap)
			if not self.is_fs_created():
				self.mkfs()

			tmp_mpoint = not self.mpoint
			if tmp_mpoint:
				tmp_mpoint = tempfile.mkdtemp()
				self.mpoint = tmp_mpoint

			try:
				transfer = cloudfs.LargeTransfer(self.snap.path, self.mpoint + '/')
				self.mount()
				if hasattr(self.snap, 'data_size'):
					fs_free = coreutils.statvfs(self.mpoint)['avail']
					if fs_free < int(self.snap.data_size):
						raise storage2.StorageError('Not enough free space'
								' on device %s to restore snapshot.' %
								self.device)

				result = transfer.run()
				if result.get('failed'):
					err = result['failed'][0]['exc_info'][1]
					raise storage2.StorageError('Failed to download snapshot'
												'data. %s' % err)
			except:
				e = sys.exc_info()[1]
				raise storage2.StorageError("Snapshot restore error: %s" % e)
			finally:
				try:
					self.umount()
				finally:
					if tmp_mpoint:
						self.mpoint = None
						os.rmdir(tmp_mpoint)

			self.snap = None


	def _snapshot(self, description, tags, **kwds):
		snap = storage2.snapshot(type='eph')
		lvm_snap = self._lvm_volume.lvm_snapshot(size='100%FREE')

		t = threading.Thread(target=snap.upload_lvm_snapshot, args=(lvm_snap, tags, self.cloudfs_dir))
		t.start()
		return snap


	def _destroy(self, force, **kwds):
		self._lvm_volume.destroy(force=force)
		self.device = None


	def _detach(self, force, **kwds):
		self._lvm_volume.detach(force=force, **kwds)


class EphSnapshot(base.Snapshot):
	"""
	Respresents snapshot of data on ephemeral volume, uploaded to cloud storage provider.
	Contains all necessary info to restore functionall ephemeral storage.

	"""

	def _destroy(self):
		"""
		Reads chunks paths from manifest, then deletes manifest and chunks
		from cloud storage.
		"""
		self._check_attr('path')
		scheme = urlparse.urlparse(self.path).scheme
		storage_drv = cloudfs.cloudfs(scheme)

		base_url = os.path.dirname(self.path)
		manifest_path = tempfile.mktemp()
		try:
			with open(manifest_path, 'w') as f:
				storage_drv.get(self.path, f)

			c = metaconf.Configuration('ini')
			c.read(manifest_path)
			for chunk in c.children('./chunks/'):
				chunk_path = os.path.join(base_url, chunk)
				storage_drv.delete(chunk_path)
			storage_drv.delete(self.path)
			self.path = None
		finally:
			os.remove(manifest_path)


	def _status(self):
		"""
		Represents current status of ephemeral snapshot.
		Status updates exclusively in 'snapshot' method of ephemeral volume
		"""
		if hasattr(self, '_snap_status'):
			return self._snap_status
		else:
			return self.UNKNOWN


	def upload_lvm_snapshot(self, lvm_snap, tags, path):
		"""
		Method which uploads data from lvm snapshot to cloud storage and
		updates snapshot status.

		EphVolume runs this method in separate thread
		"""

		try:
			self._snap_status = self.QUEUED

			lvm_snap_vol = storage2.volume(
				device=lvm_snap.device,
				mpoint=tempfile.mkdtemp())
			lvm_snap_vol.ensure(mount=True)
			self.data_size = coreutils.statvfs(lvm_snap_vol.mpoint)['used']

			try:
				transfer = cloudfs.LargeTransfer(
					src=lvm_snap_vol.mpoint + '/',
					dst=path,
					tar_it=True,
					gzip_it=True,
					tags=tags,
					transfer_id=self.id)
				self._snap_status = self.IN_PROGRESS
				manifesto = transfer.run()
				self.path = manifesto.cloudfs_path
				self._snap_status = self.COMPLETED

			finally:
				lvm_snap_vol.umount()
				os.rmdir(lvm_snap_vol.mpoint)

		except:
			self._snap_status = self.FAILED
		finally:
			lvm_snap.destroy()

"""
class EphVolumeAdapter(EphVolume):
	
	def __init__(self, **kwds):
		super(EphVolumeAdapter, self).__init__(**kwds)
		self.snap_backend = self._config.pop('cloudfs_dir')
		self._eph_vol = None
		self._eph_pvd = old_eph.EphVolumeProvider()
		
		
	def _ensure(self):
		if self.snap:
			config = self.snap \
					if isinstance(self.snap, dict) \
					else self.snap.config()
		else:	
			config = self.config()

		disk = storage2.volume(config['disk'])
		if disk.device and disk.device.startswith('/dev/sd'):
			disk = storage2.volume(type='ec2_ephemeral', name='ephemeral0')
		disk.ensure()
		self.disk = config['disk'] = disk

		if self.snap:
			if self._eph_vol:
				self._eph_vol.detach(force=True)
			self._eph_vol = self._eph_pvd.create_from_snapshot(**config)
			self.snap = None
		else:
			self._eph_vol = self._eph_pvd.create(**config)
		
		self.device = self._eph_vol.device
		
		
	def _snapshot(self, description, tags, **kwds):
		conf = self._eph_vol.config()
		del conf['id']
		eph_snap = self._eph_pvd.snapshot_factory(description, **conf)
		eph_snap = self._eph_pvd.create_snapshot(self._eph_vol, eph_snap, **kwds)
		
		snap = storage2.snapshot(type='eph')
		snap._config.update(eph_snap.config())
		snap._eph_pvd = self._eph_pvd
		return snap
	
	
	def _destroy(self, force, **kwds):
		self._eph_pvd.destroy(self._eph_vol, force, **kwds)	
	

	def _detach(self, force, **kwds):
		self._eph_pvd.detach(self._eph_vol, force)


class EphSnapshotAdapter(base.Snapshot):
	_eph_pvd = None
	
	def _status(self):
		return self._eph_pvd.get_snapshot_state(self)
"""

storage2.volume_types['eph'] = EphVolume
#storage2.volume_types['eph'] = EphVolumeAdapter
storage2.snapshot_types['eph'] = EphSnapshot
#storage2.snapshot_types['eph'] = EphSnapshotAdapter

