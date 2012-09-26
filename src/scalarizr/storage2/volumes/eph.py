import os
import sys
import logging
import tempfile
import urlparse

from scalarizr import storage2
from scalarizr.libs import metaconf
from scalarizr.util import filetool
from scalarizr.storage2 import cloudfs
from scalarizr.storage2.volumes import base


LOG = logging.getLogger(__name__)


class EphVolume(base.Volume):
	
	def __init__(self, 
				vg=None, 
				disk=None, 
				size=None, 
				cloudfs_dir=None, 
				**kwds):
		'''
		:type vg:
		:param vg:

		:type disk:
		:param disk:

		:type size:
		:param size:

		:type cloudfs_dir:
		:param cloudfs_dir:
		'''
		self._transfer = None

		# Compatibility with 1.0
		snap_backend = kwds.get('snap_backend')
		if snap_backend:
			kwds.pop('snap_backend')
			kwds['cloudfs_dir'] = snap_backend['path'] + '/'
		kwds.pop('lvm_group_cfg', None)

		super(EphVolume, self).__init__(vg=vg, disk=disk, size=size,
				cloudfs_dir=cloudfs_dir, **kwds)

		
	def _ensure(self):
		# snap should be applied after layout: download and extract data.
		# this could be done on already ensured volume. 
		# Example: resync slave data
		if not hasattr(self, '_lvm_volume'):
			self._lvm_volume = storage2.volume(
					type='lvm',
					pvs=[self.disk],
					size=self.size + 'VG',
					vg=self.vg,
					name='data')

		self._lvm_volume.ensure()
		self.device = self._lvm_volume.device

		if self.snap:
			self.mkfs()
			tmp_mpoint = not self.mpoint
			if tmp_mpoint:
				tmp_mpoint = tempfile.mkdtemp()
				self.mpoint = tmp_mpoint

			transfer = cloudfs.LargeTransfer(self.snap.path, self.mpoint + '/')
			try:
				self.mount()
				transfer.run()
			except:
				e = sys.exc_info()[1]
				raise storage2.StorageError("Snapshot restore error: %s" % e)
			finally:
				try:
					if tmp_mpoint:
						self.mpoint = None
						os.remove(tmp_mpoint)
				finally:
					self.umount()

			self.snap = None


	def _snapshot(self, description, tags, **kwds):
		lvm_snap = self._lvm_volume.lvm_snapshot(size='100%FREE')
		try:
			snap = storage2.snapshot(type='eph')
			snap.path = os.path.join(os.path.join(
							self.cloudfs_dir, snap.id + '.manifest.ini'))

			lvm_snap_vol = storage2.volume(
							device=lvm_snap.device,
							mpoint=tempfile.mkdtemp())
			lvm_snap_vol.ensure(mount=True)

			df_info = filetool.df()
			df = filter(lambda x: x.mpoint == lvm_snap_vol.mpoint, df_info)
			snap.size = df[0].used

			try:
				transfer = cloudfs.LargeTransfer(
								src=lvm_snap_vol.mpoint + '/',
								dst=snap.path,
								tar_it=True,
								gzip_it=True)
				transfer.run()
			finally:
				lvm_snap_vol.umount()
				os.remove(lvm_snap_vol.mpoint)
		finally:
			lvm_snap.destroy()

		return snap


	def _destroy(self, force, **kwds):
		self._lvm_volume.destroy(force=force)
		self.device = None


	def _detach(self, force, **kwds):
		self.umount()
		self._lvm_volume.detach(force=force, **kwds)


class EphSnapshot(base.Snapshot):

	def _destroy(self):
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
		finally:
			os.remove(manifest_path)


	def _status(self):
		return self.UNKNOWN


storage2.volume_types['eph'] = EphVolume
storage2.snapshot_types['eph'] = EphSnapshot

