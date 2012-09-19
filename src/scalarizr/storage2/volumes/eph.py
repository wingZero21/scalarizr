
from scalarizr import storage2
from scalarizr.storage2 import cloudfs
from scalarizr.storage2.volumes import base


class EphVolume(base.Volume):
	
	def __init__(self, **kwds):
		self.default_config.update({
			'vg': None,
			'disk': None,
			'size': None,
			'cloudfs_dir': None
		})
		self._lvm_volume = None
		self._transfer = None

		# Compatibility with 1.0
		snap_backed = kwds.pop('snap_backend')
		kwds['cloudfs_dir'] = snap_backend['path'] + '/'
		kwds.pop('lvm_group_cfg', None)

		super(EphVolume, self).__init__(**kwds)

		
	def _ensure(self):
		# snap should be applied after layout: download and extract data.
		# this could be done on already ensured volume. 
		# Example: resync slave data
		if not self._lvm_volume:
			self._lvm_volume = storage2.volume(
					type='lvm',
					pvs=[self._disk],
					size=self.size + 'VG',
					vg=self.vg,
					name='data')
			self._lvm_volume.ensure()
			self.device = self._lvm_volume.device
		if self.snap:
			self.mkfs()
			tmp_mpoint = not self.mpoint
			if tmp_mpoint:
				self.mpoint = tempfile.mkdtemp()
			self.mount()
			transfer = cloudfs.LargeTransfer(self.snap.path, mpoint + '/')
			try:
				self.mpoint = tmp_mpoint
				self.mount()
				transfer.run()
			except:
				exc_info = sys.exc_info()
			finally:
				try:
					self.umount()
					os.remove(tmp_mpoint)
					self.mpoint = mpoint
					if mounted_to:
						self.mount()
				except:
					LOG.warn('Failed to restore volume mpoint after file transfer')
			self.snap = None


	def _snapshot(self):
		lvm_snap = self._lvm_volume.lvm_snapshot(size='100%FREE')
		snap = storage2.snapshot(type='eph')
		snap.path = os.path.join(os.path.join(
						self.cloudfs_dir, snap.id + '.manifest.ini'))
		lvm_snap_vol = storage2.volume(
						device=lvm_snap.device, 
						mpoint=tempfile.mkdtemp())
		transfer = cloudfs.LargeTransfer(
						src=lvm_snap_vol.mpoint + '/',
						dst=snap.path
						tar_it=True,
						gzip_it=True)
		try:
			transfer.run()
		finally:
			lvm_snap_vol.umount()
			os.remove(lvm_snap_vol.mpoint)
			lvm_snap.destroy()

		return snap



