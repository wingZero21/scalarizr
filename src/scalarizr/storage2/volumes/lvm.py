'''
Created on Aug 22, 2012

@author: marat
'''

from scalarizr import storage2
from scalarizr.storage2.volumes import base
from scalarizr.linux import lvm2, coreutils


class LvmVolume(base.Volume):
	features = base.Volume.features.copy()
	features.update({
		'restore': True
	})

	default_config = base.Volume.default_config.copy()
	default_config.update({
		'pvs': [],
		# Physical volumes
		'vg': None,
		# Volume group name
		'name': None,
		# Logical volume name
		'size': None   
		# Logical volume size <int>[bBsSkKmMgGtTpPeE] or %{VG|PVS|FREE|ORIGIN}
	})


	def _lvinfo(self):
		return lvm2.lvs(lvm2.lvpath(self.vg, self.name)).values()[0]
		

	def _ensure(self):
		if self.snap:
			for snap in self.snap.pv_snaps:
				if isinstance(snap, dict):
					snap = storage2.snapshot(**snap)
				# TODO: restore
				# TODO: think how to avoid such isinstance checks
				pass 
		
		self._check_attr('vg')
		
		try:
			lv_info = self._lvinfo()
		except lvm2.NotFound:
			self._check_attr('size')

			pvs = lvm2.pvs()			
			pv_volumes = []
			for pv_volume in self.pvs:
				if isinstance(pv_volume, dict):
					pv_volume = storage2.volume(**pv_volume)
				elif isinstance(pv_volume, basestring):
					pv_volume = storage2.volume(device=pv_volume)
				pv_volume.ensure()
				if pv_volume.device not in pvs:
					lvm2.pvcreate(pv_volume.device)
				pv_volumes.append(pv_volume)
			self.pvs = pv_volumes
			
			try:
				lvm2.vgs(self.vg)
			except lvm2.NotFound:
				lvm2.vgcreate(self.vg, *[disk.device for disk in self.pv])

			kwds = {'name': self.name}
			if '%' in str(self.size):
				kwds['extents'] = self.size
			else:
				try:
					int(self.size)
					kwds['size'] = '%sG' self.size
				except:
					kwds['size'] = self.size
			lvm2.lvcreate(self.vg, **kwds)
			lv_info = self._lvinfo()

		self.device = lv_info.lv_path
			
		if lv_info.lv_attr[4] == '-':
			lvm2.lvchange(self.device, available=True)


	def lvm_snapshot(self, name=None, size=None):
		long_kwds = {
			'name': name or '%snap' % self.name,
			'snapshot': '%s/%s' % (self.vg, self.name)
		}
		if size:
			if '%' in size:
				long_kwds['extents'] = size
			else:
				long_kwds['size'] = size
		else:
			long_kwds['extents'] = '1%ORIGIN'

		lvm2.lvcreate(**long_kwds)
		lv_info = lvm2.lvs('%s/%s' % (self.vg, long_kwds['name'])).values()[0]

		return storage2.snapshot(
				type='lvm', 
				name=lv_info.lv_name, 
				vg=lv_info.vg_name,
				device=lv_info.lv_path)
	
		
	def _snapshot(self, description, tags, **kwds):
		coreutils.dmsetup('suspend', self.device)
		try:
			if not description:
				description = self.id
			description += ' PV-${index}'
			pv_snaps = storage2.concurrent_snapshot(self.pvs, 
									description, tags, **kwds)
			return storage2.snapshot(
					type='lvm',
					pv_snaps=pv_snaps,
					vg=self.vg,
					name=self.name)
		finally:
			coreutils.dmsetup('resume', self.device) 
		
		
	def _detach(self, force, **kwds):
		lvm2.lvchange(self.device, available='n')

	
	def _destroy(self, force, **kwds):
		try:
			lvm2.lvremove(self.device)
		except lvm2.NotFound:
			pass

		if force:
			try:
				vg_info = lvm2.vgs(self.vg).values()[0]
			except lvm2.NotFound:
				pass
			else:
				if not (int(vg_info.snap_count) or int(vg_info.lv_count)):
					pv_disks = [device for device, pv_info in lvm2.pvs() 
								if pv_info.vg_name == self.vg]
					lvm2.vgremove(self.vg)
					for device in pv_disks:
						lvm2.pvremove(device)

			
class LvmNativeSnapshot(base.Snapshot):
	def _destroy(self):
		lvm2.lvremove(self.device)

	
	def _status(self):
		try:
			lvm2.lvs(self.device)
			return self.COMPLETED
		except lvm2.NotFound:
			return self.FAILED


class LvmSnapshot(base.Snapshot):
	def _destroy(self):
		for snap in self.pv_snaps:
			if isinstance(snap, dict):
				snap = storage2.snapshot(**snap)
			snap.destroy()
			
			
	def _status(self):
		if all((snap.status() == self.COMPLETED for snap in self.pv_snaps)):
			return self.COMPLETED
		elif any((snap.status() == self.FAILED for snap in self.pv_snaps)):
			return self.FAILED
		return self.UNKNOWN

		
storage2.volume_types['lvm'] = LvmVolume
storage2.snapshot_types['lvm'] = LvmSnapshot
storage2.snapshot_types['lvm-native'] = LvmNativeSnapshot

