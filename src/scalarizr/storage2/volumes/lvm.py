'''
Created on Aug 22, 2012

@author: marat
'''

from scalarizr import storage2
from scalarizr.storage2.volumes import base
from scalarizr.linux import lvm2


class LvmVolume(base.Volume):

	def __init__(self, **kwds):
		self.default_config.update({
			'pv': [],
			'vg': None,
			'name': None,
			'size': None,
			'extents': None
		})
		super(LvmVolume, self).__init__(**kwds)
		
		
	def _lvinfo(self):
		return lvm2.lvs(lvm2.lvpath(self.vg, self.name)).values()[0]
		
		
	def _ensure(self):
		self._check_attr('vg')
		
		try:
			lv_info = self._lvinfo()
		except lvm2.NotFound:
			assert self.size or self.extents, 'Please specify either size or extents (not both)'

			pvs = lvm2.pvs()			
			pv_disks = []
			for pv_disk in self.pv:
				if isinstance(pv_disk, basestring):
					pv_disk = storage2.volume(device=pv_disk)
				pv_disk.ensure()
				if pv_disk.device not in pvs:
					lvm2.pvcreate(pv_disk.device)
				pv_disks.append(pv_disk)
			self.pv = pv_disks
			
			try:
				lvm2.vgs(self.vg)
			except lvm2.NotFound:
				lvm2.vgcreate(self.vg, *[disk.device for disk in self.pv])

			kwds = {'name': self.name}
			if self.size:
				kwds['size'] = self.size
			elif self.extents:
				kwds['extents'] = self.extents
			lvm2.lvcreate(self.vg, **kwds)
			lv_info = self._lvinfo()

		self.device = lv_info.lv_path
			
		if lv_info.lv_attr[4] == '-':
			lvm2.lvchange(self.device, available=True)

		
	def _snapshot(self, description, tags, **kwds):
		long_kwds = {
			'name': kwds.get('name', '%snap' % self.name),
			'snapshot': '%s/%s' % (self.vg, self.name)
		}
		if kwds.get('extents'):
			long_kwds['extents'] = kwds['extents']
		elif kwds.get('size'):
			long_kwds['size'] = kwds['size']
		else:
			long_kwds['extents'] = '1%ORIGIN'

		lvm2.lvcreate(**long_kwds)
		lv_info = lvm2.lvs('%s/%s' % (self.vg, long_kwds['name'])).values()[0]
		
		return storage2.snapshot(
				type='lvm', 
				name=lv_info.lv_name, 
				vg=lv_info.vg_name,
				device=lv_info.lv_path)


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
			

		
class LvmSnapshot(base.Snapshot):
			
	def _destroy(self):
		lvm2.lvremove(self.device)

	
	def _status(self):
		try:
			lvm2.lvs(self.device)
			return self.COMPLETED
		except lvm2.NotFound:
			return self.FAILED


storage2.volume_types['lvm'] = LvmVolume
storage2.snapshot_types['lvm'] = LvmSnapshot
		