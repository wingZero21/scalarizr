
from scalarizr.linux import lvm2
from scalarizr.storage2.volumes import base


class EphVolume(base.Volume):
	
	def __init__(self, **kwds):
		self.default_config.update({
			'vg': None,
			'lvm_group_cfg': None,
			'disk': None,
			'size': None,
			'snap_backend': None
		})
		super(EphVolume, self).__init__(**kwds)
		
	def _ensure(self):
		# snap should be applied after layout: download and extract data.
		# this could be done on already ensured volume. 
		# Example: resync slave data 
		raise NotImplementedError()

