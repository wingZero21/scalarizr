
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
		raise NotImplementedError()

