
from scalarizr.linux import mount
from scalarizr import storage2
from scalarizr.storage2.volumes import base


class TmpfsVolume(base.Volume):

	error_messages = base.Volume.error_messages.copy()
	error_messages.update({
		'invalid_size': 'Volume size required and should be int value in Mb'
	})

	default_config = base.Volume.default_config.copy()
	default_config.update({
		'size': None   # size in Mb
	})


	def _ensure(self):
		expr = isinstance(self.size, int) and self.size
		assert expr, self.error_messages['invalid_size']
		self._check_attr('mpoint')
		if not self.device:
			if not os.path.exists(self.mpoint):
				os.makedirs(self.mpoint)
			mount.mount('tmpfs', self.mpoint, 
					'-t', 'tmpfs', 
					'-o', 'size=%sM' % self.size)
			self.device = self.mpoint
			

	def _destroy(self):
		mount.umount(self.mpoint)


storage2.volume_types['tmpfs'] = TmpfsVolume

