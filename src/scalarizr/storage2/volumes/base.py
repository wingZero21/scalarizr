
import uuid

from scalarizr import storage2
from scalarizr.linux import mount as mountmod
import os


LOG = storage2.LOG


class Base(object):

	default_config = {
		'version': '2.0',
		'type': 'base',
		'id': None
	}
	_config = None
	initial_config = None
	
	def __init__(self, **kwds):
		if not self._config:
			self._config = self.default_config.copy()
		self._config.update(kwds)
		self.initial_config = self._config.copy()


	def config(self):
		return self._dictify(self._config)

	
	def __setattr__(self, name, value):
		data = self.__dict__ if name in dir(self) else self.__dict__['_config']
		data[name] = value
	
	
	def __getattr__(self, name):
		if name in self.__dict__['_config']:
			return self.__dict__['_config'][name]
		raise AttributeError(name)
	
	
	def __hasattr__(self, name):
		return name in self.__dict__['_config']
	
	
	def _dictify(self, data=None):
		if isinstance(data, dict):
			ret = {}
			for key in data:
				ret[key] = self._dictify(data[key])
			return ret
		elif isinstance(data, list):
			ret = [self._dictify(item) for item in data]
		elif isinstance(data, Base):
			ret = data.config()
		else:
			ret = data
			
		return ret

	def _genid(self, prefix=''):
		return '%s%s-%s' % (prefix, self.type, uuid.uuid4().hex[0:8])		


class Volume(Base):
	MAX_SIZE = None
	
	error_messages = {
		'empty_attr': 'Attribute should be specified: %s',
		'restore_unsupported': 'Restores from snapshot not supported by this volume type: %s',
	}
	
	features = {
		'restore': False
	}

	def __init__(self, **kwds):
		self.default_config.update({
			'device': None,
			'fstype': 'ext3',
			'fscreated': False,
			'mpoint': None,
			'snap': None
		})
		super(Volume, self).__init__(**kwds)
		

	def ensure(self, mount=False, mkfs=False, **updates):
		if not self.features['restore']:
			self._check_restore_unsupported()
		if self.snap and isinstance(self.snap, Snapshot):
			self.snap = self.snap.config()
		self._ensure()
		self._check_attr('device')
		if not self.id:
			self.id = self._genid('vol-')
		if mount:
			try:
				self.mount()
			except mountmod.NoFileSystem:
				if mkfs:
					self.mkfs()
					self.mount()
				else:
					raise
		return self.config()
	
	
	def snapshot(self, description=None, tags=None, **kwds):
		return self._snapshot(description, tags, **kwds)


	def destroy(self, force=False, **kwds):
		if self.device:
			self.detach(force, **kwds)
		self._destroy(force, **kwds)


	def detach(self, force=False, **kwds):
		if not self.device:
			return
		self.umount()
		self._detach(force, **kwds)


	def mount(self):
		self._check(mpoint=True)
		mounted_to = self.mounted_to()
		if mounted_to == self.mpoint:
			return
		elif mounted_to: 
			self.umount()
		if not os.path.exists(self.mpoint):
			os.makedirs(self.mpoint)
		mountmod.mount(self.device, self.mpoint)


	def umount(self):
		self._check()
		mountmod.umount(self.device)


	def mounted_to(self):
		self._check()
		try:
			return mountmod.mounts()[self.device].mpoint
		except KeyError:
			return False


	def mkfs(self):
		self._check()
		fs = storage2.filesystem(self.fstype)
		LOG.info('Creating filesystem on %s', self.device)
		fs.mkfs(self.device)
		self.fscreated = True


	def _check(self, fstype=True, device=True, **kwds):
		if fstype:
			self._check_attr('fstype')
		if device:
			self._check_attr('device')
		for name in kwds:
			self._check_attr(name)

	
	def _check_attr(self, name):
		assert hasattr(self, name) and getattr(self, name),  \
				self.error_messages['empty_attr'] % name
	
	def _check_restore_unsupported(self):
		if self.snap:
			msg = self.error_messages['restore_unsupported'] % self.type
			raise NotImplementedError(msg)
		
	
	def _ensure(self):
		pass
	
	
	def _snapshot(self, description, tags, **kwds):
		pass


	def _detach(self, force, **kwds):
		pass

	
	def _destroy(self, force, **kwds):
		pass


storage2.volume_types['base'] = Volume	
	
	
class Snapshot(Base):
	QUEUED = 'queued'
	IN_PROGRESS = 'in-progress'
	COMPLETED = 'completed'
	FAILED = 'failed'

	def __init__(self, **kwds):
		super(Snapshot, self).__init__(**kwds)
		if not self._config.get('id'):
			self._config['id'] = self._genid('snap-')
	
	def destroy(self):
		return self._destroy()
	
	
	def status(self):
		return self._status()
	

	def _destroy(self):
		pass
	
	def _status(self):
		pass
	

storage2.snapshot_types['base'] = Snapshot
