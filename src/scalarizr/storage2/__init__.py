import uuid


def create_volume():
	pass

def create_snapshot():
	pass

storage_types = {}

class StorageError(Exception):
	pass

class NoFileSystem(StorageError):
	pass


class Base(object):
	_data = None
	
	def __init__(self, **kwds):
		if not self._data:
			self._data = {}
		self._data['type'] = kwds.pop('type', 'base')
		self._data.update(kwds)


	def config(self):
		return self._walk_config(self._data)

	
	def __setattr__(self, name, value):
		data = self.__dict__ if 'name' in self.__dict__ else self.__dict__['_data']
		data[name] = value
	
	
	def __getattr__(self, name):
		if name in self.__dict__['_data']:
			return self.__dict__['_data'][name]
		raise AttributeError(name)
	
	
	def _walk_config(self, data=None):
		# todo: walk and extract config
		
		if hasattr(data, '__iter__'):
			
			ret = {}
			for key, value in self._data:
				if hasattr(value, '__iter__'):
					if isinstance(value, list):
						ret[key] = [self._walk_config(v) for v in value]
					else:
						ret[key] = self._walk_config(data)
				if isinstance(value, Base):
					ret[key] = value.config()
				else:
					ret[key] = value
			pass
			return ret
		return data


	def _genid(self, prefix=''):
		return '%s%s-%s' % (prefix, self.type, uuid.uuid4().hex[0:8])		


class Volume(Base):
	
	def ensure(self, mount=False, mkfs=False, **updates):
		self._ensure()
		if not self.id:
			self.id = self._genid('vol-')
		if mount:
			try:
				self.mount()
			except NoFileSystem:
				if mkfs:
					self.mkfs()
					self.mount()
				else:
					raise
		return self.config()
	
	def snapshot(self, description=None, tags=None):
		return self._snapshot(description, tags)


	def destroy(self, force=False):
		if self.device:
			self.detach(force)
		self._destroy(force)


	def detach(self, force=False):
		self.umount()
		self._detach(force)


	def mount(self):
		pass


	def umount(self):
		pass


	def mkfs(self):
		pass

	
	def _ensure(self):
		pass
	
	
	def _snapshot(self, description, tags):
		pass


	def _detach(self, force):
		pass

	
	def _destroy(self, force):
		pass
	
	
class Snapshot(Base):
	QUEUED = 'queued'
	IN_PROGRESS = 'in-progress'
	COMPLETED = 'completed'
	FAILED = 'failed'

	def __init__(self, **kwds):
		super(Snapshot, self).__init__(**kwds)
		if not self._data.get('id'):
			self._data['id'] = self._genid('snap-')
	
	def destroy(self):
		return self._destroy()
	
	
	def status(self):
		return self._status()
	
	
	def _destroy(self):
		pass
	
	def _status(self):
		pass
	
