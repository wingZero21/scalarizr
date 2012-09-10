
import sys

from scalarizr import storage2
from scalarizr.libs import pubsub


class Error(Exception):
	pass


backup_types = {}
restore_types = {}


def backup(*args, **kwds):
	# backup factory
	pass

def restore(*args, **kwds):
	# backup factory
	pass


class ConfigMixin(object):
	default_config = {}

	_config = None
	initial_config = None


	def __init__(self, **kwds):
		if not self._config:
			self._config = self.default_config.copy()
		self._config.update(kwds)
		self.initial_config = self._config.copy()		


	def config(self):
		return self._config.copy()


	def __setattr__(self, name, value):
		data = self.__dict__ if name in dir(self) else self.__dict__['_config']
		data[name] = value
	
	
	def __getattr__(self, name):
		if name in self.__dict__['_config']:
			return self.__dict__['_config'][name]
		raise AttributeError(name)
	
	
	def __hasattr__(self, name):
		return name in self.__dict__['_config']

	

class Task(pubsub.Observable, ConfigMixin):


	def __init__(self, **kwds):
		ConfigMixin.__init__(self, **kwds)
		self.define_events(
			'start',    # When job is started
			'complete', # When job is finished with success
			'error'     # When job is finished with error
		)
		self.__running = False


	def kill(self):
		if self.__running:
			self._kill()


	def _kill(self):
		pass


	def run(self):
		if self.__running:
			raise Error('Another operation is running')
		try:
			self.__running = True
			self.fire('start')
			result = self._run()
			self.fire('complete', result)
		except:
			self.fire('error', sys.exc_info())
			raise
		finally:
			self.__running = False


	def _run(self):
		pass


	@property
	def running(self):
		return self.__running



class Backup(Task):
	features = {
		'boot_slave': True
	}

	default_config = {
		'type': 'base',
		'description': None,
		'tags': None
	}


class Restore(Task):

	default_config = {
		'type': 'base'
	}

backup_types['base'] = Backup
restore_types['base'] = Restore


class SnapBackup(backup.Backup):
	default_config = backup.Backup.default_config.copy()
	default_config.update({
		'volume': None
	})

	def __init__(self, **kwds):
		super(SnapBackup, self).__init__(**kwds)
		self.define_events(
			# Fires when all service disk I/O activity should be freezed 
			'freeze_service'   
		)

	def _run(self):
		vol = storage2.volume(self.volume)
		service_state = self.fire('freeze_service', vol)
		snap = self.volume.snapshot(self.description, tags=self.tags)
		return backup.restore(
				type='snap', 
				snapshot=snap.config(),
				service_state=service_state)


class SnapRestore(backup.Restore):
	default_config = backup.Restore.default_config.copy()
	default_config.update({
		'snapshot': None,
		'service_state': None
	})


	def _run(self):
		snap = storage2.snapshot(self.snapshot)
		vol = snap.restore()
		return vol.config()
		

backup_types['snap'] = SnapBackup
restore_types['snap'] = SnapRestore



