
import sys

from scalarizr import storage2
from scalarizr.libs import pubsub, cdo


class Error(Exception):
	pass


backup_types = {}
restore_types = {}


def backup(*args, **kwds):
	if args:
		if isinstance(args[0], dict):
			return backup(**args[0])
		else:
			return args[0]
	type_ = kwds.get('type', 'base')
	try:
		cls = backup_types[type_]
	except KeyError:
		msg = "Unknown backup type '%s'. "
		"Have you registered it in "
		"scalarizr.services.backup.backup_types?" % type_
		raise Error(msg)
	return cls(**kwds)


def restore(*args, **kwds):
	if args:
		if isinstance(args[0], dict):
			return restore(**args[0])
		else:
			return args[0]
	type_ = kwds.get('type', 'base')
	try:
		cls = restore_types[type_]
	except KeyError:
		msg = "Unknown restore type '%s'. "
		"Have you registered it in " 
		"scalarizr.services.backup.restore_types?" % type_
		raise Error(msg)
	return cls(**kwds)




	

class Task(pubsub.Observable, cdo.ConfigDriven):

	def __init__(self, **kwds):
		cdo.ConfigDriven.__init__(self, **kwds)
		pubsub.Observable.__init__(self, 
			'start',    # When job is started
			'complete', # When job is finished with success
			'error'     # When job is finished with error
		)
		self.__running = False
		self.__result  = None


	def kill(self):
		if self.__running:
			self._kill()
			self._cleanup()


	def _kill(self):
		pass


	def _cleanup(self):
		pass
	

	def run(self):
		if self.__running:
			raise Error('Another operation is running')
		try:
			self.__running = True
			self.fire('start')
			self.__result = self._run()
			self.fire('complete', self.__result)
			return self.__result
		except:
			exc_info = sys.exc_info()
			self.fire('error', exc_info)
			self._cleanup()
			raise exc_info[0], exc_info[1], exc_info[2]
		finally:
			self.__running = False


	def _run(self):
		pass


	@property
	def running(self):
		return self.__running

	def result(self):
		return self.__result


class Backup(Task):
	features = {
		'start_slave': True
	}

	default_config = {
		'type': 'base',
		'description': None,
		'tags': None
	}


class Restore(Task):

	features = {
		'master_binlog_reset': False
	}
	'''
	When 'master_binlog_reset' = False, 
	rolling this restore on Master causes replication binary log reset. 
	Slaves should start from the binary log head. Detecting the first 
	position in binary log is implementation dependent and Master is 
	responsible for this.
	'''


	default_config = {
		'type': 'base'
	}

backup_types['base'] = Backup
restore_types['base'] = Restore


class SnapBackup(Backup):
	default_config = Backup.default_config.copy()
	default_config.update({
		'volume': None
	})

	def __init__(self, **kwds):
		super(SnapBackup, self).__init__(**kwds)
		self.define_events(
			# Fires when all disk I/O activity should be freezed 
			'freeze'   
		)

	def _run(self):
		self.volume = storage2.volume(self.volume)
		state = {}
		self.fire('freeze', self.volume, state)
		snap = self.volume.snapshot(self.description, tags=self.tags)
		return restore(
				type=self.type, 
				snapshot=snap,
				**state)


class SnapRestore(Restore):
	default_config = Restore.default_config.copy()
	default_config.update({
		'snapshot': None,
		'volume': None
	})


	def _run(self):
		self.snapshot = storage2.snapshot(self.snapshot)
		if self.volume:
			self.volume = storage2.volume(self.volume)
			self.volume.snap = self.snapshot
			self.volume.ensure()
		else:
			self.volume = self.snapshot.restore()
		return self.volume
		

backup_types['snap'] = SnapBackup
restore_types['snap'] = SnapRestore



