
import logging
import re
import operator

from scalarizr import linux
import Queue
import sys
import threading

LOG = logging.getLogger(__name__)


volume_types = dict()
snapshot_types = dict()
filesystem_types = dict()


def volume(**kwds):
	type_ = kwds.get('type', 'base')
	if type_ not in volume_types:
		try:
			__import__('scalarizr.storage2.volumes.%s' % type_)
		except ImportError:
			pass
	try:
		cls = volume_types[type_]
	except KeyError:
		raise KeyError("Unknown volume type '%s'. "
						"Have you registered it in storage2.volume_types?" % type_)
	return cls(**kwds)


def snapshot(**kwds):
	type_ = kwds.get('type', 'base')
	if type_ not in snapshot_types:
		try:
			__import__('scalarizr.storage2.volumes.%s' % type_)
		except ImportError:
			pass
	try:
		cls = snapshot_types[type_]
	except KeyError:
		raise KeyError("Unknown snapshot type '%s'. "
					"Have you registered it in storage2.snapshot_types?" % type_)
	return cls(**kwds)


def filesystem(fstype=None):
	fstype = fstype or 'ext3'
	if not fstype in filesystem_types:
		try:
			__import__('scalarizr.storage2.filesystems.%s' % fstype)
		except ImportError:
			pass
	try:
		cls = filesystem_types[fstype]
	except KeyError:
		raise KeyError("Unknown filesystem type '%s'. "
					"Have you registered it in storage2.filesystem_types?" % fstype)
	return cls()


def concurrent_snapshot(volumes, description, tags=None, **kwds):
	'''
	Concurrently calls vol.snapshot() and 
	returns list of snapshot in correct order
	'''
	results = []
	def snapshot(index, volumes, description, tags=None, **kwds):
		try:
			snap = volume.snapshot(description, tags, **kwds)
			results.append((1, index, snap))
		except:
			exc_info = sys.exc_info()
			LOG.warn('Failed to create snapshot of %s(%s): %s', 
					vol.id, vol.type, exc_info[1], exc_info=exc_info)
			results.put((0, index, exc_info))

	threads = []
	index = 0
	for vol in volumes:
		sindex = str(index)
		description0 = description.replace('${index}', sindex)
		tags0 = tags and tags.copy() or {}
		for k, v in tags0.items():
			tags0[k] = v.replace('${index}', sindex)
		thread = threading.Thread(
					target=snapshot, 
					args=(index, vol, description0, tags0), 
					kwargs=kwds)
		thread.start()
		threads.append(thread)
		index += 1
	
	for thread in threads:
		thread.join()
		
	# sort results by index
	results = sorted(results, key=operator.itemgetter(1))
	if not all((r[0] for r in results)):
		# delete created snapshots to rollback
		for r in results:
			if not r[0]: continue
			snap = r[2]
			try:
				snap.destroy(force=True)
			except:
				exc_info = sys.exc_info()
				LOG.warn('Failed to delete snapshot %s(%s): %s', 
						snap.id, snap.type, exc_info[1], exc_info=exc_info)
		raise StorageError(
				'Failed to create one or more snapshots. '
				'Successfuly created snapshots were deleted to rollback. '
				'See log for detailed report about each failed snapshot')
	return (r[2] for r in results)
		
			
class StorageError(linux.LinuxError):
	pass

class OperationError(StorageError):
	pass

RHEL_DEVICE_ORDERING_BUG = False
if linux.os.redhat_family:
	# Check that system is affected by devices ordering bug
	# https://bugzilla.redhat.com/show_bug.cgi?id=729340
	from scalarizr.linux import mount
	try:
		entry = mount.mounts()['/dev/xvde']
		RHEL_DEVICE_ORDERING_BUG = entry.mpoint == '/'
	except KeyError:
		pass 
