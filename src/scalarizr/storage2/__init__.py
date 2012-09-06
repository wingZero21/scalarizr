
import logging
import re

from scalarizr import linux

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

		
class StorageError(linux.LinuxError):
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
