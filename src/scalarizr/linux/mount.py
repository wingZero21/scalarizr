'''
Created on Aug 27, 2012

@author: marat
'''

import re
import collections

from scalarizr import linux

class NoFileSystem(linux.LinuxError):
	pass

MOUNT_EXEC = linux.which('mount')
UMOUNT_EXEC = linux.which('umount')

_MountEntry = collections.namedtuple('_MountEntry', 'device mpoint fstype options dump fsck_order')

class mounts(object):
	'''
	Utility for /etc/fstab
	@see http://www.debianhelp.co.uk/fstab.htm
	'''
	filename = None	
	_entries = None
	_entry_re = None

	
	def __init__(self, filename='/proc/mounts'):
		self.filename = filename
		self._entry_re = re.compile("\\s+")
		self.reload()


	def reload(self):
		self._entries = []
		for line in open(self.filename):
			if line[0] != "#":
				m = filter(None, self._entry_re.split(line))
				if m:
					self._entries.append(_MountEntry(*m))

	
	def __getitem__(self, device_or_mpoint):
		self.reload()
		for entry in self._entries:
			if entry.device == device_or_mpoint or entry.mpoint == device_or_mpoint:
				return entry
		raise KeyError(device_or_mpoint)


	def __contains__(self, device_or_mpoint):
		self.reload()
		return any([entry.device == device_or_mpoint or 
				entry.mpoint == device_or_mpoint 
				for entry in self._entries])
		

def mount(device, mpoint, **long_kwds):
	args = [MOUNT_EXEC, device, mpoint] + linux.build_cmd_args(long=long_kwds)
	try:
		linux.system(args, error_text='Cannot mount %s -> %s' % (device, mpoint))
	except linux.LinuxError, e:
		if 'you must specify the filesystem type' in e.err:
			raise NoFileSystem(device)
		raise


def umount(device_or_mpoint, **long_kwds):
	args = [UMOUNT_EXEC, '-f' , device_or_mpoint] + linux.build_cmd_args(long=long_kwds)
	try:
		linux.system(args, error_text='Cannot umount %s' % device_or_mpoint)
	except linux.LinuxError, e:
		if not 'not mounted' in e.err:
			raise
	
