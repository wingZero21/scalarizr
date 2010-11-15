'''
Created on Nov 11, 2010

@author: marat
'''

import re, os
from . import FileSystem
from scalarizr.storage import _system
from scalarizr.util import system
from . import MOUNT_PATH, MKFS_PATH

JFS_TUNE_PATH	= "/sbin/jfs_tune"

class JfsFileSystem(FileSystem):
		
	_fsname 		 = None
	__label_re 		 = None
	umount_on_resize = None
	
	def __init__(self):
		self._fsname    = 'jfs'
		self.__label_re  = re.compile("volume\s+label:\s+'(?P<label>.*)'", re.IGNORECASE)
		umount_on_resize = False
		
	def mkfs(self, device, **options):
		if not os.path.exists(device):
			raise Exception("Device %s doesn't exist." % device)
		
		cmd = '%s -t %s -q %s' % (MKFS_PATH, self._fsname, device)
		error = "Error occured during filesystem creation on device '%s'" % device
		_system(cmd, error)

		
	def set_label(self, device, label):
		out,err,rcode = system('%s -L "%s" %s' % (JFS_TUNE_PATH, label, device))
		if rcode or err:
			raise Exception("Error while setting label for device '%s'.\
							 Return code: %s.\nSTDERR: %s " % (device, rcode, err))
	
	def get_label(self, device):
		cmd = '%s -l %s' % (JFS_TUNE_PATH, device)
		error = "Error while getting info for device '%s'" % device
		out = _system(cmd, error)
		
		res = re.search(self.__label_re, out)
		if not res:
			raise Exception("Volume label wasn't found in jfs_tune's output")
		return res.group('label')
	
	def resize(self, device, size=None, **options):
		if not os.path.exists(device):
			raise Exception("Device %s doesn't exist." % device)
		
		res = re.search('%s\s+on\s+(?P<mpoint>.+)\s+type' % device, system(MOUNT_PATH)[0])
		if not res:
			raise Exception('Mount device before resizing jfs file system')
		
		mpoint = res.group('mpoint')
		cmd = '%s -o remount,resize %s' % (MOUNT_PATH, mpoint)
		error = 'Error occured during filesystem remount. Mpoint: %s' % mpoint
		_system(cmd, error)
		
		
		
		

filesystems = dict(JfsFileSystem=('jfs', 'jfs2'))
