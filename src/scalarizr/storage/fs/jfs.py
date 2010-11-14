'''
Created on Nov 11, 2010

@author: marat
'''

import re, os
from . import FileSystem
from scalarizr.util import system
from . import MOUNT_PATH

JFS_TUNE_PATH	= "/sbin/jfs_tune"

class JfsFileSystem(FileSystem):
		
	_fsname 		 = None
	__label_re 		 = None
	umount_on_resize = None
	
	def __init__(self):
		self._fsname    = 'jfs'
		self.__label_re  = re.compile("volume\s+label:\s+'(?P<label>).*'", re.IGNORECASE)
		umount_on_resize = False
		
	def set_label(self, device, label):
		out,err,rcode = system('%s -L "%s" %s' % (JFS_TUNE_PATH, label, device))
		if rcode or err:
			raise Exception("Error while setting label for device '%s'.\
							 Return code: %s.\nSTDERR: %s " % (device, rcode, err))
	
	def get_label(self, device):
		out,err,rcode = system('%s -l %s' % (JFS_TUNE_PATH, device))
		if rcode or err:
			raise Exception("Error while getting info for device '%s'.\
							 Return code: %s.\nSTDERR: %s " % (device, rcode, err))
		res = re.search(self.__label_re, out)
		if not res:
			raise Exception("Volume label wasn't found in jfs_tune's output")
		return res.group('label')
	
	def resize(self, device, size=None, **options):
		if not os.path.exists(device):
			raise Exception("Device %s doesn't exist." % device)
		
		res = re.search('%s\s+on\s+(?P<mpoint>.+)\s+type' % device, system(MOUNT_PATH)[0])
		if not res:
			raise Exception('Mount device before resizing xfs file system')	
		
		
		

filesystems = dict(JfsFileSystem=('jfs', 'jfs2'))
