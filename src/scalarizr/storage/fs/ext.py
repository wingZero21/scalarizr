'''
Created on Nov 11, 2010

@author: marat
'''
from . import FileSystem
import os
from scalarizr.util import system

E2LABEL_PATH	= "/sbin/e2label"
MKFS_PATH		= "/sbin/mkfs"
RESIZE2FS_PATH	= "/sbin/resize2fs"
E2FSCK_PATH		= "/sbin/e2fsck"


class ExtFileSystem(FileSystem):
	umount_on_resize	= None
	__fsname			= None
	__max_label_length	= 16
	
	def __init__(self):
		self.__fsname = 'ext3' # ext2, 3, 4, 
		self.umount_on_resize = True
		
	def _system(self, cmd, error):
		out,rcode = system(cmd + ' 2>&1')[0::2]
		if rcode:
			raise Exception(error+"\n" + "Return code: %s. Error: %s" % (rcode, out))
		return out

	def mkfs(self, device, **options):
		if not os.path.exists(device):
			raise Exception("Device %s doesn't exist." % device)
		
		cmd = '%s -t %s -F %s' % (MKFS_PATH, self.__fsname, device)
		error = "Error occured during filesystem creation on device '%s'" % device
		self._system(cmd, error)

	def resize(self, device, size=None, **options):
		if not os.path.exists(device):
			raise Exception("Device %s doesn't exist." % device)
		
		cmd = '%s -f %s' % (E2FSCK_PATH, device)
		error = "Error occured during filesystem check on device '%s'" % device
		self._system(cmd, error)
		
		cmd = '%s %s' % (RESIZE2FS_PATH, device)
		error = "Error occured during filesystem resize on device '%s'" % device
		self._system(cmd, error)

	def _set_label(self, device, label):
		label	= label[:self.__max_label_length]
		
		cmd		= '%s %s %s' % (E2LABEL_PATH, device, label)
		error	= "Error while setting label for device '%s'" % device
		self._system(cmd, error)
	
	def _get_label(self, device):
		cmd		= '%s %s' % (E2LABEL_PATH, device)
		error	= "Error while getting label for device '%s'" % device
		out = self._system(cmd, error)
		
		return out.strip()

filesystems = dict(ExtFileSystem=('ext2', 'ext3', 'ext4'))

