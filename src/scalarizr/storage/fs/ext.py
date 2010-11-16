'''
Created on Nov 11, 2010

@author: marat
'''
from . import FileSystem
from scalarizr.storage import _system
import os
import subprocess

E2LABEL_PATH	= "/sbin/e2label"
RESIZE2FS_PATH	= "/sbin/resize2fs"
E2FSCK_PATH		= "/sbin/e2fsck"


class ExtFileSystem(FileSystem):

	umount_on_resize	= None
	_fsname				= None
	__max_label_length	= None

	def __init__(self):
		self.umount_on_resize = True
		self.__max_label_length = 16

	def resize(self, device, size=None, **options):
		if not os.path.exists(device):
			raise Exception("Device %s doesn't exist." % device)
		p = subprocess.Popen('%s -f %s' % (E2FSCK_PATH, device), shell=True)
		out, err = p.communicate()
		if p.returncode:
			raise Exception("Error occured during filesystem check on device '%s'.'\nOut:%s\nErr:%s" % (device, out,err))
		
		cmd = '%s %s' % (RESIZE2FS_PATH, device)
		error = "Error occured during filesystem resize on device '%s'" % device
		_system(cmd, error)

	def set_label(self, device, label):
		label	= label[:self.__max_label_length]
		
		cmd		= '%s %s %s' % (E2LABEL_PATH, device, label)
		error	= "Error while setting label for device '%s'" % device
		_system(cmd, error)
	
	def get_label(self, device):
		cmd		= '%s %s' % (E2LABEL_PATH, device)
		error	= "Error while getting label for device '%s'" % device
		out = _system(cmd, error)		
		return out.strip()
	
class Ext3FileSystem(ExtFileSystem):
	
	def __init__(self):
		self._fsname = 'ext3'
		ExtFileSystem.__init__(self)
		
class Ext4FileSystem(ExtFileSystem):
	
	def __init__(self):
		self._fsname = 'ext4'
		ExtFileSystem.__init__(self)

filesystems = dict(Ext3FileSystem=('ext3',), Ext4FileSystem=('ext4'))

