'''
Created on Nov 11, 2010

@author: marat
'''

from . import FileSystem
from scalarizr.storage import _system
import re, os
from scalarizr.util import system
from . import MOUNT_PATH

XFS_ADMIN_PATH  = "/usr/sbin/xfs_admin"
XFS_GROWFS_PATH = "/usr/sbin/xfs_growfs"


class XfsFileSystem(FileSystem):
	_fsname = None
	umount_on_resize	= None
	
	def __init__(self):
		self._fsname    = 'xfs'
		self.__label_re  = re.compile('label\s+=\s+"(?P<label>.*)"', re.IGNORECASE)
		umount_on_resize = False

	def set_label(self, device, label):
		cmd   = '%s -L "%s" %s' % (XFS_ADMIN_PATH, label, device)
		error = "Error while setting label for device '%s'" % device 
		_system(cmd, error)

	def get_label(self, device):
		cmd   = '%s -l %s' % (XFS_ADMIN_PATH, device)
		error = "Error while getting label for device '%s'" % device 
		res   = re.search(self.__label_re, _system(cmd, error))
		if not res:
			raise Exception("Volume label wasn't found in xfs_admin's output")
		return res.group('label')
	
	def resize(self, device, size=None, **options):
		if not os.path.exists(device):
			raise Exception("Device %s doesn't exist." % device)
		
		res = re.search('%s\s+on\s+(?P<mpoint>.+)\s+type' % device, system(MOUNT_PATH)[0])
		if not res:
			raise Exception('Mount device before resizing jfs file system')
		cmd = "%s %s" % (XFS_GROWFS_PATH, res.group('mpoint'))
		error = "Error occured during file system resize operation"
		_system(cmd, error)

filesystems = dict(XfsFileSystem=('xfs',))
