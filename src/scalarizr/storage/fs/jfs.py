'''
Created on Nov 11, 2010

@author: marat
'''

import re
from . import FileSystem
from scalarizr.util import system

JFS_TUNE_PATH	= "/sbin/jfs_tune"

class JfsFileSystem(FileSystem):

	def __init__(self):
		self._label_re = re.compile("volume\s+label:\s+'(?P<label>).*'", re.IGNORECASE)
		
	def _set_label(self, device, label):
		out,err,rcode = system('%s -L "%s" %s' % (JFS_TUNE_PATH, label, device))
		if rcode or err:
			raise Exception("Error while setting label for device '%s'.\
							 Return code: %s.\nSTDERR: %s " % (device, rcode, err))
	
	def _get_label(self, device):
		out,err,rcode = system('%s -l %s' % (JFS_TUNE_PATH, device))
		if rcode or err:
			raise Exception("Error while getting info for device '%s'.\
							 Return code: %s.\nSTDERR: %s " % (device, rcode, err))
		res = re.search(self._label_re, out)
		if not res:
			raise Exception("Volume label wasn't found in jfs_tune's output")
		return res.group('label')
	
	def resize(self, device, size=None, **options):
		pass
filesystems = dict(JfsFileSystem=('jfs', 'jfs2'))
