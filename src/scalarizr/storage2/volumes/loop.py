'''
Created on Jan 6, 2011

@author: marat
'''

import os
import sys
import time
import shutil
import random

from scalarizr import storage2
from scalarizr.storage2.volumes import base
from scalarizr.linux import coreutils


class LoopVolume(base.Volume):
	
	features = base.Volume.features.copy()
	features.update({
		'restore': True
	})

	default_config = base.Volume.default_config.copy()
	default_config.update({
		'file': None,   	
		# Filename for loop device
		'size': None,  		
		# Size in Gb or % of root device
		'zerofill': False	
		# Fill device with zero bytes. Takes more time, 
		# but greater GZip compression
	})
	
	
	def _ensure(self):
		if self.snap:
			try:
				filename = '%s.%s' % (self.snap['file'].split('.')[0], 
									self._uniq()) 
				shutil.copy(self.snap['file'], file_)
			except:
				msg = 'Failed to copy snapshot file %s: %s' % (
						self.snap['file'], sys.exc_info()[1])
				raise storage2.StorageError(msg)
			del self._config['snap']
			self.file = filename


		if self.device and os.path.exists(self.device):
			if coreutils.losetup_all()[self.device] != self.file:
				coreutils.losetup(self.device, detach=True)

		if not self.device or not os.path.exists(self.device):
			# Construct volume
			if (not self.size and \
				(not self.file or not os.path.exists(self.file))):
				msg = 'You must specify size of a new loop device '
						'or existing file'
				raise storage2.StorageError(msg)
			if not self.file:
				self.file = '/mnt/loopdev' + self._uniq()
			if not os.path.exists(self.file):
				if '%ROOT' in str(self.size).upper():
					try:
						pc = int(self.size.split('%')[0])
					except:
						msg = 'Incorrect size format: %s' % self.size
						raise storage2.StorageError(msg)
					stat = os.starvfs('/')
					total = stat.f_bsize * stat.f_blocks / 1048576
					size = total * pc / 100
					free = stat.f_bsize * stat.f_bfree / 1048576
					if size > free:
						msg = 'Expected loop size is greater then ' \
								'available free space on a root filesystem. ' \
								'Expected: %sMb / Free: %sMb' % (size, free)
						raise storage2.StorageError(msg)
				else:
					size = int(float(self.size) * 1024)
				dd_kwds = {'if': '/dev/zero', 'of': self.file, 'bs': '1M'}
				if self.zerofill:
					dd_kwds.update({'count': size})
				else:
					dd_kwds.update({'seek': size - 1, 'count': 1})
				coreutils.dd(**dd_kwds)
			if self.device:
				coreutils.losetup(self.device, self.file)
			else:
				coreutils.losetup(self.file, find=True)
				self.device = coreutils.losetup_all(flip=True)[self.file]
				
		
				
	def _snapshot(self, description, tags, **kwds):
		snapfile = '%s.snap.%s' % (self.file, self._uniq())
		shutil.copy(self.file, snapfile)
		return storage2.snapshot(type='loop', file=snapfile)


	def _detach(self, force, **kwds):
		if self.device:
			coreutils.losetup(self.device, detach=True)
		self.device = None

	
	def _destroy(self, force, **kwds):
		if force and self.file:
			os.remove(self.file)


	def _uniq(self):
		return repr(time.time())


class LoopSnapshot(base.Snapshot):
	pass


storage2.volume_types['loop'] = LoopVolume
storage2.snapshot_types['loop'] = LoopSnapshot

