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
from scalarizr.storage2.util import loop


class LoopVolume(base.Volume):
	
	features = base.Volume.features.copy()
	features['restore'] = True
	
	
	def __init__(self, **kwds):
		self.default_config.update({
			'type': 'loop',
			'file': None,   	
			# Filename for loop device
			
			'size': None,  		
			# Size in MB or % of root device
			
			'zerofill': False	
			# Fill device with zero bytes. Takes more time, 
			# but greater GZip compression
		})
		super(LoopVolume, self).__init__(**kwds)
	
	def _ensure(self):
		size = self.size.lower()

		if 'snap' in self._config:
			file = self.snap['file']
			try:
				base = file.split('.')[0]
				new_file = base + time.strftime('.%d-%m_%H:%M:%S_') + str(random.randint(1, 1000))
				shutil.copy(self.snapshot['file'], new_file)
			except:
				raise storage2.StorageError("Can't copy snapshot file %s. Error: %s" % (file, sys.exc_info()[1]))
	
			del self._config['snap']
			self.file = new_file
			

		if not (self.device and self.file and loop.listloop().get(self.device) == self.file):
			# Construct volume
			if (not self.size and (not self.file or not os.path.exists(self.file))):
				raise storage2.StorageError('You must specify size of new loop device or existing file.')
			
			if not self.file:
				self.file = '/mnt/loopdev%s' % repr(time.time())
			if not os.path.exists(self.file):			
				try:
					size = int(float(self.size) * 1024)
				except ValueError:
					if isinstance(self.size, basestring) and '%root' in self.size:
						# Getting size in percents
						try:
							size_pct = int(size.replace('%root', ''))
						except:
							raise storage2.StorageError('Incorrect size format: %s' % self.size)
						# Retrieveing root device size and usage 
						# FIXME: use os.statvfs('/')
						root_size, used_pct = (storage2.system(('df', '-P', '-B', '1024', '/'))[0].splitlines()[1].split()[x] for x in (1,4))
						root_size = int(root_size) / 1024
						used_pct = int(used_pct[:-1])
						
						if size_pct > (100 - used_pct):
							raise storage2.StorageError('No enough free space left on device.')
						# Calculating loop device size in Mb
						size = (root_size / 100) * size_pct
					else:
						raise storage2.StorageError('Incorrect size format: %s' % self.size)
			
			existed = filter(lambda x: x[1] == file, loop.listloop().iteritems())
			if existed:
				self.device = existed[0][0]
			else:
				self.device = loop.mkloop(file, size=size, quick=not self.zerofill)
		
				
	def _snapshot(self, description, tags, **kwds):
		backup_filename = self.file + '.%s.bak' % time.strftime('%d-%m_%H:%M:%S')
		shutil.copy(self.file, backup_filename)
		return storage2.snapshot(type='loop', file=backup_filename)


	def _detach(self, force, **kwds):
		if self.device:
			loop.rmloop(self.device)
		self.device = None

	
	def _destroy(self, force, **kwds):
		if force and self.file:
			os.remove(self.file)


class LoopSnapshot(base.Snapshot):
	pass

storage2.volume_types['loop'] = LoopVolume
storage2.snapshot_types['loop'] = LoopSnapshot

