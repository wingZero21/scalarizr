'''
Created on Jan 6, 2011

@author: marat
'''

from . import VolumeConfig, Volume, Snapshot, VolumeProvider, Storage, StorageError, system
from .util.loop import mkloop, rmloop

import os
import time
import shutil 
from scalarizr.storage.util.loop import listloop

class LoopConfig(VolumeConfig):
	type = 'loop'
	file = None
	size = None
	zerofill = None

class LoopVolume(Volume, LoopConfig):
	pass
		
class LoopSnapshot(Snapshot, LoopConfig):
	pass

	
class LoopVolumeProvider(VolumeProvider):
	type = 'loop'
	vol_class = LoopVolume
	snap_class = LoopSnapshot
	
	def create(self, **kwargs):
		
		'''
		@param file: Filename for loop device
		@type file: basestring
		
		@param size: Size in MB or % of root device
		@type size: int | str
		
		@param zerofill: Fill device with zero bytes. Takes more time, but greater GZip compression
		@type zerofill: bool
		'''
		size = kwargs.get('size')
		file = kwargs.get('file')
		device = kwargs.get('device')
		
		if not (device and file and listloop().get(device) == file):
			# Construct volume
			if (not size and (not file or not os.path.exists(file))):
				raise StorageError('You must specify size of new loop device or existing file.')
			
			if not file:
				file = '/mnt/loopdev' + time.strftime('%Y%m%d%H%M%S')
			if not os.path.exists(file):			
				try:
					size = int(size)
				except ValueError:
					if isinstance(size, basestring) and '%root' in size.lower():
						# Getting size in percents
						try:
							size_pct = int(size.lower().replace('%root', ''))
						except:
							raise StorageError('Incorrect size format: %s' % size)
						# Retrieveing root device size and usage 
						root_size, used_pct = (system(('df', '-B', '1024', '/'))[0].splitlines()[1].split()[x] for x in (1,4))
						root_size = int(root_size) / 1024
						used_pct = int(used_pct[:-1])
						
						if size_pct > (100 - used_pct):
							raise StorageError('No enough free space left on device.')
						# Calculating loop device size in Mb
						size = (root_size / 100) * size_pct
					else:
						raise StorageError('Incorrect size format: %s' % size)
			
			kwargs['file']	= file
			kwargs['device'] = mkloop(file, device=device, size=size, quick=not kwargs.get('zerofill'))
			
		return super(LoopVolumeProvider, self).create(**kwargs)
	
	def create_snapshot(self, vol, snap):
		backup_filename = vol.file + '.%s.bak' % time.strftime('%d-%m-%Y_%H:%M')
		shutil.copy(vol.file, backup_filename)
		snap.file = backup_filename
		return snap
	
	def detach(self, vol, force=False):
		super(LoopVolumeProvider, self).detach(vol, force)
		rmloop(vol.devname)
		vol.device = None
		vol.detached = True
		return vol.config()

	def destroy(self, vol, force=False, **kwargs):		
		super(LoopVolumeProvider, self).destroy(vol, force, **kwargs)
		rmloop(vol.devname)
		vol.device = None
		
Storage.explore_provider(LoopVolumeProvider)
