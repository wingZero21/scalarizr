'''
Created on Jan 6, 2011

@author: marat
'''

import os
import sys
import copy
import time
import logging
import binascii


from . import VolumeConfig, Volume, Snapshot, VolumeProvider, Storage, \
		StorageError, devname_not_empty
from .util import mdadm, fsfreeze



from scalarizr.libs.pubsub import Observable


logger = logging.getLogger(__name__)


class RaidConfig(VolumeConfig):
	type	= 'raid'
	level	= None
	disks	= None

class RaidVolume(Volume, RaidConfig):

	def __init__(self, device=None, mpoint=None, fstype=None, type=None, *args, **kwargs):
		super(RaidVolume, self).__init__(device, mpoint, fstype, type, *args, **kwargs)
		self._events = Observable()
		self._events.define_events('active', 'inactive')

	def on(self, *args, **kwargs):
		self._events.on(*args, **kwargs)

	def un(self, event, listener):
		self._events.un(event, listener)

	def fire(self, event, *args, **kwargs):
		self._events.fire(event, *args, **kwargs)

	
class RaidSnapshot(Snapshot, RaidConfig):
	pass


class RaidVolumeProvider(VolumeProvider):
	type = 'raid'
	vol_class = RaidVolume
	snap_class = RaidSnapshot

	_mdadm = None
	_logger = None
	
	def __init__(self):
		self._mdadm = mdadm.Mdadm()
		self._logger = logging.getLogger(__name__)

	
	def create(self, **kwargs):
		'''
		@param disks: Raid disks
		@type disks: list(Volume)
		
		@param level: Raid level 0, 1, 5, 10 - are valid values
		@type level: int
		'''
		kwargs['device'] = self._mdadm.create(list(vol.devname for vol in kwargs['disks']), kwargs['level'])
		volume = super(RaidVolumeProvider, self).create(**kwargs)
		return volume
	
	def create_from_snapshot(self, **kwargs):
		'''
		@param level: Raid level 0, 1, 5 - are valid values
		@param disks: Volumes
		'''

		level = kwargs['level']

		if int(level) in (1, 10):
			device	= self._mdadm.create([vol.devname for vol in kwargs['disks']], level)
		else:
			device	= self._mdadm.assemble([vol.devname for vol in kwargs['disks']])

		return RaidVolume(device, disks	= kwargs['disks'], level = kwargs['level'])

	
	@devname_not_empty
	def create_snapshot(self, vol, snap, **kwargs):
		fsfreeze.freeze(vol.mpoint)

		try:
			# Creating RAID members snapshots
			snap.level		= vol.level
			snap.disks		= []

			for i, _vol in enumerate(vol.disks):
				description = 'RAID%s disk #%d - %s' % (vol.level, i, snap.description)

				if int(vol.level) in (1, 10) and (i % 2):
					last_copy = copy.copy(snap.disks[i-1])
					last_copy.description = description
					snap.disks.append(last_copy)
					continue

				pvd = Storage.lookup_provider(_vol.type)
				_snap = pvd.snapshot_factory(description)
				snap.disks.append(pvd.create_snapshot(_vol, _snap, tags=kwargs.get('tags')))

		except:
			e, t = sys.exc_info()[1:]
			raise StorageError, "Error occured during snapshot creation: '%s'" % e, t

		finally:
			fsfreeze.unfreeze(vol.mpoint)

		return snap


	def destroy(self, vol, force=False, **kwargs):
		super(RaidVolumeProvider, self).destroy(vol, force, **kwargs)
		
		remove_disks=kwargs.get('remove_disks') 
		if not vol.detached:

			self._mdadm.delete(vol.device)
		if remove_disks:
			if getattr(vol.disks, '__iter__', False):
				for disk in vol.disks:
					disk.destroy(force=force)


	@devname_not_empty			
	def detach(self, vol, force=False):
		self._logger.debug('Detaching volume %s' % vol.devname)
		super(RaidVolumeProvider, self).detach(vol, force)
		self._mdadm.delete(vol.device, zero_superblock=False)
		for disk in vol.disks:
			disk.detach(force)
		ret = vol.config()
		vol.detached = True
		return ret


Storage.explore_provider(RaidVolumeProvider)
