'''
Created on Sep 12, 2011

@author: marat
'''

from scalarizr.bus import bus
from scalarizr.storage import Storage, Volume, VolumeProvider, StorageError, devname_not_empty, \
	VolumeConfig, Snapshot


class CSConfig(VolumeConfig):
	type = 'cloudstack'
	snapshot_id = None
	zone_id = None
	disk_offering_id = None
	size = None

class CSVolume(Volume, CSConfig):
	pass


class CSSnapshot(Snapshot, CSConfig):
	_ignores = ('snapshot_id',)	


class CSVolumeProvider(VolumeProvider):
	type = CSConfig.type
	vol_class = CSVolume
	snap_class = CSConfig
	snapshot_state_map = {
		'Creating' : Snapshot.CREATING,
		'BackingUp' : Snapshot.CREATED,
		'BackedUp' : Snapshot.COMPLETED,
		'error' : Snapshot.FAILED
	}


	def _create(self, **kwargs):
		'''
		@param id: EBS volume id		
		@param device: Device name
		@param size: Size in GB
		@param zone_id: Availability zone
		@param disk_offering_id: Disk offering ID
		@param snapshot_id: Snapshot id
		'''
		ebs_vol = None
		pl = bus.platform
		conn = self._new_ec2_conn()
		
		if conn:
			# Find free devname			
			device = kwargs.get('device')
			if device:
				device = ebstool.get_ebs_devname(device)
			used_letters = set(row['device'][-1] 
						for row in Storage.volume_table() 
						if row['state'] == 'attached' or ( \
							pl.get_instance_type() == 't1.micro' and row['state'] == 'detached'
						))
			avail_letters = tuple(set(self.all_letters) - used_letters)
			if not device or not (device[-1] in avail_letters) or os.path.exists(device):
				letter = firstmatched(lambda l: not os.path.exists('/dev/sd%s' % l), avail_letters)
				if letter:
					device = '/dev/sd%s' % letter
				else:
					raise StorageError('No free letters for block device name remains')
			
			self._logger.debug('storage._create kwds: %s', kwargs)
			volume_id = kwargs.get('id')
			# xxx: hotfix
			if volume_id and volume_id.startswith('snap-'):
				volume_id = None
			snap_id = kwargs.get('snapshot_id')
			ebs_vol = None
			delete_snap = False
			volume_attached = False			
			try:
				if volume_id:
					self._logger.debug('EBS has been already created')
					try:
						ebs_vol = conn.get_all_volumes([volume_id])[0]
					except IndexError:
						raise StorageError("EBS volume '%s' doesn't exist." % volume_id)
					
					if ebs_vol.zone != pl.get_avail_zone():
						self._logger.warn('EBS volume %s is in the different availability zone (%s). ' + 
										'Snapshoting it and create a new EBS volume in %s', 
										ebs_vol.id, ebs_vol.zone, pl.get_avail_zone())
						volume_id = None
						delete_snap = True
						snap_id = ebstool.create_snapshot(conn, ebs_vol, logger=self._logger, wait_completion=True).id
					else:
						snap_id = None
						
				if snap_id or not volume_id:
					self._logger.debug('Creating new EBS')
					kwargs['avail_zone'] = pl.get_avail_zone()
					ebs_vol = ebstool.create_volume(conn, kwargs.get('size'), kwargs.get('avail_zone'), 
						snap_id, logger=self._logger, tags=kwargs.get('tags'))

			
				if 'available' != ebs_vol.volume_state():
					if ebs_vol.attachment_state() == 'attaching':
						wait_until(lambda: ebs_vol.update() and ebs_vol.attachment_state() != 'attaching', timeout=600, 
								error_text='EBS volume %s hangs in attaching state' % ebs_vol.id)
					
					if ebs_vol.attach_data.instance_id != pl.get_instance_id():
						self._logger.debug('EBS is attached to another instance')
						self._logger.warning("EBS volume %s is not available. Detaching it from %s", 
											ebs_vol.id, ebs_vol.attach_data.instance_id)						
						ebstool.detach_volume(conn, ebs_vol, force=True, logger=self._logger)
					else:
						self._logger.debug('EBS is attached to this instance')
						device = ebstool.real_devname(ebs_vol.attach_data.device)
						wait_until(lambda: os.path.exists(device), sleep=1, timeout=300, 
								error_text="Device %s wasn't available in a reasonable time" % device)						
						volume_attached = True

				
				if not volume_attached:
					self._logger.debug('Attaching EBS to this instance')
					device = ebstool.attach_volume(conn, ebs_vol, pl.get_instance_id(), device, 
						to_me=True, logger=self._logger)[1]
				
			except (Exception, BaseException), e:
				self._logger.debug('Caught exception')
				if ebs_vol:
					self._logger.debug('Detaching EBS')
					if (ebs_vol.update() and ebs_vol.attachment_state() != 'available'):
						ebstool.detach_volume(conn, ebs_vol, force=True, logger=self._logger)
							
					#if not volume_id:
					#	ebs_vol.delete()
						
				raise StorageError('EBS volume construction failed: %s' % str(e))
			
			finally:
				if delete_snap and snap_id:
					conn.delete_snapshot(snap_id)
					
			
			kwargs['device'] = device
			kwargs['id'] = ebs_vol.id
			
		elif kwargs.get('device'):
			kwargs['device'] = ebstool.get_system_devname(kwargs['device'])
			
		return super(EbsVolumeProvider, self).create(**kwargs)

	create = _create
	
	def create_from_snapshot(self, **kwargs):
		'''
		@param size: Size in GB
		@param avail_zone: Availability zone
		@param id: Snapshot id
		'''
		return self._create(**kwargs)

	def create_snapshot(self, vol, snap, **kwargs):
		conn = self._new_ec2_conn()
		ebs_snap = ebstool.create_snapshot(conn, vol.id, snap.description, tags=kwargs.get('tags'))
		snap.id = ebs_snap.id
		return snap

	def get_snapshot_state(self, snap):
		conn = self._new_ec2_conn()
		state = conn.get_all_snapshots((snap.id,))[0].status
		return self.snapshot_state_map[state]

	def blank_config(self, cnf):
		cnf.pop('snapshot_id', None)

	def destroy(self, vol, force=False, **kwargs):
		'''
		@type vol: EbsVolume
		'''
		super(EbsVolumeProvider, self).destroy(vol)
		conn = self._new_ec2_conn()
		if conn:
			ebstool.detach_volume(conn, vol.id, self._logger)
			ebstool.delete_volume(conn, vol.id, self._logger)
		vol.device = None							
	
	def destroy_snapshot(self, snap):
		conn = self._new_ec2_conn()
		if conn:
			self._logger.debug('Deleting EBS snapshot %s', snap.id)
			conn.delete_snapshot(snap.id)
	
	@devname_not_empty		
	def detach(self, vol, force=False):
		super(EbsVolumeProvider, self).detach(vol)
		
		try:
			pl = bus.platform
			conn = pl.new_ec2_conn()
			vol.detached = True			
		except AttributeError:
			pass
		else:
			ebstool.detach_volume(conn, vol.id, self._logger)
		finally:
			vol.device = None



Storage.explore_provider(CSVolumeProvider)
