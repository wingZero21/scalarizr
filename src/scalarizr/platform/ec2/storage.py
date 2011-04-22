'''
Created on Nov 24, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.bus import bus
from scalarizr.storage import Storage, Volume, VolumeProvider, StorageError, devname_not_empty, \
	VolumeConfig, Snapshot
from scalarizr.storage.transfer import TransferProvider, TransferError
from . import ebstool

import os
import sys
import logging
import urlparse
import string

from boto.s3.key import Key
from boto.exception import BotoServerError, S3ResponseError
from scalarizr.util import firstmatched


class EbsConfig(VolumeConfig):
	type = 'ebs'
	snapshot_id = None
	avail_zone = None
	size = None

class EbsVolume(Volume, EbsConfig):
	avail_zone	= None
	size		= None

class EbsSnapshot(Snapshot, EbsConfig):
	_ignores = ('snapshot_id',)	
		

class EbsVolumeProvider(VolumeProvider):
	type = 'ebs'
	vol_class = EbsVolume
	snap_class = EbsSnapshot
	all_letters = tuple(string.ascii_lowercase[14:])
	
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
	
	def _create(self, **kwargs):
		'''
		@param id: EBS volume id		
		@param device: Device name
		@param size: Size in GB
		@param avail_zone: Availability zone
		@param snapshot_id: Snapshot id
		'''
		ebs_vol = None
		pl = bus.platform		
		try:	
			conn = pl.new_ec2_conn()
		except AttributeError:
			conn = None
		
		if conn:
			# Find free devname			
			device = kwargs.get('device')
			used_letters = set(row['device'][-1] 
						for row in Storage.volume_table() 
						if row['type'] == 'ebs' or string \
							or row['device'].startswith('/dev/sd'))
			avail_letters = tuple(set(self.all_letters) - used_letters)
			if not device or not (device[-1] in avail_letters) or os.path.exists(device):
				letter = firstmatched(lambda l: not os.path.exists('/dev/sd%s' % l), avail_letters)
				if letter:
					device = '/dev/sd%s' % letter
				else:
					raise StorageError('No free letters for block device name remains')
			
			volume_id = kwargs.get('id')
			snap_id = kwargs.get('snapshot_id')
			ebs_vol = None
			delete_snap = False
			volume_attached = False			
			try:
				if volume_id and not snap_id:
					''' EBS has been already created '''
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
						snap_id = ebstool.create_snapshot(conn, ebs_vol, logger=self._logger).id
					
				if snap_id or not volume_id:
					''' Create new EBS '''
					kwargs['avail_zone'] = kwargs.get('avail_zone') or pl.get_avail_zone()
					ebs_vol = ebstool.create_volume(conn, kwargs.get('size'), kwargs['avail_zone'], 
						snap_id, logger=self._logger)

			
				if 'available' != ebs_vol.volume_state():
					if ebs_vol.attach_data.instance_id != pl.get_instance_id():
						''' EBS attached to another instance '''						
						self._logger.warning("EBS volume %s is not available. Detaching it from %s", 
											ebs_vol.id, ebs_vol.attach_data.instance_id)						
						ebstool.detach_volume(conn, ebs_vol, force=True, logger=self._logger)
					else:
						''' EBS attached to this instance'''
						volume_attached = True
						device = ebs_vol.attach_data.device
				
				if not volume_attached:
					''' Attach EBS to this instance '''
					device = ebstool.attach_volume(conn, ebs_vol, pl.get_instance_id(), device, 
						to_me=True, logger=self._logger)[1]
				
			except (Exception, BaseException), e:
				if ebs_vol:
					''' Detach EBS '''
					if (ebs_vol.update() and ebs_vol.attachment_state() != 'available'):
						ebstool.detach_volume(conn, ebs_vol, force=True, logger=self._logger)
							
					if not volume_id:
						ebs_vol.delete()
						
				raise StorageError('EBS volume construction failed: %s' % str(e))
			
			finally:
				if delete_snap and snap_id:
					conn.delete_snapshot(snap_id)
					
			
			kwargs['device'] = device
			kwargs['id'] = ebs_vol.id
			
		return super(EbsVolumeProvider, self).create(**kwargs)

	create = _create
	
	def create_from_snapshot(self, **kwargs):
		'''
		@param size: Size in GB
		@param avail_zone: Availability zone
		@param id: Snapshot id
		'''
		return self._create(**kwargs)

	def create_snapshot(self, vol, snap):
		pl = bus.platform
		conn = pl.new_ec2_conn()
		ebs_snap = conn.create_snapshot(vol.id, snap.description)
		snap.id = ebs_snap.id
		return snap

	def get_snapshot_state(self, snap):
		pl = bus.platform
		conn = pl.new_ec2_conn()
		state = conn.get_all_snapshots((snap.id,))[0].status
		if state == 'creating':
			state = Snapshot.CREATED
		return state

	def destroy(self, vol, force=False, **kwargs):
		'''
		@type vol: EbsVolume
		'''
		super(EbsVolumeProvider, self).destroy(vol)
		try:
			pl = bus.platform
			conn = pl.new_ec2_conn()
		except AttributeError:
			pass
		else:
			ebstool.detach_volume(conn, vol.id, self._logger)
			ebstool.delete_volume(conn, vol.id, self._logger)
		finally:
			vol.device = None							
	
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



Storage.explore_provider(EbsVolumeProvider, default_for_snap=True)

class S3TransferProvider(TransferProvider):
	schema	= 's3'
	urlparse.uses_netloc.append(schema)
	
	acl	= None
	
	_logger = None
	_bucket = None
	
	def __init__(self, acl='aws-exec-read'):
		self._logger = logging.getLogger(__name__)
		self.acl = acl

	def configure(self, remote_path):
		self._parse_path(remote_path)
	
	def put(self, local_path, remote_path):
		self._logger.info("Uploading '%s' to S3 under '%s'", local_path, remote_path)
		bucket_name, key_name = self._parse_path(remote_path)
		key_name = os.path.join(key_name, os.path.basename(local_path))
		
		try:
			connection = self._get_connection()
			
			if not self._bucket_check_cache(bucket_name):
				try:
					bck = connection.get_bucket(bucket_name)
				except S3ResponseError, e:
					if e.code == 'NoSuchBucket':
						pl = bus.platform
						try:  
							region = pl.s3_endpoints.keys()[pl.s3_endpoints.values().index(connection.host)]
							location = location_from_region(region)
						except:
							location = ''
						bck = connection.create_bucket(
							bucket_name, 
							location=location, 
							policy=self.acl
						)
					else:
						raise
				# Cache bucket
				self._bucket = bck				
			
			file = None
			try:
				key = Key(self._bucket)
				key.name = key_name
				file = open(local_path, "rb")			
				key.set_contents_from_file(file, policy=self.acl)
				return self._format_path(bucket_name, key_name)
			finally:
				if file:
					file.close()
			
		except:
			exc = sys.exc_info()
			raise TransferError, exc[1], exc[2]

	
	def get(self, remote_path, local_path):
		self._logger.info('Downloading %s from S3 to %s' % (remote_path, local_path))
		bucket_name, key_name = self._parse_path(remote_path)
		dest_path = os.path.join(local_path, os.path.basename(remote_path))
		
		try:
			connection = self._get_connection()
			
			if not self._bucket_check_cache(bucket_name):
				try:
					bkt = connection.get_bucket(bucket_name, validate=False)
					key = bkt.get_key(key_name)
				except S3ResponseError, e:
					if e.code in ('NoSuchBucket', 'NoSuchKey'):
						raise TransferError("S3 path '%s' not found" % remote_path)
					raise
				# Cache container object
				self._bucket = bkt				
			
			key.get_contents_to_filename(dest_path)			
			return dest_path			
			
		except:
			exc = sys.exc_info()
			raise TransferError, exc[1], exc[2]
	
	def list(self, remote_path):
		bucket_name, key_name = self._parse_path(remote_path)
		connection = self._get_connection()
		bkt = connection.get_bucket(bucket_name, validate=False)
		files = [self._format_path(self.bucket.name, key.name) for key in bkt.list(prefix=key_name)] 
		return tuple(files)
	
	def _bucket_check_cache(self, bucket):
		if self._bucket and self._bucket.name != bucket:
			self._bucket = None
		return self._bucket
	
	def _get_connection(self):
		pl = bus.platform
		return pl.new_s3_conn()
	
	def _format_path(self, bucket, key):
		return '%s://%s/%s' % (self.schema, bucket, key)

	def _parse_path(self, path):
		o = urlparse.urlparse(path)
		if o.scheme != self.schema:
			raise TransferError('Wrong schema')
		return o.hostname, o.path[1:]


def location_from_region(region):
	if region == 'us-east-1' or not region:
		return ''
	elif region == 'eu-west-1':
		return 'EU'
	else: 
		return region
	
	
# Workaround over bug when EBS volumes cannot be reattached on the same letter,
# and instance need to be rebooted to fix this issue.

def _cleanup_volume_table(*args, **kwargs):
	db = bus.db
	conn = db.get().get_connection()
	cur = conn.cursor()
	cur.execute("DELETE FROM storage where (device LIKE '/dev/sd%' or type = 'ebs') and state = 'detached'")
	conn.commit()

bus.on(init=lambda *args, **kwargs: bus.on(before_reboot_finish=_cleanup_volume_table))