'''
Created on Nov 24, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.bus import bus
from scalarizr.storage import Storage, Volume, VolumeProvider, StorageError, devname_not_empty, \
	VolumeConfig, Snapshot
from scalarizr.storage.transfer import TransferProvider, TransferError
from scalarizr.platform.ec2 import ebstool

import os
import logging
import string
from urlparse import urlparse

from boto import connect_ec2, connect_s3
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
	pass
		

class EbsVolumeProvider(VolumeProvider):
	type = 'ebs'
	vol_class = EbsVolume
	snap_class = EbsSnapshot
	unused_letters = list(string.ascii_lowercase[14:])
	
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
		attached = False		
		conn = connect_ec2()
		pl = bus.platform
		
		# Find free devname
		device = kwargs.get('device')
		if not device or not (device[-1] in EbsVolumeProvider.unused_letters) or os.path.exists(device):
			if not EbsVolumeProvider.unused_letters:
				EbsVolumeProvider.unused_letters = list(string.ascii_lowercase[14:])
			letter = firstmatched(lambda l: not os.path.exists('/dev/sd%s' % l), EbsVolumeProvider.unused_letters)
			if letter:
				device = '/dev/sd%s' % letter
				EbsVolumeProvider.unused_letters.remove(device[-1])				
			else:
				raise StorageError('No free letters for block device name remains')
		
		volume_id = kwargs.get('id')
		try:
			if volume_id:
				''' EBS volume has been already created '''

				try:
					ebs_vol = conn.get_all_volumes([volume_id])[0]
				except IndexError:
					raise StorageError("Volume '%s' doesn't exist." % volume_id)
				
				if 'available' != ebs_vol.volume_state():
					self._logger.warning("Volume %s is not available.", ebs_vol.id)
					if ebs_vol.attach_data.instance_id != pl.get_instance_id():
						''' Volume attached to another instance '''
						ebstool.detach_volume(conn, ebs_vol, force=True, logger=self._logger)
					else:
						'''Volume attached to this instance'''
						attached = True
						device = ebs_vol.attach_data.device
			else:
				''' Create new EBS '''
				kwargs['avail_zone'] = kwargs.get('avail_zone') or pl.get_avail_zone()
				ebs_vol = ebstool.create_volume(conn, kwargs['size'], kwargs['avail_zone'], 
					kwargs.get('snapshot_id'), logger=self._logger)
			
			if not attached:
				ebstool.attach_volume(conn, ebs_vol, pl.get_instance_id(), device, 
					to_me=True, logger=self._logger)
			
		except (Exception, BaseException), e:
			self._logger.error("Ebs creation failed. Error: %s" % e)
			if ebs_vol:
				# detach volume
				if (ebs_vol.update() and ebs_vol.attachment_state() != 'available'):
					ebstool.detach_volume(conn, ebs_vol, logger=self._logger)
					'''
					try:	
						ebs_vol.detach(force=True)
						wait_until(lambda: ebs_vol.update() and ebs_vol.attachment_state() == 'available',
							   logger = self._logger)
					except EC2ResponseError, e:
						if not "is in the 'available' state" in str(e):
							raise
					'''
						
				if not volume_id:
					ebs_vol.delete()
					
			raise StorageError('Volume creating failed: %s' % e)
		
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
		kwargs['snapshot_id'] = kwargs['id']
		del kwargs['id']
		return self._create(**kwargs)

	def create_snapshot(self, vol, snap):
		conn = connect_ec2()
		ebs_snap = conn.create_snapshot(vol.id, snap.description)
		snap.id = ebs_snap.id
		return snap

	def destroy(self, vol, force=False):
		'''
		@type vol: EbsVolume
		'''
		super(EbsVolumeProvider, self).destroy(vol)
		conn = connect_ec2()
		ebstool.detach_volume(conn, vol.id, self._logger)
		ebstool.delete_volume(conn, vol.id, self._logger)
		vol.device = None
	
	@devname_not_empty		
	def detach(self, vol, force=False):
		super(EbsVolumeProvider, self).detach(vol)
		try:
			key_id 	   = os.environ['AWS_ACCESS_KEY_ID']
			secret_key = os.environ['AWS_SECRET_ACCESS_KEY']
		except KeyError:
			raise Exception("Can't get AWS credentials from OS environment variables.")
		
		conn = connect_ec2(key_id, secret_key)
		ebstool.detach_volume(conn, vol.id, self._logger)
		vol.device = None
		vol.detached = True


Storage.explore_provider(EbsVolumeProvider, default_for_snap=True)


class S3TransferProvider(TransferProvider):
	
	schema	= 's3'
	acl		= None
	
	def __init__(self, acl=None):
		self._logger = logging.getLogger(__name__)
		self.acl = acl

	def configure(self, remote_path, force=False):
		o = urlparse(remote_path)

		if o.scheme != self.schema:
			raise TransferError('Wrong schema.')		
		try:
			s3_key_id = os.environ["AWS_ACCESS_KEY_ID"]
			s3_secret_key = os.environ["AWS_SECRET_ACCESS_KEY"]
		except KeyError:
			raise TransferError("Can't get S3 credentials from environment variables.")
		
		s3_con = connect_s3(s3_key_id, s3_secret_key)
		try:
			self.bucket = s3_con.get_bucket(o.hostname)
		except S3ResponseError, e:
			if 'NoSuchBucket' in str(e) and force:
				self.bucket = s3_con.create_bucket(o.hostname)
			else:
				raise
			
		self.prefix = o.path

	
	def put(self, local_path, remote_path):
		self._logger.info("Uploading '%s' to S3 bucket '%s'", local_path, self.bucket.name)
		file = None
		base_name = os.path.basename(local_path)
		obj_path = os.path.join(self.prefix, base_name) if self.prefix else base_name
		try:
			key = Key(self.bucket)
			key.name = obj_path
			file = open(local_path, "rb")			
			key.set_contents_from_file(file, policy=self.acl)
			
		except (BotoServerError, OSError), e:
			raise TransferError, e
		finally:
			if file:
				file.close()
		
		return os.path.join(self.bucket.name, key.name)

	
	def get(self, filename, dest):
		dest_path = os.path.join(dest, os.path.basename(filename))
		try:
			key = self.bucket.get_key(filename)
			key.get_contents_to_filename(dest_path)
		except (BotoServerError, OSError), e:
			raise TransferError, e
		return os.path.join(self.bucket.name, dest_path)
	
	def list(self, url=None):
		prefix = urlparse(url).path[1:] if url else self.prefix
		if not prefix:
			prefix = ''
		files = [key.name for key in self.bucket.list(prefix=prefix)] 
		return files
	
def location_from_region(region):
	if region == 'us-east-1' or not region:
		return ''
	elif region == 'eu-west-1':
		return 'EU'
	else: 
		return region
