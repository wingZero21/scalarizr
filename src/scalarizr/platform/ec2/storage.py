'''
Created on Nov 24, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.bus import bus
from scalarizr.storage import Volume, VolumeProvider

import os
import logging

from boto import connect_ec2
from boto.s3.key import Key
from boto.exception import BotoServerError
from scalarizr.platform.ec2 import ebstool

class EbsVolume(Volume):
	volume_id = None
	def __init__(self,  devname, mpoint=None, fstype=None, type=None, volume_id=None, **kwargs):
		Volume.__init__(self, devname, mpoint, fstype, type)
		self.volume_id = volume_id


class EbsVolumeProvider(VolumeProvider):
	type = 'ebs'
	vol_class = EbsVolume
	
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
	
	def _create(self, **kwargs):
		'''
		@param size: Size in GB
		@param avail_zone: Availability zone
		@param snapshot_id: Snapshot id
		'''
		conn = connect_ec2()
		pl = bus.platform
		
		ebs_vol = ebstool.create_volume(conn, kwargs['size'], kwargs['zone'], 
				kwargs.get('snapshot_id'), logger=self._logger)
		ebstool.attach_volume(conn, ebs_vol, pl.get_instance_id(), kwargs['device'], 
				to_me=True, logger=self._logger)
		kwargs['volume_id'] = ebs_vol.id
		
		return super(EbsVolumeProvider, self).create(**kwargs)

	create = _create
	
	def create_from_snapshot(self, **kwargs):
		'''
		@param size: Size in GB
		@param avail_zone: Availability zone
		@param id: Snapshot id
		'''
		kwargs['snapshot_id'] = kwargs['id']
		return self._create(**kwargs)

	def create_snapshot(self, vol, snap):
		conn = connect_ec2()
		ebs_snap = conn.create_snapshot(vol.volume_id, snap.description)
		snap.id = dict(type=self.type, id=ebs_snap.id)
		return snap

	def destroy(self, vol):
		'''
		@type vol: EbsVolume
		'''
		super(EbsVolumeProvider, self).destroy(vol)
		conn = connect_ec2()
		conn.delete_volume(vol.volume_id)



'''
class S3UploadDest(uploader.UploadDest):
	
	def __init__(self, bucket, prefix=None, acl=None, logger=None):
		self.bucket = bucket
		self.prefix = prefix
		self.acl = acl 
		self._logger = logger or logging.getLogger(__name__)
	
	def put(self, filename):
		self._logger.info("Uploading '%s' to S3 bucket '%s'", filename, self.bucket.name)
		file = None
		base_name = os.path.basename(filename)
		obj_path = os.path.join(self.prefix, base_name) if self.prefix else base_name
		try:
			key = Key(self.bucket)
			key.name = obj_path
			file = open(filename, "rb")
			
			key.set_contents_from_file(file, policy=self.acl)
			
		except (BotoServerError, OSError), e:
			raise uploader.TransferError, e
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
			raise uploader.TransferError, e
		return os.path.join(self.bucket.name, dest_path)
	
	def get_prefix(self):
		return self.prefix
	
	def get_list_files(self):
		files = [key.name for key in self.bucket.get_all_keys(prefix=self.prefix)] \
			if self.prefix else [key.name for key in self.bucket.get_all_keys()]
		return files
	
def location_from_region(region):
	if region == 'us-east-1' or not region:
		return ''
	elif region == 'eu-west-1':
		return 'EU'
	else: 
		return region
'''