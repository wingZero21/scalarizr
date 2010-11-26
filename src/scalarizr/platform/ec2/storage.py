'''
Created on Nov 24, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.storage import uploader, Volume, Snapshot

import os
import logging

from boto import connect_ec2
from boto.s3.key import Key
from boto.exception import BotoServerError


class EbsVolume(Volume):
	ec2_volume_id = None
	def __init__(self,  devname, mpoint, fstype=None, ec2_volume_id=None):
		Volume.__init__(self, devname, mpoint, fstype)
		self.ec2_volume_id = ec2_volume_id
	
	def _create_snapshot(self, description):
		ec2_conn = connect_ec2()
		snap = ec2_conn.create_snapshot(self.volume_id, description)
		return Snapshot(snap.id, description)
	
	
class S3UploadDest(uploader.UploadDest):
	
	def __init__(self, bucket, acl=None, logger=None):
		self.bucket = bucket
		self.acl = acl 
		self._logger = logger or logging.getLogger(__name__)
	
	def put(self, filename):
		self._logger.info("Uploading '%s' to S3 bucket '%s'", filename, self.bucket.name)
		file = None
		try:
			key = Key(self.bucket)
			key.name = os.path.basename(filename)
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
	
	def get_list_files(self):
		return [key.name for key in self.bucket.get_all_keys()]
	
def location_from_region(region):
	if region == 'us-east-1' or not region:
		return ''
	elif region == 'eu-west-1':
		return 'EU'
	else: 
		return region