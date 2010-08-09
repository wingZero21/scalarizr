from scalarizr.platform import Platform, PlatformError
from scalarizr.bus import bus
from boto import connect_ec2, connect_s3
from boto.ec2.regioninfo import RegionInfo
import logging
import urllib2
import re
import os
from scalarizr.util import configtool
from threading import Thread, Lock
from Queue import Queue, Empty
from boto.s3 import Key
from boto.exception import BotoServerError
"""
Platform configuration options
"""
OPT_ACCOUNT_ID = "account_id"
OPT_KEY = "key"
OPT_KEY_ID = "key_id"
OPT_EC2_CERT_PATH = "ec2_cert_path"
OPT_CERT_PATH = "cert_path"
OPT_PK_PATH = "pk_path"


"""
User data options 
"""
UD_OPT_S3_BUCKET_NAME = "s3bucket"


def get_platform():
	return Ec2Platform()

class Ec2Platform(Platform):
	name = "ec2"
	
	_meta_url = "http://169.254.169.254/"
	_ec2_endpoints = {
		"us-east-1" 		: "ec2.amazonaws.com",
		"us-west-1" 		: "ec2.us-west-1.amazonaws.com",
		"eu-west-1" 		: "ec2.eu-west-1.amazonaws.com",
		"ap-southeast-1" 	: "ec2.ap-southeast-1.amazonaws.com"
	}
	_s3_endpoints = {
		'us-east-1' 		: 's3.amazonaws.com',
		'us-west-1' 		: 's3-us-west-1.amazonaws.com',
		'eu-west-1' 		: 's3.amazonaws.com',
		'ap-southeast-1' 	: 's3-ap-southeast-1.amazonaws.com'
	}	
	_properties = {}
	_metadata = None
	_logger = None
	
	_ec2_cert = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
	
	def get_private_ip(self):
		return self._get_property("latest/meta-data/local-ipv4")
	
	def get_public_ip(self):
		return self._get_property("latest/meta-data/public-ipv4")
	
	def get_public_hostname(self):
		return self._get_property("latest/meta-data/public-hostname")
	
	def _get_property(self, name):
		if not self._properties.has_key(name):
			self._properties[name] = self._fetch_ec2_meta(name)
		return self._properties[name]
	
	def _fetch_ec2_meta(self, key):
		url = self._meta_url + key
		try:
			r = urllib2.urlopen(url)
			return r.read().strip()
		except IOError, e:
			if isinstance(e, urllib2.HTTPError):
				if e.code == 404:
					return ""
			raise PlatformError("Cannot fetch ec2 metadata url '%s'. Error: %s" % (url, e))
		
	def get_user_data(self, key=None):
		if self._metadata is None:
			rawmeta = self._fetch_ec2_meta("latest/user-data")
			self._metadata = {}
			for k, v in re.findall("([^=]+)=([^;]*);?", rawmeta):
				self._metadata[k] = v
			
		if key:
			return self._metadata[key] if key in self._metadata else None
		else:
			return self._metadata 

	def get_instance_id(self):
		return self._get_property("latest/meta-data/instance-id")
	
	def get_instance_type(self):
		return self._get_property("latest/meta-data/instance-type")
	
	def get_ami_id(self):
		return self._get_property("latest/meta-data/ami-id")

	def get_ancestor_ami_ids(self):
		return self._get_property("latest/meta-data/ancestor-ami-ids").split("\n")
	
	def get_kernel_id(self):
		return self._get_property("latest/meta-data/kernel-id")
	
	def get_ramdisk_id(self):
		return self._get_property("latest/meta-data/ramdisk-id")
	
	def get_avail_zone(self):
		return self._get_property("latest/meta-data/placement/availability-zone")
	
	def get_region(self):
		return self.get_avail_zone()[0:-1]
	
	def get_block_device_mapping(self):
		keys = self._get_property("latest/meta-data/block-device-mapping").split("\n")
		ret = {}
		for key in keys:
			ret[key] = self._get_property("latest/meta-data/block-device-mapping/" + key)
		return ret
		
	def get_ssh_pub_key(self):
		return self._get_property("latest/meta-data/public-keys/0/openssh-key")
			
	def get_account_id(self):
		return self.get_access_data("account_id").encode("ascii")
			
	def get_access_keys(self):
		# Keys must be in ASCII because hmac functions doesn't works with unicode		
		return (self.get_access_data("key_id").encode("ascii"), self.get_access_data("key").encode("ascii"))
			
	def get_cert_pk(self):
		return (self.get_access_data("cert").encode("ascii"), self.get_access_data("pk").encode("ascii"))
	
	def get_ec2_cert(self):
		if not self._ec2_cert:
			config = bus.config
			sect_name = configtool.get_platform_section_name(self.name)
			self._ec2_cert = configtool.read_key(config.get(sect_name, OPT_EC2_CERT_PATH), 
					key_title="EC2 certificate")
		return self._ec2_cert
	
	def new_ec2_conn(self):
		""" @rtype: boto.ec2.connection.EC2Connection """
		key_id, key = self.get_access_keys()
		region = self.get_region()
		self._logger.info("Return ec2 connection (endpoint: %s)", self._ec2_endpoints[region])
		return connect_ec2(key_id, key, region=RegionInfo(name=region, endpoint=self._ec2_endpoints[region]))

	def new_s3_conn(self):
		key_id, key = self.get_access_keys()
		self._logger.info("Return s3 connection (endpoint: %s)", self._s3_endpoints[self.get_region()])
		return connect_s3(key_id, key, host=self._s3_endpoints[self.get_region()])


def s3_location_from_region(region):
	if region == 'us-east-1' or not region:
		return ''
	elif region == 'eu-west-1':
		return 'EU'
	else: 
		return region


class S3Uploader(object):
	_queue = None
	state = None
	
	def __init__(self, pool=2, max_attempts=3, logger=None):
		self._logger = logger or logging.getLogger(__name__) 
		self._queue = Queue()
		self._pool = pool
		self._max_attempts = max_attempts
	
	def upload(self, files, bucket, s3_conn=None, acl=None, progress_cb=None):
		if not s3_conn:
			platform = bus.platform
			s3_conn  = platform.new_s3_conn()
		# Enqueue 
		for file in files:
			self._queue.put((file, 0)) # filename, attempt, last_error
			
		self._result = [] # list of tuples (filename, ok, last_error)
		
		self.state = "starting"
		
		self._uploaders = []
		self._failed_files = []
		self._failed_files_lock = Lock()
		
		#Starting threads
		for n in range(self._pool):
			uploader = Thread(name="Uploader-%s" % n, target=self._worker, 
					args=(s3_conn, bucket, acl))
			self._logger.debug("Starting uploader '%s'", uploader.getName())
			uploader.start()
			self._uploaders.append(uploader)

		self.state = "in-progress"
		# Join workers
		for uploader in self._uploaders:
			uploader.join()
			self._logger.debug("Uploader '%s' finished", uploader.getName())
		self.state = "done"
	
		if self._failed_files:
			raise PlatformError("Cannot upload several files. %s" % [", ".join(self._failed_files)])
		
		self._logger.info("Upload complete!")

		# Return tuple of all files	def set_access_data(self, access_data):
		return tuple([os.path.join(bucket.name, file) for file in self._result])

	def _worker(self, s3_conn, bucket, acl):
		self._logger.debug("queue: %s, bucket: %s", self._queue, bucket)
		try:
			while 1:
				filename, upload_attempts = self._queue.get(False)
				try:
					self._logger.info("Uploading '%s' to S3 bucket '%s'", filename, bucket.name)
					key = Key(bucket)
					key.name = os.path.basename(filename)
					file = open(filename, "rb")
					key.set_contents_from_file(file, policy=acl)
					self._result.append(key.name)
				except (BotoServerError, OSError), e:
					self._logger.error("Cannot upload '%s'. %s", filename, e)
					if upload_attempts < self._max_attempts:
						self._logger.info("File '%s' will be uploaded within the next attempt", filename)
						upload_attempts += 1
						self._queue.put((filename, upload_attempts))
					else:
						try:
							self._failed_files_lock.acquire()
							self._failed_files.append(filename)
						finally:
							self._failed_files_lock.release()
				finally:
					file.close()
		except Empty:
			return