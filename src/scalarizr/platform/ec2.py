from scalarizr.platform import Platform, PlatformError
from scalarizr.bus import bus
from boto import connect_ec2, connect_s3
from boto.ec2.regioninfo import RegionInfo
import logging
import urllib2
import re
from scalarizr.util import configtool

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
		return connect_ec2(key_id, key, 
				region=RegionInfo(name=self.get_avail_zone(), endpoint="ec2.amazonaws.com"))

	def new_s3_conn(self):
		key_id, key = self.get_access_keys()
		return connect_s3(key_id, key)

