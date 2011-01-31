
from scalarizr.bus import bus
from scalarizr.platform import Platform, PlatformError
from scalarizr.storage.transfer import Transfer
from .storage import S3TransferProvider

from boto import connect_ec2, connect_s3
from boto.ec2.regioninfo import RegionInfo
import logging, urllib2, re, os


Transfer.explore_provider(S3TransferProvider)


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
		'eu-west-1' 		: 's3-eu-west-1.amazonaws.com',
		'ap-southeast-1' 	: 's3-ap-southeast-1.amazonaws.com'
	}	
	_properties = {}
	_metadata = None
	_logger = None
	
	_ec2_cert = None
	_cnf = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._cnf = bus.cnf
	
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
	
	def block_devs_mapping(self):
		keys = self._get_property("latest/meta-data/block-device-mapping").split("\n")
		ret = list()
		for key in keys:
			ret.append((key, self._get_property("latest/meta-data/block-device-mapping/" + key)))
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
			# XXX: not ok
			self._ec2_cert = self._cnf.read_key(os.path.join(bus.etc_path, self._cnf.rawini.get(self.name, OPT_EC2_CERT_PATH)), title="EC2 certificate")
		return self._ec2_cert
	
	def new_ec2_conn(self):
		""" @rtype: boto.ec2.connection.EC2Connection """
		region = self.get_region()
		self._logger.debug("Return ec2 connection (endpoint: %s)", self._ec2_endpoints[region])
		return connect_ec2(region=RegionInfo(name=region, endpoint=self._ec2_endpoints[region]))

	def new_s3_conn(self):
		self._logger.debug("Return s3 connection (endpoint: %s)", self._s3_endpoints[self.get_region()])
		return connect_s3(host=self._s3_endpoints[self.get_region()])
	
	def set_access_data(self, access_data):
		Platform.set_access_data(self, access_data)
		key_id, key = self.get_access_keys()
		os.environ['AWS_ACCESS_KEY_ID'] = key_id
		os.environ['AWS_SECRET_ACCESS_KEY'] = key

	def clear_access_data(self):
		Platform.clear_access_data(self)
		try:
			del os.environ['AWS_ACCESS_KEY_ID']
			del os.environ['AWS_SECRET_ACCESS_KEY']
		except KeyError:
			pass
		
	@property
	def cloud_storage_path(self):
		return 's3://' + self.get_user_data(UD_OPT_S3_BUCKET_NAME)
	

