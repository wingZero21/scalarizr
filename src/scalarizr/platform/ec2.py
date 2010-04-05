from scalarizr.platform import Platform, PlatformError
from scalarizr.util import write_key_file, read_key_file
from boto import connect_ec2, connect_s3
from boto.ec2.regioninfo import RegionInfo
import logging
import urllib2
import re



def get_platform():
	return AwsPlatform()

class AwsPlatform(Platform):
	name = "ec2"
	
	_meta_url = "http://169.254.169.254/"
	_properties = {}
	_metadata = None
	_logger = None
	
	_account_id = None
	_key = _key_id = None
	_pk = _cert = _ec2_cert = None
	
	_ec2_conn = None
	_s3_conn = None	
	
	def __init__(self):
		Platform.__init__(self)
		self._logger = logging.getLogger(__name__)
	
	def get_private_ip(self):
		return self._get_property("latest/meta-data/local-ipv4")
	
	def get_public_ip(self):
		return self._get_property("latest/meta-data/public-ipv4")
	
	def _get_property(self, name):
		if not self._properties.has_key(name):
			self._properties[name] = self._fetch_ec2_meta(name)
		return self._properties[name]
	
	def _fetch_ec2_meta(self, key):
		try:
			r = urllib2.urlopen(self._meta_url + key)
			return r.read().strip()
		except IOError, e:
			if isinstance(e, urllib2.HTTPError):
				if e.code == 404:
					return ""
			raise PlatformError("Cannot fetch ec2 metadata key '%s'. Error: %s" % (key, e))
		
	def get_metadata(self):
		if self._metadata is None:
			rawmeta = self._fetch_ec2_meta("latest/user-data")
			self._metadata = {}
			for k, v in re.findall("([^=]+)=([^;]*);?", rawmeta):
				self._metadata[k] = v
			
		return self._metadata 

	def get_instance_id(self):
		return self._get_property("latest/meta-data/instance-id")
	
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
	
	
	def _set_config_options(self, config, section, options):
		# FIXME: maybe better remove _set_config_options ?
		for k, v in options.items():
			if k == "account_id":
				write_key_file("aws_account_id", v)
			if k == "key_id":
				write_key_file("aws_key_id", v)
			elif k == "key":
				write_key_file("aws_key", v)
			elif k == "cert":
				write_key_file("ec2_cert.pem", v)
			elif k == "pk":
				write_key_file("ec2_pk.pem", v)
			else:
				config.set(section, k, v)
			
	def get_account_id(self):
		if not self._account_id:
			self._account_id = read_key_file("aws_account_id", title="AWS account id")
		return self._account_id
			
	def get_access_keys(self):
		if not self._key:
			self._key = read_key_file("aws_key", title="AWS access secret key")
			self._key_id = read_key_file("aws_key_id", title="AWS access key_id")
		return (self._key_id, self._key)
			
	def get_cert_pk(self):
		if not self._cert:
			self._cert = read_key_file("ec2_cert.pem", title="EC2 user certificate")
			self._pk = read_key_file("ec2_pk.pem", title="EC2 user private key")
		return (self._cert, self._pk)
	
	def get_ec2_cert(self):
		if not self._ec2_cert:
			self._ec2_cert = read_key_file("ec2_cert.pem", title="EC2 certificate", public=True)
		return self._ec2_cert
	
	def get_ec2_conn(self):
		if self._ec2_conn is None:
			key_id, key = self.get_access_keys()
			self._ec2_conn = connect_ec2(key_id, key, 
						region=RegionInfo(name=self.get_avail_zone(), endpoint="ec2.amazonaws.com"))
		return self._ec2_conn

	def get_s3_conn(self):
		if self._s3_conn is None:
			key_id, key = self.get_access_keys()
			self._s3_conn = connect_s3(key_id, key)
		return self._s3_conn
		

	
	

	


	