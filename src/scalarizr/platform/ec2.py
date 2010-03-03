
from scalarizr.core import Bus, BusEntries
from scalarizr.platform import Platform, PlatformError
import logging
import urllib2
import re
import string
import boto
from boto.ec2.regioninfo import RegionInfo


def get_platform():
	return AwsPlatform()

class AwsPlatform(Platform):
	name = "ec2"
	
	_meta_url = "http://169.254.169.254/"
	_properties = {}
	_metadata = None
	_logger = None
	
	def __init__(self):
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
			return string.strip(r.read())
		except Exception, e:
			self._logger.error(str(e))
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
	
	def get_avail_zone(self):
		return self._get_property("latest/meta-data/placement/availability-zone")


_aws_instance = None
def Aws():
	global _aws_instance
	if _aws_instance is None:
		_aws_instance = _Aws()
	return _aws_instance

class _Aws():
	_key = None
	_key_id = None
	_ec2_conn = None
	_s3_conn = None
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
	
	def set_keys(self, key_id, key):
		self._key = key
		self._key_id = key_id

		bus = Bus()
		base_path = bus[BusEntries.BASE_PATH]
		
		s = base_path + "/etc/.keys/aws_key_id"
		try:
			f = open(s, "w+")
			f.write(self._key_id)
			f.close()
		except IOError, e:
			self._logger.error("Cannot save AWS key_id into file '%s'. IOError: %s" % (s, str(e)))
			raise
		
		s = base_path + "/etc/.keys/aws_key"
		try:
			f = open(s, "w+")
			f.write(self._key)
			f.close()
		except IOError, e:
			self._logger.error("Cannot save AWS key into file '%s'. IOError: %s" % (s, str(e)))
			raise
		
		
	def _retrive_keys(self):
		if not (self._key and self._key_id):
			bus = Bus()
			base_path = bus[BusEntries.BASE_PATH]
			s = base_path + "/etc/.keys/aws_key_id"
			try:
				self._key_id = open(s, "r").read()
			except IOError, e:
				self._logger.error("Cannot read AWS key_id file '%s'. IOError: %s" % (s, str(e)))
				raise
		
			s = base_path + "/etc/.keys/aws_key"
			try:
				self._key = open(s, "r").read()
			except IOError, e:
				self._logger.error("Cannot read AWS key file '%s'. IOError: %s" % (s, str(e)))
				raise
			
		return (self._key_id, self._key)
		
	def get_ec2_conn(self):
		if self._ec2_conn is None:
			platform = AwsPlatform()
			(key_id, key) = self._retrive_keys()
			self._ec2_conn = boto.connect_ec2(key_id, key, 
						region=RegionInfo(name=platform.get_avail_zone(), endpoint="ec2.amazonaws.com"))
		return self._ec2_conn

	def get_s3_conn(self):
		if self._s3_conn is None:
			(key_id, key) = self._retrive_keys()
			self._s3_conn = boto.connect_s3(key_id, key)
		return self._s3_conn

