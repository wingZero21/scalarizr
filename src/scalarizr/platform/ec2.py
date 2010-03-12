
from scalarizr.core import Bus, BusEntries
from scalarizr.platform import Platform, PlatformError
import os
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
	
	_key = None
	_key_id = None
	_pk = None
	_cert = None
	
	_ec2_conn = None
	_s3_conn = None	
	
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
	
	def get_kernel_id(self):
		return self._get_property("latest/meta-data/kernel-id")
	
	def get_ramdisk_id(self):
		return self._get_property("latest/meta-data/ramdisk-id")
	
	def get_avail_zone(self):
		return self._get_property("latest/meta-data/placement/availability-zone")
	
	def _set_config_options(self, config, section, options):
		base_path = Bus()[BusEntries.BASE_PATH]
		for k in options:
			filename = None
			if k == "key_id":
				filename = "etc/.keys/aws_key_id"
			elif k == "key":
				filename = "etc/.keys/aws_key"
			elif k == "cert":
				filename = "etc/.keys/cert.pem"
			elif k == "pk":
				filename = "etc/.keys/pk.pem"
			if not filename is None:
				k = k + "_path"
				v = filename
				f = open(base_path + "/" + filename, "w+")
				f.write(options[k])
				f.close()
			else:
				v = options[k]
			config.set(section, k, v)
			
	def get_keys(self):
		if not (self._key and self._key_id):
			bus = Bus()
			base_path = bus[BusEntries.BASE_PATH]
			
			s = base_path + "/" + self.get_config_option(AwsPlatformOptions.KEY_ID_PATH)
			try:
				self._key_id = open(s, "r").read()
			except OSError, e:
				self._logger.error("Cannot read AWS key_id file '%s'. OSError: %s" % (s, str(e)))
				raise
		
			s = base_path + "/" + self.get_config_option(AwsPlatformOptions.KEY_PATH)
			try:
				self._key = open(s, "r").read()
			except OSError, e:
				self._logger.error("Cannot read AWS key file '%s'. OSError: %s" % (s, str(e)))
				raise
			
		return (self._key_id, self._key)
			
	def get_cert_pk(self):
		if (not self._cert and self._pk):
			bus = Bus()
			base_path = bus[BusEntries.BASE_PATH]
			
			s = base_path + "/" + self.get_config_option(AwsPlatformOptions.CERT_PATH)
			try:
				self._cert = open(s, "r").read()
			except OSError, e:
				self._logger.error("Cannot read AWS certificate file '%s'. OSError: %s" % (s, str(e)))
				raise

			s = base_path + "/" + self.get_config_option(AwsPlatformOptions.PK_PATH)
			try:
				self._pk = open(s, "r").read()
			except OSError, e:
				self._logger.error("Cannot read AWS private key file '%s'. OSError: %s" % (s, str(e)))
				raise
		
		return (self._cert, self._pk)
	
	def get_ec2_conn(self):
		if self._ec2_conn is None:
			key_id, key = self.get_keys()
			self._ec2_conn = boto.connect_ec2(key_id, key, 
						region=RegionInfo(name=self.get_avail_zone(), endpoint="ec2.amazonaws.com"))
		return self._ec2_conn

	def get_s3_conn(self):
		if self._s3_conn is None:
			key_id, key = self.get_keys()
			self._s3_conn = boto.connect_s3(key_id, key)
		return self._s3_conn
		
class AwsPlatformOptions:
	ACCOUNT_ID = "account_id"
	KEY_ID_PATH = "key_id_path"
	KEY_PATH = "key_path"
	CERT_PATH = "cert_path"
	PK_PATH = "pk_path"

	
class Fstab:
	LOCATION = None
	_entries = None
	_filename = None
	_re = None
	
	def __init__(self, filename=None):
		self._filename = filename if not filename is None else Mtab.LOCATION
		self._re = re.compile("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+).*$")
		
	def list_entries(self, rescan=False):
		if not self._entries or rescan:
			self._entries = []
			f = open(self._filename, os.O_RDONLY)
			for line in f:
				if line[0:1] == "#":
					continue
				m = self._re.match(line)
				if m:
					self._entries.append(_TabEntry(
						m.group(0), m.group(1), m.group(2), m.group(3), line.strip()
					))
			f.close()
			
		return list(self._entries)

class Mtab(Fstab):
	LOCAL_FS_TYPES = None	
		
class _TabEntry(object):
	device = None
	mpoint = None
	fstype = None
	options = None	
	value = None
	
	def __init__(self, device, mpoint, fstype, options, value):
		self.device = device
		self.mpoint = mpoint
		self.fstype = fstype
		self.options = options		
		self.value = value

		
_os = os.uname()[0].lower()

if _os == "linux":
	Fstab.LOCATION = "/etc/fstab"	
	Mtab.LOCATION = "/etc/mtab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs')

elif _os == "sun":
	Fstab.LOCATION = "/etc/vfstab"	
	Mtab.LOCATION = "/etc/mnttab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs', 
		'ufs', 'sharefs', 'dev', 'devfs', 'ctfs', 'mntfs',
		'proc', 'lofs',   'objfs', 'fd', 'autofs')

del _os

	