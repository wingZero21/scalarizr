
from scalarizr.platform import Platform, PlatformError
import logging
import urllib2
import re
import string

def get_platform():
	return Ec2Platform()

class Ec2Platform(Platform):
	_meta_url = "http://169.254.169.254/"
	_properties = {}
	_metadata = None
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__package__ + "." + self.__class__.__name__)
	
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
