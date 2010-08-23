from scalarizr.platform import Platform, PlatformError
from scalarizr.bus import bus
import logging
import urllib2
import re
from scalarizr.util import configtool
from scalarizr.util import system



def get_platform():
	return rsPlatform()

class rsPlatform(Platform):
	
	name 			= "rs"
	
	_meta_url		= None
	_storage_url	= None
	_cdn_url		= None
	_storage_url	= None
	_auth_token		= None
	_id				= None
	_metadata		= None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._metadata = {}
	
	def _fetch_rs_meta(self, key):
		url = self._meta_url + key
		try:
			r = urllib2.urlopen(url)
			return r.read().strip()
		except IOError, e:
			if isinstance(e, urllib2.HTTPError):
				if e.code == 401:
					self._update_auth_data()
					return self._fetch_rs_meta(key)
			raise PlatformError("Cannot fetch rs metadata url '%s'. Error: %s" % (url, e))
		

	def _update_auth_data(self):
		"""
		Send request to Scalr.
		Scalr returns new auth-token, api url, storage url and cdn url
		"""
		pass

	def get_private_ip(self):
		return self._get_ip("eth0")

	def get_public_ip(self):
		return self._get_ip("eth1")
	
	def _get_ip(self, iface=None):
		if not iface:
			raise PlatformError('You must specify interface name for getting ip address')
		if not hasattr(self, '_ip_re'):
			self._ip_re = re.compile('inet\s*addr:(?P<ip>[\d\.]+)', re.M)
			
		out = system('ifconfig ' + iface)[0]
		result = re.search(self._ip_re, out)
		if not result:
			return None		
		return result.group('ip')
			