__author__ = 'Nick Demyanchuk'

import os
import base64
import urllib2
import httplib2

try:
	import json
except ImportError:
	import simplejson as json

from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build

from scalarizr.platform import Platform
from scalarizr.storage.transfer import Transfer
from .storage import GoogleCSTransferProvider

Transfer.explore_provider(GoogleCSTransferProvider)

COMPUTE_RW_SCOPE = 'https://www.googleapis.com/auth/compute'
STORAGE_FULL_SCOPE = 'https://www.googleapis.com/auth/devstorage.full_control'


def get_platform():
	return GcePlatform()


class GcePlatform(Platform):
	metadata_url = 'http://metadata.google.internal/0.1/meta-data/'
	_metadata = None


	def get_user_data(self, key=None):
		if self._userdata is None:
			self._userdata = dict()
			resp = self._get_metadata('attributes/')
			keys = resp.strip().split()
			for key in keys:
				value = self._get_metadata('attributes/%s' % key)
				self._userdata[key] = value

		return self._userdata.get(key) if key else self._userdata


	def _get_metadata(self, key):
		if self._metadata is None:
			self._metadata = dict()

		if not key in self._metadata:
			key_url = os.path.join(self.metadata_url, key)
			resp = urllib2.urlopen(key_url)
			self._metadata[key] = resp.read()

		return self._metadata[key]


	def get_public_ip(self):
		network = self._get_metadata('network')
		network = json.loads(network)
		return network['networkInterface'][0]['accessConfiguration'][0]['externalIp']


	def get_private_ip(self):
		network = self._get_metadata('network')
		network = json.loads(network)
		return network['networkInterface'][0]['ip']


	def get_project_id(self):
		return self._get_metadata('project-id')


	def get_zone(self):
		return self._get_metadata('zone')


	def get_numeric_project_id(self):
		return self._get_metadata('numeric-project-id')


	def get_machine_type(self):
		return self._get_metadata('machine-type')


	def get_instance_id(self):
		return self._get_metadata('instance-id')


	def get_image(self):
		return self._get_metadata('image')


	def new_compute_client(self):
		http = self._get_auth()
		return build('compute', 'v1beta12', http=http)


	def new_storage_client(self):
		http = self._get_auth()
		return build('storage', 'v1beta1', http=http)


	def _get_auth(self):
		http = httplib2.Http()
		email = self.get_access_data('service_account_name')
		pk = base64.b64decode(self.get_access_data('key'))
		cred = SignedJwtAssertionCredentials(email, pk, scope=[COMPUTE_RW_SCOPE, STORAGE_FULL_SCOPE])
		return cred.authorize(http)






