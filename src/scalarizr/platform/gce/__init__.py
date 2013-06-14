from __future__ import with_statement

__author__ = 'Nick Demyanchuk'

import os
import base64
import logging
import urllib2
import httplib2
import threading
from httplib import BadStatusLine

try:
    import json
except ImportError:
    import simplejson as json

from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build, Resource
from apiclient.http import HttpRequest

from scalarizr.platform import Platform
from scalarizr.storage.transfer import Transfer
from scalarizr.platform.gce.storage import GoogleCSTransferProvider
from scalarizr import util


Transfer.explore_provider(GoogleCSTransferProvider)

COMPUTE_RW_SCOPE = 'https://www.googleapis.com/auth/compute'
STORAGE_FULL_SCOPE = 'https://www.googleapis.com/auth/devstorage.full_control'


LOG = logging.getLogger(__name__)


class GoogleApiClientLoggerFilter:
    def filter(self, record):
        if 'takes exactly' or 'takes at most' in record.message:
            return False
        return True

api_logger = logging.getLogger('oauth2client.util')
api_logger.addFilter(GoogleApiClientLoggerFilter())


def get_platform():
    return GcePlatform()

class BadStatusLineHandler(object):
    def __init__(self, obj):
        self._obj = obj

    def _wrap(self, fn):
        def wrapper(*args, **kwargs):
            tries = 3
            while True:
                try:
                    return fn(*args, **kwargs)
                except BadStatusLine:
                    tries -= 1
                    if not tries:
                        raise
                    LOG.warning('Caught "BadStatusLine" exception from google API, retrying')
        return wrapper

    def __getattribute__(self, item):
        try:
            return object.__getattribute__(self, item)
        except AttributeError:
            item = getattr(self._obj, item)
            if callable(item) or isinstance(item, (HttpRequest, Resource)):
                return BadStatusLineHandler(item)
            else:
                return item

    def __call__(self, *args, **kwargs):
        if self._obj.__name__ in ('execute', 'next_chunk'):
            return self._wrap(self._obj.__call__)(*args, **kwargs)
        else:
            item = self._obj(*args, **kwargs)
            if isinstance(item, (HttpRequest, Resource)):
                return BadStatusLineHandler(item)
            else:
                return item



class GoogleServiceManager(object):
    """
    Manages 1 service connection per thread
    Works out dead threads' connections
    """

    def __init__(self, pl, s_name, s_ver, *scope):
        self.pl = pl
        self.s_name= s_name
        self.s_ver = s_ver
        self.scope = list(scope)
        self.map = {}
        self.lock = threading.Lock()
        self.pool = []


    def get_service(self):
        current_thread = threading.current_thread()
        with self.lock:
            if not current_thread in self.map:
                # Check other threads
                for t, s in self.map.items():
                    if not t.is_alive():
                        self.pool.append(s)
                        del self.map[t]

                if self.pool:
                    s = self.pool.pop()
                    self.map[current_thread] = s
                    return s

                http = self._get_auth()
                s = build(self.s_name, self.s_ver, http=http)
                wrapped = BadStatusLineHandler(s)
                self.map[current_thread] = wrapped

            return self.map[current_thread]


    def _get_auth(self):
        http = httplib2.Http()
        email = self.pl.get_access_data('service_account_name')
        pk = base64.b64decode(self.pl.get_access_data('key'))
        cred = SignedJwtAssertionCredentials(email, pk, scope=self.scope)
        return cred.authorize(http)



class GcePlatform(Platform):
    metadata_url = 'http://metadata/computeMetadata/v1beta1/'
    _metadata = None

    def __init__(self):
        Platform.__init__(self)
        self.compute_svc_mgr = GoogleServiceManager(
                self, 'compute', 'v1beta14', COMPUTE_RW_SCOPE, STORAGE_FULL_SCOPE)

        self.storage_svs_mgr = GoogleServiceManager(
                self, 'storage', 'v1beta1', STORAGE_FULL_SCOPE)


    def get_user_data(self, key=None):
        if self._userdata is None:
            try:
                raw_userdata = self._get_metadata('instance/attributes/scalr').strip()
                self._userdata = self._parse_user_data(raw_userdata)
            except urllib2.HTTPError, e:
                if 404 == e.code:
                    self._userdata = dict()
                else:
                    raise

        return self._userdata.get(key) if key else self._userdata


    def _get_metadata(self, key, url=None):
        if url is None:
            url = key

        if self._metadata is None:
            self._metadata = dict()

        if not url in self._metadata:
            key_url = os.path.join(self.metadata_url, url)
            resp = urllib2.urlopen(key_url)
            self._metadata[key] = resp.read()

        return self._metadata[key]


    def get_public_ip(self):
        network = self._get_metadata('network', 'instance/network-interfaces/?recursive=true')
        network = json.loads(network)
        return network[0]['accessConfigs'][0]['externalIp']


    def get_private_ip(self):
        network = self._get_metadata('network', 'instance/network-interfaces/?recursive=true')
        network = json.loads(network)
        return network[0]['ip']


    def get_project_id(self):
        return self._get_metadata('project/project-id')


    def get_zone(self):
        return self._get_metadata('instance/zone')


    def get_numeric_project_id(self):
        return self._get_metadata('project/numeric-project-id')


    def get_machine_type(self):
        return self._get_metadata('instance/machine-type')


    def get_instance_id(self):
        return self._get_metadata('instance/id')


    def get_hostname(self):
        return self._get_metadata('instance/hostname')


    def get_image(self):
        return self._get_metadata('instance/image')


    def new_compute_client(self):
        return self.compute_svc_mgr.get_service()


    def new_storage_client(self):
        return self.storage_svs_mgr.get_service()
