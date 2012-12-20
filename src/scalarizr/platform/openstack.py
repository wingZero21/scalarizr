import urllib2
import json
import os

from cinderclient.v1 import client as cinder_client
from novaclient.v1_1 import client as nova_client

from scalarizr.platform import Platform
from scalarizr.platform import PlatformError
from scalarizr.util.filetool import read_file
from scalarizr.bus import bus

#TODO: move next hardcode to some config
# class OpenstackCredentials:
#     USER = 'admin'
#     PASSWORD = 'password'
#     TENANT = 'demo'
#     SERVER_ADDRESS = 'http://192.168.1.100'

#     AUTH_URL = '%s:5000/v2.0' % SERVER_ADDRESS
#     KEYSTONE_ENDPOINT = AUTH_URL
#     GLANCE_ENDPOINT = '%s:9292' % SERVER_ADDRESS


class OpenstackServiceWrapper(object):
    def _make_connection(self, **kwargs):
        raise NotImplementedError()

    def __init__(self, user, password, tenant, auth_url):
        self.user = user
        self.password = password
        self.tenant = tenant
        self.auth_url = auth_url
        self.connection = None
        self.connect = self.reconnect

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def reconnect(self):
        self.connection = self._make_connection()

    #TODO: make connection check more properly
    @property
    def has_connection(self):
        self.reconnect()
        return self.connection is not None


class CinderWrapper(OpenstackServiceWrapper):

    def _make_connection(self, **kwargs):
        return cinder_client.Client(self.user,
                                    self.password,
                                    self.tenant,
                                    self.auth_url)


class NovaWrapper(OpenstackServiceWrapper):

    def _make_connection(self, service_type='compute', **kwargs):
        return nova_client.Client(self.user,
                                  self.password,
                                  self.tenant,
                                  self.auth_url,
                                  service_type=service_type)


class OpenstackPlatform(Platform):

    _meta_url = "http://169.254.169.254/openstack/latest/meta_data.json"
    _metadata = {}
    _userdata = None

    def _get_property(self, name):
        if not name in self._metadata:
            self._metadata = self._fetch_metadata()
        return self._metadata[name]

    def get_server_id(self):
        self._get_property('uuid')

    def get_avail_zone(self):
        self._get_property('availability_zone')

    def get_ssh_pub_key(self):
        self._get_property('public_keys')  # TODO: take one key

    def get_user_data(self, key=None):
        if self._userdata is None:
            self._metadata = self._fetch_metadata()
            self._userdata = self._metadata['meta']
        if key:
            return self._userdata[key] if key in self._userdata else None
        else:
            return self._userdata

    def _fetch_metadata(self):
        """
        Fetches whole metadata dict. Unlike Ec2LikePlatform,
        which fetches data for concrete key.
        """
        url = self._meta_url
        try:
            r = urllib2.urlopen(url)
            response = r.read().strip()
            return json.loads(response)
        except IOError, e:
            urllib_error = isinstance(e, urllib2.HTTPError) or \
                isinstance(e, urllib2.URLError)
            if urllib_error:
                metadata = self._fetch_metadata_from_file()
                # TODO: move some keys from metadata to parent dict,
                # that should be there when fetching from url
                return {'meta': metadata}
            raise PlatformError("Cannot fetch %s metadata url '%s'. "
                                "Error: %s" % (self.name, url, e))

    def _fetch_metadata_from_file(self):
        cnf = bus.cnf
        if self._userdata is None:
            path = cnf.private_path('.user-data')
            if os.path.exists(path):
                rawmeta = read_file(path)
                if not rawmeta:
                    raise PlatformError("Empty user-data")
                return self._parse_user_data(rawmeta)
        return self._userdata

    def new_cinder_connection(self):
        return CinderWrapper(self._access_data["user"],
                             self._access_data["password"],
                             self._access_data["tenant"],
                             self._access_data["auth_url"])

    def new_nova_connection(self):
        return NovaWrapper(self._access_data["user"],
                           self._access_data["password"],
                           self._access_data["tenant"],
                           self._access_data["auth_url"])


def get_platform():
    return OpenstackPlatform()
