import urllib2
import json
import os
import sys
import logging
import socket
import struct
import array
import fcntl

from cinderclient.v1 import client as cinder_client
from novaclient.v1_1 import client as nova_client

from scalarizr.platform import Platform
from scalarizr.platform import PlatformError
from scalarizr.bus import bus


LOG = logging.getLogger(__name__)


class OpenstackServiceWrapper(object):
	def _make_connection(self, **kwargs):
		raise NotImplementedError()

	def __init__(self, user, password, tenant, auth_url, region_name=None):
		self.user = user
		self.password = password
		self.tenant = tenant
		self.auth_url = auth_url
		self.region_name = region_name
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
                                    auth_url=self.auth_url,
                                    region_name=self.region_name,
                                    **kwargs)


class NovaWrapper(OpenstackServiceWrapper):

    def _make_connection(self, service_type='compute', **kwargs):
        return nova_client.Client(self.user,
                                  self.password,
                                  self.tenant,
                                  auth_url=self.auth_url,
                                  region_name=self.region_name,
                                  service_type=service_type,
                                  **kwargs)


def net_interfaces():
	# http://code.activestate.com/recipes/439093-get-names-of-all-up-network-interfaces-linux-only/#c7
    is_64bits = sys.maxsize > 2**32
    struct_size = 40 if is_64bits else 32
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    max_possible = 8 # initial value
    while True:
        num_bytes = max_possible * struct_size
        names = array.array('B', '\0' * num_bytes)
        outbytes = struct.unpack('iL', fcntl.ioctl(
            s.fileno(),
            0x8912,  # SIOCGIFCONF
            struct.pack('iL', num_bytes, names.buffer_info()[0])
        ))[0]
        if outbytes == num_bytes:
            max_possible *= 2
        else:
            break
    namestr = names.tostring()
    return [(namestr[i:i+16].split('\0', 1)[0],
            socket.inet_ntoa(namestr[i+20:i+24]))
            for i in range(0, outbytes, struct_size)]


class OpenstackPlatform(Platform):

    _meta_url = "http://169.254.169.254/openstack/latest/meta_data.json"
    _metadata = {}
    _userdata = None

    _private_ip = None
    _public_ip = None

    features = ['volumes', 'snapshots']


    def get_private_ip(self):
        if not self._private_ip:
            ifaces = net_interfaces()
            self._private_ip = ifaces['eth1' if 'eth1' in ifaces else 'eth0']
        return self._private_ip

    def get_public_ip(self):
        if not self._public_ip:
            ifaces = net_interfaces()
            self._public_ip =  ifaces['eth0']
        return self._public_ip


    def _get_property(self, name):
        if not name in self._userdata:
            self.get_user_data()
        return self._userdata[name]

    def get_server_id(self):
        nova = self.new_nova_connection()
        nova.connect()
        servers = nova.servers.list()
        for srv in servers:
            srv_private_addrs = map(lambda addr_info: addr_info['addr'],
                                    srv.addresses['private'])
            if self.get_private_ip() in srv_private_addrs:
                return srv.id

    def get_avail_zone(self):
        return self._get_property('availability_zone')

    def get_ssh_pub_key(self):
        return self._get_property('public_keys')  # TODO: take one key

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
                rawmeta = None
                with open(path, 'r') as fp:
                    rawmeta = fp.read()
                if not rawmeta:
                    raise PlatformError("Empty user-data")
                return self._parse_user_data(rawmeta)
        return self._userdata

    def set_access_data(self, access_data):
        self._access_data = access_data
        # if it's Rackspace NG, we need to set env var CINDER_RAX_AUTH
        # for proper nova and cinder authentication
        if 'rackspacecloud' in self._access_data["keystone_url"]:
            os.environ["CINDER_RAX_AUTH"] = "True"
            os.environ["NOVA_RAX_AUTH"] = "True"

    def new_cinder_connection(self):
        if not self._access_data:
            return None
        api_key = self._access_data["api_key"]
        password = self._access_data["password"]
        return CinderWrapper(self._access_data["username"],
                             password or api_key,
                             self._access_data["tenant_name"],
                             self._access_data["keystone_url"],
                             self._access_data["cloud_location"])

    def new_nova_connection(self):
        if not self._access_data:
            return None
        api_key = self._access_data["api_key"]
        password = self._access_data["password"]
        return NovaWrapper(self._access_data["username"],
                           password or api_key,
                           self._access_data["tenant_name"],
                           self._access_data["keystone_url"],
                           self._access_data["cloud_location"])


def get_platform():
    return OpenstackPlatform()
