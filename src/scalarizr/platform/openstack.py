import urllib2
import json
import os
import logging
import re
from time import sleep


from cinderclient.v1 import client as cinder_client
from novaclient.v1_1 import client as nova_client
import swiftclient


from scalarizr import platform
from scalarizr.bus import bus
from scalarizr.storage.transfer import Transfer, TransferProvider
from scalarizr.storage2.cloudfs import swift as swiftcloudfs


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


class OpenstackPlatform(platform.Platform):

    _meta_url = "http://169.254.169.254/openstack/latest/meta_data.json"
    _metadata = {}
    _userdata = None

    _private_ip = None
    _public_ip = None

    features = ['volumes', 'snapshots']

    def __init__(self):
        platform.Platform.__init__(self)
        # Work over [Errno -3] Temporary failure in name resolution
        # http://bugs.centos.org/view.php?id=4814
        os.chmod('/etc/resolv.conf', 0755)

    def get_private_ip(self):
        if self._private_ip is None:
            ifaces = platform.net_interfaces()
            self._private_ip = ifaces['eth1' if 'eth1' in ifaces else 'eth0']
        return self._private_ip

    def get_public_ip(self):
        if self._public_ip is None:
            ifaces = platform.net_interfaces()
            self._public_ip = ifaces['eth0'] if 'eth1' in ifaces and 'eth0' in ifaces else ''
        return self._public_ip

    def _get_property(self, name):
        if not name in self._userdata:
            self.get_user_data()
        return self._userdata[name]

    def get_server_id(self):
        nova = self.new_nova_connection()
        nova.connect()
        servers = nova.servers.list()
        my_private_ip = self.get_private_ip()
        for server in servers:
            private_ip = 'private' in server.addresses and server.addresses['private']['addr']
            if not private_ip:
                ips = [address['addr'] 
                        for network in server.addresses.values()
                        for address in network
                        if address['addr'].startswith('10.')]
                if ips:
                    private_ip = ips[0]

            if my_private_ip == private_ip:
                return server.id

        raise BaseException("Can't get server_id because we can't get "
                            "server private ip")

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
            raise platform.PlatformError("Cannot fetch %s metadata url '%s'. "
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
                    raise platform.PlatformError("Empty user-data")
                return self._parse_user_data(rawmeta)
        return self._userdata

    def set_access_data(self, access_data):
        self._access_data = access_data
        # if it's Rackspace NG, we need to set env var CINDER_RAX_AUTH
        # and NOVA_RAX_AUTH for proper nova and cinder authentication
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

    def new_swift_connection(self):
        if not self._access_data:
            return None
        api_key = self._access_data["api_key"]
        password = self._access_data["password"]
        keystone_url = self._access_data["keystone_url"]
        kwds = {}
        if 'rackspacecloud' in self._access_data["keystone_url"]:
            keystone_url = re.sub(r'v2\.\d$', 'v1.0',
                            self._access_data['keystone_url'])
            kwds['auth_version'] = '1'
        else:
            kwds['auth_version'] = '2'
            kwds['tenant_name'] = self._access_data["tenant_name"]

        return swiftclient.Connection(keystone_url, 
                    self._access_data["username"],
                    password or api_key,
                    **kwds)


def get_platform():
    # Filter keystoneclient* and swiftclient* log messages
    class FalseFilter:
        def filter(self, record):
            return False
    for cat in ('keystoneclient', 'swiftclient'):
        log = logging.getLogger(cat)
        log.addFilter(FalseFilter())

    return OpenstackPlatform()


class SwiftTransferProvider(TransferProvider):
    schema = 'swift'
    
    _logger = None
    
    def __init__(self):
        self._logger = logging.getLogger(__name__)
        self._driver = swiftcloudfs.SwiftFileSystem()  
        TransferProvider.__init__(self)   

    def put(self, local_path, remote_path):
        self._logger.info('Uploading %s to Swift under %s' % (local_path, remote_path))
        return self._driver.put(local_path, os.path.join(remote_path, os.path.basename(local_path)))
    
    def get(self, remote_path, local_path):
        self._logger.info('Downloading %s from Swift to %s' % (remote_path, local_path))
        return self._driver.get(remote_path, local_path)
        
    
    def list(self, remote_path):
        return self._driver.ls(remote_path)


Transfer.explore_provider(SwiftTransferProvider)