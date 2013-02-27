import urllib2
import json
import os
import logging
import re
from time import sleep
import urlparse
import sys
import socket

from cinderclient.v1 import client as cinder_client
from novaclient.v1_1 import client as nova_client
import swiftclient

from scalarizr import platform
from scalarizr.bus import bus
from scalarizr.storage.transfer import Transfer, TransferProvider, TransferError
from scalarizr.node import __node__


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
            self._public_ip = ifaces['eth0'] if 'eth1' in ifaces else ''
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
            srv_private_addrs = []
            for _ in xrange(20):
                # if for some reason nova returns server without private ip
                # waiting for 1 sec than try again.
                # If after 20 tries still no ip, give up and try another srv
                try:
                    srv_private_addrs = [addr_info['addr'] for addr_info in
                                         srv.addresses['private']]
                    break
                except KeyError:
                    sleep(1)
                    srv.update()

            if self.get_private_ip() in srv_private_addrs:
                return srv.id

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
    return OpenstackPlatform()


class SwiftTransferProvider(TransferProvider):
    schema = 'swift'
    urlparse.uses_netloc.append(schema)
    
    _username = None
    _api_key = None
    
    _logger = None
    _container = None
    
    def __init__(self):
        self._logger = logging.getLogger(__name__)      

    def put(self, local_path, remote_path):
        self._logger.info('Uploading %s to Swift under %s' % (local_path, remote_path))
        container, obj = self._parse_path(remote_path)
        obj = os.path.join(obj, os.path.basename(local_path))
        
        try:
            connection = self._get_connection()
            
            if not self._container_check_cache(container):
                try:
                    ct = connection.get_container(container)
                except swiftclient.ClientException:
                    self._logger.debug('Container %s not found. Trying to create.', container)
                    ct = connection.create_container(container)
                # Cache container object
                self._container = ct
                
            o = self._container.create_object(obj)
            o.load_from_filename(local_path)
            return self._format_path(container, obj)            
            
        except (swiftclient.ClientException, OSError, Exception, socket.timeout):
            exc = sys.exc_info()
            raise TransferError, exc[1], exc[2]
    
    def get(self, remote_path, local_path):
        self._logger.info('Downloading %s from Swift to %s' % (remote_path, local_path))
        container, obj = self._parse_path(remote_path)
        dest_path = os.path.join(local_path, os.path.basename(remote_path))
        
        try:
            connection = self._get_connection()
            
            if not self._container_check_cache(container):
                try:
                    ct = connection.get_container(container)
                except swiftclient.ClientException:
                    raise TransferError("Container '%s' not found" % container)
                # Cache container object
                self._container = ct                
            
            try:
                o = self._container.get_object(obj)
            except swiftclient.ClientException:
                raise TransferError("Object '%s' not found in container '%s'" 
                        % (obj, container))
            
            o.save_to_filename(dest_path)
            return dest_path            
            
        except (swiftclient.ClientException, OSError, Exception):
            exc = sys.exc_info()
            raise TransferError, exc[1], exc[2]

    
    def configure(self, remote_path, username=None, api_key=None):
        if username:
            self._username = username
            self._api_key = api_key
        
    
    def list(self, remote_path):
        container, obj = self._parse_path(remote_path)
        connection = self._get_connection()
        ct = connection.get_container(container)
        objects = container.get_objects(path=obj)
        return tuple([self._format_path(ct, obj.name) for obj in objects]) if objects else ()   

    def _get_connection(self):
        return __node__['openstack']['new_swift_connection']

    def _container_check_cache(self, container):
        if self._container and self._container.name != container:
            self._container = None
        return self._container

    def _format_path(self, container, obj):
        return '%s://%s/%s' % (self.schema, container, obj)
    
    def _parse_path(self, path):
        o = urlparse.urlparse(path)
        if o.scheme != self.schema:
            raise TransferError('Wrong schema')
        return o.hostname, o.path[1:]


Transfer.explore_provider(SwiftTransferProvider)