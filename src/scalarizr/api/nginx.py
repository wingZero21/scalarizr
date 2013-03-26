from __future__ import with_statement

import os

from scalarizr import rpc
from scalarizr.bus import bus
import scalarizr.libs.metaconf as metaconf
from scalarizr.config import BuiltinBehaviours

CNF_SECTION = BuiltinBehaviours.WWW
APP_INC_PATH = 'app_include_path'


class DestinationType(object):
    """DestinationType is enum-like class for destinations types"""
    ROLE = 1
    SERVER = 2
        

class Destination(object):
    """Destination objects are used to form backends objects"""
    def __init__(self,
                 id_=None,
                 type_=None,
                 location=None,
                 backup=False,
                 port=None,
                 max_fails=None,
                 fail_timeout=None
                 down=False):
        self.id_ = id_  # role id or server ip
        self.type_ = type_
        self.location = location
        self.backup = backup
        self.port = port
        self.max_fails = max_fails
        self.fail_timeout = fail_timeout
        self.down = down

    @classmethod
    def from_dict(cls, dict_):
        dest = Destination()
        if 'id' in dict_:
            dest.id_ = dict_['id']
            dest.type_ = DestinationType.ROLE
        elif 'host' in dict_:
            dest.id_ = dict_['host']
            dest.type_ = DestinationType.SERVER
        else:
            raise BaseException('Unknown type of destination')

        for k, v in dict_:
            setattr(dest, k, v)

        return dest


class Backend(object):

    def __init__(self, name=None, destinations=None):
        self.name = name
        self.destinations = destinations

    def get_role_servers(self, role_id):  # Maybe move this method to other class/place
        """ Method is used to get role servers from scalr"""
        # TODO: finish this method
        return []

    def get_backend_conf(self):
        """Returns config for one upstream server"""
        config = metaconf.Configuration('nginx')
        config.add('upstream', self.name or 'backend')
        config.add('upstream/iphash', '')  # Is it really needed?

        for dest in self.destinations:
            dest_servers = []
            if dest.type_ == DestinationType.ROLE:
                dest_servers = self.get_role_servers(dest.id_)
            elif dest.type_ == DestinationType.SERVER:
                dest_servers = [dest.id_]
            # TODO: finish method


class NginxAPI(object):

    def parse_roles(self, roles):
        destinations = []
        for role in roles:
            if type(role) is int:
                role = {'id': role}
            elif type(role) is str:
                role = {'id': int(role)}
            # assuming that role is dict by default
            destinations.append(Destination.from_dict(role))

        return destinations

    def parse_servers(self, servers):
        destinations = []
        for server in servers:
            pass  # TODO:
        return destinations

    def get_role_servers(self, role_id):
        # TODO:
        return ['tst']

    def extend_app_servers_config(self, destinations):
        ini = bus.cnf.rawini
        config_dir = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH))
        config_path = os.path.join(config_dir, 'app-servers.include')
        config = metaconf.Configuration('nginx')

        config.read(config_path)
        sorted_destinations = sorted(destinations,
                                     key=lambda x: x.location,
                                     reverse=True)
        for dest in sorted_destinations:
            dest ################


    def extend_https_config(self, addr, destinations):
        ini = bus.cnf.rawini
        config_dir = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH))
        config_path = os.path.join(config_dir, 'https.include')
        config = metaconf.Configuration('nginx')

        config.read(config_path)
        server_xpath = 'server[%i]' % len(config.get_list('server'))
        config.add('server', '')
        config.add('%s/listen' % server_xpath, '80')  # TODO: we can have a custom port to listen
        config.add('%s/server_name' % server_xpath, addr)
        # TODO: add some typical server info MORE

        # TODO: for each destination create location block
        for i, dest in enumerate(destinations):
            location_xpath = '%s/location' % server_xpath
            config.add(location_xpath, dest.location)

            location_xpath = '%s[%i]' % (location_xpath, i + 1)
            config.add('%s/proxy_pass' % location_xpath, 'http://%s' % dest.id_)
            config.add('%s/proxy_set_header' % location_xpath, 'Host $host')
            config.add('%s/proxy_set_header' % location_xpath, 'X-Real-IP $remote_addr')
            config.add('%s/proxy_set_header' % location_xpath, 'Host $host')
            config.add('%s/client_max_body_size' % location_xpath, '10m')
            config.add('%s/client_body_buffer_size' % location_xpath, '128k')
            config.add('%s/proxy_buffering' % location_xpath, 'on')
            config.add('%s/proxy_connect_timeout' % location_xpath, '15')
            config.add('%s/proxy_intercept_errors' % location_xpath, 'on')

    @rpc.service_method
    def add_proxy(self, 
        addr,
        roles=[],
        servers=[],
        port=None,
        ssl=False,
        ssl_port=None,
        ssl_certificate_id=None,
        backend_port=None,
        backend_ip_hash=False,
        backend_max_fails=None,
        backend_fail_timeout=None):
        
        destinations = self.parse_roles(roles)
        destinations.extend(self.parse_servers(servers))

        self.extend_app_servers_config(destinations)
        self.extend_https_config(addr, destinations)  # TODO: add other params
