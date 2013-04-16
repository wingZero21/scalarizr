from __future__ import with_statement

import os
from fnmatch import fnmatch

from scalarizr import rpc
from scalarizr.bus import bus
import scalarizr.libs.metaconf as metaconf
from scalarizr.node import __node__
from scalarizr.handlers.nginx import NginxInitScript


APP_INC_PATH = 'app_include_path'
HTTPS_INC_PATH = 'https_include_path'


__nginx__ = __node__['nginx']


class NginxAPI(object):

    # TODO: move next method to Configuration class
    def _find_xpath(self, conf, element_xpath, value):
        '''
        Finds xpath of certain element by given value.
        Use this method when you need to find certain element in list of 
        elements with same name. Example:
        config contents:
        ``server 12.23.34.45;``
        ``server 10.10.12.11 backend;``
        ``server 10.10.12.12 backend;``

        ``api._find_xpath(conf, 'server', '12.23.34.45')`` will find first
        element (its xpath will be 'server[1]').

        Wildcards can be used:
        ``api._find_xpath(conf, 'server', '10.10.12.11*')`` will find second
        element ('server[2]')
        '''
        for i, _ in enumerate(conf.get_list(element_xpath)):
            xpath = '%s[%i]' % (element_xpath, i + 1)
            if fnmatch(conf.get(xpath), value):
                return xpath
        return None

    def __init__(self, app_inc_dir=None, https_inc_dir=None):
        self.service = NginxInitScript()

        if not app_inc_dir:
            app_inc_dir = os.path.dirname(__nginx__[APP_INC_PATH])
        self.app_inc_path = os.path.join(app_inc_dir, 'app-servers.include')
        self.app_servers_inc = metaconf.Configuration('nginx')
        self.app_servers_inc.read(self.app_inc_path)

        if not https_inc_dir:
            https_inc_dir = os.path.dirname(__nginx__[HTTPS_INC_PATH])
        self.https_inc_path = os.path.join(https_inc_dir, 'https.include')
        self.https_inc = metaconf.Configuration('nginx')

        self.https_inc.read(self.https_inc_path)

    def _get_role_servers(self, role_id):
        """ Method is used to get role servers from scalr """
        server_location = __node__['cloud_location']
        queryenv = bus.queryenv_service
        roles = queryenv.list_roles(farm_role_id=role_id)
        servers = []
        for role in roles:
            ips = [h.internal_ip if server_location == h.cloud_location else
                   h.external_ip
                   for h in role.hosts]
            servers.extend(ips)
        return servers

    def _get_ssl_cert(self, ssl_certificate_id):
        """
        Gets ssl certificate and key from Scalr, writes them to files and
        returns paths to files.
        """
        queryenv = bus.queryenv_service
        cert, key, cacert = queryenv.get_ssl_certificate(ssl_certificate_id)
        cert = cert + '\n' + cacert

        keys_dir_path = os.path.join(bus.etc_path, "private.d/keys")
        if not os.path.exists(keys_dir_path):
            os.mkdir(keys_dir_path)

        cert_path = os.path.join(keys_dir_path, 'https.crt')
        with open(cert_path, 'w') as fp:
            fp.write(cert)

        key_path = os.path.join(keys_dir_path, 'https.key')
        with open(key_path, 'w') as fp:
            fp.write(key)

        return (cert_path, key_path)

    def _parse_roles(self, roles):
        """
        Parses list of roles. Role can be either int (role id)
        or dictionary. Dictionary example
        .. code-block:: python
            {
            'id': 123,
            'port': '80',
            'backup': True,
            # ...
            # other backend params
            }
        Returns destination dictionaries with format like above
        plus servers list in 'servers' key.
        """
        destinations = []
        for role in roles:
            if type(role) is str:
                role = {'id': role}
            elif type(role) is int:
                role = {'id': str(role)}
            # assuming that role is dict by default
            role['servers'] = self._get_role_servers(role['id'])
            destinations.append(role)

        return destinations

    def _parse_servers(self, servers):
        """
        Parses list of servers. Servers can be either str (server ip)
        or dictionary. Dictionary example
        .. code-block:: python
            {
            'host': '10.20.30.40',
            'port': '80',
            'backup': True,
            # ...
            # other backend params
            }
        Returns destination dictionaries with format like above
        plus servers list in 'servers' key (will contain only one ip).
        """
        destinations = []
        for server in servers:
            if type(server) is str:
                server = {'servers': [server]}
            else:
                # assuming that server is dict by default
                server['servers'] = [server['host']]
            destinations.append(server)

        return destinations

    def _make_backend_conf(self,
                           name,
                           destinations,
                           port=None,
                           ip_hash=True,
                           max_fails=None,
                           fail_timeout=None):
        """Returns config for one backend server"""
        config = metaconf.Configuration('nginx')
        config.add('upstream', name or 'backend')
        if ip_hash:
            config.add('upstream/iphash', '')

        for dest in destinations:
            for i, server in enumerate(dest['servers']):
                if 'port' in dest or port:
                    server = '%s:%s' % (server, dest.get('port', port))

                if 'backup' in dest and dest['backup']:
                    server = '%s %s' % (server, 'backup')

                _max_fails = dest.get('max_fails', max_fails)
                if _max_fails:
                    server = '%s %s' % (server, 'max_fails=%i' % _max_fails)

                _fail_timeout = dest.get('fail_timeout', fail_timeout)
                if _fail_timeout:
                    server = '%s %s' % (server, 'fail_timeout=%is' % _fail_timeout)

                if 'down' in dest and dest['down']:
                    server = '%s %s' % (server, 'down')

                config.add('upstream/server', server)

        return config

    def _group_destinations(self, destinations):
        '''
        Groups destinations by location in list of lists.
        If no location defined assumes that it'r '/' location.
        '''
        sorted_destinations = sorted(destinations,
                                     key=lambda x: x.get('location'),
                                     reverse=True)

        # Making backend dicts from destinations with similar location
        first_dest = sorted_destinations[0]
        if not first_dest.get('location'):
            first_dest['location'] = '/'
        grouped_destinations = [[first_dest]]
        # Grouping destinations with similar location
        for dest in sorted_destinations[1:]:
            if not dest.get('location'):
                dest['location'] = '/'
            if grouped_destinations[-1][0]['location'] == dest['location']:
                grouped_destinations[-1].append(dest)
            else:
                grouped_destinations.append([dest])

        return grouped_destinations

    def _add_backend(self,
                     name,
                     destinations,
                     port=None,
                     ip_hash=True,
                     max_fails=None,
                     fail_timeout=None):
        '''
        Adds backend to app-servers config, but without writing it to file.
        '''
        backend = self._make_backend_conf(name,
                                          destinations,
                                          port=port,
                                          ip_hash=ip_hash,
                                          max_fails=max_fails,
                                          fail_timeout=fail_timeout)
        self.app_servers_inc.extend(backend)

    def _add_backends(self,
                      addr,
                      grouped_destinations,
                      port=None,
                      ip_hash=True,
                      max_fails=None,
                      fail_timeout=None):
        '''
        Makes backend for each group of destinations and writes it to
        app-servers config file.
        Returns tuple of pairs with location and backend names:
        [[dest1, dest2], [dest3]] -> ((location1, name1), (location2, name2))

        Tuple of pairs is used instead of dict, because we need to keep order 
        saved.
        '''
        locations_and_backends = ()
        # making backend configs for each group
        for backend_destinations in grouped_destinations:
            # TODO: delete backends from initial config, that have similar name as new
            location = backend_destinations[0]['location']

            # if location.startswith('/'):
            #     location_name = location[1:]
            name = '%s%s' % (addr, location.replace('/', '_'))
            if name.endswith('_'):
                name = name[:-1]

            self._add_backend(name,
                              backend_destinations,
                              port=port,
                              ip_hash=ip_hash,
                              max_fails=max_fails,
                              fail_timeout=fail_timeout)

            locations_and_backends += ((location, name),)

        return locations_and_backends

    def _make_server_conf(self,
                          addr,
                          locations_and_backends,
                          port=None,
                          ssl=False,
                          ssl_port=None,
                          ssl_certificate_id=None):
        '''
        Makes config (metaconf.Configuration object)
        '''
        config = metaconf.Configuration('nginx')
        server_xpath = 'server'
        config.add('server', '')
        config.add('%s/listen' % server_xpath, port or '80')

        if ssl:
            config.add('%s/listen' % server_xpath, ssl_port or '443')
            config.add('%s/ssl' % server_xpath, 'on')
            ssl_cert_path, ssl_cert_key_path = self._get_ssl_cert(ssl_certificate_id)
            config.add('%s/ssl_certificate' % server_xpath, ssl_cert_path)
            config.add('%s/ssl_certificate_key' % server_xpath, ssl_cert_key_path)

            # TODO: move next hardcoded strings to some constants
            config.add('%s/ssl_session_timeout' % server_xpath, '10m')
            config.add('%s/ssl_session_cache' % server_xpath, 'shared:SSL:10m')
            config.add('%s/ssl_protocols' % server_xpath, 'SSLv2 SSLv3 TLSv1')
            config.add('%s/ssl_ciphers' % server_xpath, 'ALL:!ADH:!EXPORT56:RC4+RSA:+HIGH:+MEDIUM:+LOW:+SSLv2:+EXP')
            config.add('%s/ssl_prefer_server_ciphers' % server_xpath, 'on')

        config.add('%s/server_name' % server_xpath, addr)

        for i, (location, backend_name) in enumerate(locations_and_backends):
            location_xpath = '%s/location' % server_xpath
            config.add(location_xpath, location)

            location_xpath = '%s[%i]' % (location_xpath, i + 1)
            config.add('%s/proxy_pass' % location_xpath, 'http://%s' % backend_name)
            # TODO: move next hardcoded strings to some constants
            config.add('%s/proxy_set_header' % location_xpath, 'Host $host')
            config.add('%s/proxy_set_header' % location_xpath, 'X-Real-IP $remote_addr')
            config.add('%s/proxy_set_header' % location_xpath, 'Host $host')
            config.add('%s/client_max_body_size' % location_xpath, '10m')
            config.add('%s/client_body_buffer_size' % location_xpath, '128k')
            config.add('%s/proxy_buffering' % location_xpath, 'on')
            config.add('%s/proxy_connect_timeout' % location_xpath, '15')
            config.add('%s/proxy_intercept_errors' % location_xpath, 'on')

        return config

    def _add_server(self,
                    addr,
                    locations_and_backends,
                    port=None,
                    ssl=False,
                    ssl_port=None,
                    ssl_certificate_id=None):
        '''
        Adds server to https config, but without writing it to file.
        '''
        server_config = self._make_server_conf(addr,
                                               locations_and_backends,
                                               port,
                                               ssl,
                                               ssl_port,
                                               ssl_certificate_id)
        self.https_inc.extend(server_config)
        

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
                  backend_fail_timeout=None,
                  reread_conf=True):
        '''
        Adds proxy
        '''
        # TODO: write tests
        destinations = self._parse_roles(roles)
        destinations.extend(self._parse_servers(servers))

        grouped_destinations = self._group_destinations(destinations)

        if reread_conf:
            self.app_servers_inc.read(self.app_inc_path)
            self.https_inc.read(self.https_inc_path)

        locations_and_backends = self._add_backends(addr,
                                                    grouped_destinations,
                                                    port=backend_port,
                                                    ip_hash=backend_ip_hash,
                                                    max_fails=backend_max_fails,
                                                    fail_timeout=backend_fail_timeout)
        self._add_server(addr,
                         locations_and_backends,
                         port=port,
                         ssl=ssl,
                         ssl_port=ssl_port,
                         ssl_certificate_id=ssl_certificate_id)

        self.app_servers_inc.write(self.app_inc_path)
        self.https_inc.write(self.https_inc_path)

        self.service.restart()

    def _remove_backend(self, name):
        '''
        Removes backend with given name from app-servers config.
        '''
        xpath = self._find_xpath(self.app_servers_inc, 'upstream', name)
        self.app_servers_inc.remove(xpath)

    def _remove_server(self, name):
        '''
        Removes server from https.include config. Also removes used backends.
        '''
        for i, _ in enumerate(self.https_inc.get_list('server')):

            server_xpath = 'server[%i]' % i + 1
            server_name = self.https_inc.get('%s/server_name' % server_xpath)

            if server_name == name:
                backends = self.https_inc.get_list('%s/location' % server_xpath)
                
                for backend in backends:
                    self._remove_backend(backend)

                self.https_inc.remove(server_xpath)

                break

    @rpc.service_method
    def remove_proxy(self, addr):
        '''
        Removes proxy for addr. Removes created server and its backends.
        '''
        # TODO: review method
        self.https_inc.read(self.https_inc_path)
        self.app_servers_inc.read(self.app_inc_path)

        self._remove_server(addr)

        self.https_inc.write(self.https_inc_path)
        self.app_servers_inc.write(self.app_inc_path)
        self.service.restart()

    @rpc.service_method
    def update_proxy(self, **kwds):
        '''
        Applies new configuration for existing proxy
        '''
        try:
            # try to apply changes
            addr = kwds.get('addr')
            if addr:
                self.https_inc.read(self.https_inc_path)
                self.app_servers_inc.read(self.app_inc_path)

                self.https_inc.write(self.https_inc_path + '.bak')
                self.app_servers_inc.write(self.app_inc_path + '.bak')

                self._remove_server(addr)

                self.add_proxy(reread_conf=False, **kwds)

        except:
            # undo changes
            self.https_inc.read(self.https_inc_path + '.bak')
            self.app_servers_inc.read(self.app_inc_path + '.bak')
            self.https_inc.write(self.https_inc_path)
            self.app_servers_inc.write(self.app_inc_path)

    # TODO: use this method in backend conf making or smth.
    def _host_to_str(self, host):
        if type(host) == str:
            return host

        result = host['ip']
        if 'port' in host:
            result = '%s:%s' % (result, host['port'])

        if 'backup' in host and host['backup']:
            result = '%s %s' % (result, 'backup')

        _max_fails = host.get('max_fails')
        if _max_fails:
            result = '%s %s' % (result, 'max_fails=%i' % _max_fails)

        _fail_timeout = host.get('fail_timeout')
        if _fail_timeout:
            result = '%s %s' % (result, 'fail_timeout=%is' % _fail_timeout)

        if 'down' in host:
            result = '%s %s' % (result, 'down')

        return result

    def add_host(self, backend, host):
        '''
        Adds host to backend with given name. Parameter host can be dict
        or string
        '''
        self.app_servers_inc.read(self.app_inc_path)

        xpath = self._find_xpath(self.app_servers_inc, 'upstream', backend)
        self.app_servers_inc.add('%s/server' % xpath, self._host_to_str(host))

        self.app_servers_inc.write(self.app_inc_path)
        self.service.restart()

    def remove_host(self, backend, host):
        '''
        Adds host to backend with given name. Parameter host is ip of server
        '''
        self.app_servers_inc.read(self.app_inc_path)

        backend_xpath = self._find_xpath(self.app_servers_inc,
                                         'upstream',
                                         backend)
        host_xpath = self._find_xpath(self.app_inc_path,
                                      '%s/server' % backend_xpath,
                                      host + '*')
        self.app_servers_inc.remove(host_xpath)

        self.app_servers_inc.write(self.app_inc_path)
        self.service.restart()
