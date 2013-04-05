from __future__ import with_statement

import os

from scalarizr import rpc
from scalarizr.bus import bus
import scalarizr.libs.metaconf as metaconf
from scalarizr.config import BuiltinBehaviours

CNF_SECTION = BuiltinBehaviours.WWW
APP_INC_PATH = 'app_include_path'


class NginxAPI(object):

    def _get_role_servers(self, role_id):
        """ Method is used to get role servers from scalr """
        queryenv = bus.queryenv_service
        roles = queryenv.list_roles(farm_role_id=role_id)
        servers = []
        for role in roles:
            ips = [host.external_ip for host in role.hosts]  # TODO: decide when to take external, and when internal ip?
            servers.extend(ips)
        return servers

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
        """Returns config for one upstream server"""
        config = metaconf.Configuration('nginx')
        config.add('upstream', name or 'backend')
        if ip_hash:
            config.add('upstream/iphash', '')

        for dest in destinations:
            for i, server in enumerate(dest['servers']):
                if 'port' in dest or port:
                    server = '%s:%s' % (server, dest.get('port', port))

                if 'backup' in dest:
                    server = '%s %s' % (server, 'backup')

                _max_fails = dest.get('max_fails', max_fails)
                if _max_fails:
                    server = '%s %s' % (server, 'max_fails=%i' % _max_fails)

                _fail_timeout = dest.get('fail_timeout', fail_timeout)
                if _fail_timeout:
                    server = '%s %s' % (server, 'fail_timeout=%is' % _fail_timeout)

                if 'down' in dest:
                    server = '%s %s' % (server, 'down')

                config.add('upstream/server', server)

        return config

    def _group_destinations(self, destinations):
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

    def extend_app_servers_config(self,
                                  addr,
                                  grouped_destinations,
                                  port=None,
                                  ip_hash=True,
                                  max_fails=None,
                                  fail_timeout=None):
        ini = bus.cnf.rawini
        config_dir = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH))
        config_path = os.path.join(config_dir, 'app-servers.include')
        config = metaconf.Configuration('nginx')

        config.read(config_path)

        # using tuple instead of dict because we need to keep order saved
        locations_and_backends = ()
        # making backend configs for each group
        for backend_destinations in grouped_destinations:
            # TODO: delete backends from initial config, that have similar name as new
            location = grouped_destinations[0]['location']
            if location.startswith('/'):
                location = location[1:]
            name = '%s_%s' % (addr, location.replace('/', '_'))

            backend = self._make_backend_conf(name,
                                              backend_destinations,
                                              port=port,
                                              ip_hash=ip_hash,
                                              max_fails=max_fails,
                                              fail_timeout=fail_timeout)
            config.extend(backend)

            locations_and_backends += ((location, name),)

        config.write(config_path)

        return locations_and_backends

    def _get_ssl_cert(self, ssl_certificate_id):
        """
        Gets ssl certificate and key from Scalr, writes them to files and
        returns paths to files.
        """
        # TODO: finish method
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

    def _make_server_conf(self,
                            addr,
                            locations_and_backends,
                            port=None,
                            ssl=False,
                            ssl_port=None,
                            ssl_certificate_id=None):
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

    def extend_https_config(self,
                            addr,
                            locations_and_backends,
                            port=None,
                            ssl=False,
                            ssl_port=None,
                            ssl_certificate_id=None):
        # TODO: Check this method
        ini = bus.cnf.rawini
        config_dir = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH))
        config_path = os.path.join(config_dir, 'https.include')
        config = metaconf.Configuration('nginx')

        config.read(config_path)
        
        # server_xpath = 'server[%i]' % len(config.get_list('server'))
        server_config = self._make_server_conf(addr,
                                               locations_and_backends,
                                               port,
                                               ssl,
                                               ssl_port,
                                               ssl_certificate_id)
        config.extend(server_config)
        

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
        # TODO: write tests!
        destinations = self._parse_roles(roles)
        destinations.extend(self._parse_servers(servers))

        grouped_destinations = self._group_destinations(destinations)

        locations_and_backends = self.extend_app_servers_config(addr,
                                                                grouped_destinations,
                                                                port=backend_port,
                                                                ip_hash=backend_ip_hash,
                                                                max_fails=backend_max_fails,
                                                                fail_timeout=backend_fail_timeout)
        self.extend_https_config(addr,
                                 locations_and_backends,
                                 port=port,
                                 ssl=ssl,
                                 ssl_port=ssl_port,
                                 ssl_certificate_id=ssl_certificate_id)

    def add_backend(conf, backend_name, **kwds):
        pass
