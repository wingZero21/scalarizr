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
        """ Method is used to get role servers from scalr"""
        # TODO: finish this method
        return []

    def _parse_roles(self, roles):
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
        destinations = []
        for server in servers:
            if type(server) is str:
                server = {'servers': [server]}
            else:
                # assuming that server is dict by default
                server['servers'] = [server['host']]
            destinations.append(server)

        return destinations

    def _make_backend_conf(self, name, destinations, port='80', ip_hash=True):
        """Returns config for one upstream server"""
        config = metaconf.Configuration('nginx')
        config.add('upstream', name or 'backend')
        if ip_hash:
            config.add('upstream/iphash', '')

        for dest in destinations:
            for i, server in enumerate(dest['servers']):
                if 'port' in dest:
                    server = '%s:%s' % (server, dest['port'])  # TODO: conflict with port param
                if 'backup' in dest:
                    server = '%s %s' % (server, 'backup')
                if 'max_fails' in dest:
                    server = '%s %s' % (server, 'max_fails=%i' % dest['max_fails'])
                if 'fail_timeout' in dest:
                    server = '%s %s' % (server, 'fail_timeout=%is' % dest['fail_timeout'])
                if 'down' in dest:
                    server = '%s %s' % (server, 'down')

                config.add('upstream/server', server)

        return config

    def add_backend(conf, backend_name, **kwds):
        pass

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
                                  port='80',
                                  ip_hash=True):
        ini = bus.cnf.rawini
        config_dir = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH))
        config_path = os.path.join(config_dir, 'app-servers.include')
        config = metaconf.Configuration('nginx')

        config.read(config_path)

        # using tuple instead of dict because we need to keep order saved
        locations_and_backend_names = ()
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
                                              ip_hash=ip_hash)
            config.extend(backend)

            locations_and_backend_names += ((location, name),)

        config.write(config_path)

        return locations_and_backend_names

    def get_ssl_cert(self, ssl_certificate_id):
        # TODO: finish method
        return ('1', '2')

    def extend_https_config(self,
                            addr,
                            locations_and_backend_names,
                            port=None,
                            ssl=False,
                            ssl_port=None,
                            ssl_certificate_id=None):
        # TODO: Check and finish this method
        ini = bus.cnf.rawini
        config_dir = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH))
        config_path = os.path.join(config_dir, 'https.include')
        config = metaconf.Configuration('nginx')

        config.read(config_path)
        server_xpath = 'server[%i]' % len(config.get_list('server'))
        config.add('server', '')
        config.add('%s/listen' % server_xpath, port or '80')
        if ssl:
            config.add('%s/listen' % server_xpath, ssl_port or '443')
            config.add('%s/ssl' % server_xpath, 'on')
            # TODO: add ssl_certificate_id
            ssl_cert_path, ssl_cert_key_path = self.get_ssl_cert(ssl_certificate_id)
            config.add('%s/ssl_certificate' % server_xpath, ssl_cert_path)
            config.add('%s/ssl_certificate_key' % server_xpath, ssl_cert_key_path)

        config.add('%s/server_name' % server_xpath, addr)

        for i, (location, backend_name) in locations_and_backend_names:
            location_xpath = '%s/location' % server_xpath
            config.add(location_xpath, location)

            location_xpath = '%s[%i]' % (location_xpath, i + 1)
            config.add('%s/proxy_pass' % location_xpath, 'http://%s' % backend_name)
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
                  backend_max_fails=None,  # is this needed?
                  backend_fail_timeout=None):  # is this needed?
        # TODO: write tests!
        destinations = self._parse_roles(roles)
        destinations.extend(self._parse_servers(servers))

        grouped_destinations = self._group_destinations(destinations)

        locations_and_backend_names = self.extend_app_servers_config(addr,
                                                                     grouped_destinations,
                                                                     port=backend_port,
                                                                     ip_hash=backend_ip_hash)
        self.extend_https_config(addr,
                                 locations_and_backend_names,
                                 port=port,
                                 ssl=ssl,
                                 ssl_port=ssl_port,
                                 ssl_certificate_id=ssl_certificate_id)
