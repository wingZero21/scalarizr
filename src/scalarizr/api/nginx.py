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
            if type(role) is int:
                role = {'id': role}
            elif type(role) is str:
                role = {'id': int(role)}
            # assuming that role is dict by default
            role['servers'] = self._get_role_servers(role['id'])
            destinations.append(role)

        return destinations

    def _parse_servers(self, servers):
        destinations = []
        for server in servers:
            if type(server) is str:
                server = {'servers': [server]}
            # assuming that server is dict by default
            server['servers'] = [server['host']]
            destinations.append(server)

        return destinations

    def _make_backend_conf(self, name, destinations):
        """Returns config for one upstream server"""
        config = metaconf.Configuration('nginx')
        config.add('upstream', self.name or 'backend')
        config.add('upstream/iphash', '')  # Is it really needed?

        for dest in self.destinations:
            for server in dest['servers']:
                if dest.get('port'):
                    server = '%s:%s' % (server, dest['port'])
                config.add('upstream/server', server)

                if dest.get('backup'):
                    config.add('upstream/server', 'backup')
                if dest.get('max_fails'):
                    config.add('upstream/server', 'max_fails=%i' % dest['max_fails'])
                if dest.get('fail_timeout'):
                    config.add('upstream/server', 'fail_timeout=%is' % dest['fail_timeout'])
                if dest.get('down'):
                    config.add('upstream/server', 'down')

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

    def extend_app_servers_config(self, addr, grouped_destinations):
        ini = bus.cnf.rawini
        config_dir = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH))
        config_path = os.path.join(config_dir, 'app-servers.include')
        config = metaconf.Configuration('nginx')

        config.read(config_path)

        # using tuple instead of dict because we need to keep order saved
        location_and_backend_name = ()
        # making backend configs for each group
        for backend_destinations in grouped_destinations:
            # TODO: delete backends from initial config, that have similar name as new
            location = grouped_destinations[0]['location']
            name = '%s_%s' % (addr, location)  # TODO: validate name

            backend = self._make_backend_conf(name, backend_destinations)
            config.extend(backend)

            location_and_backend_name = (location, name)

        config.write(config_path)

        return location_and_backend_name

    def extend_https_config(self, addr, location_and_backend_name):
        ini = bus.cnf.rawini
        config_dir = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH))
        config_path = os.path.join(config_dir, 'https.include')
        config = metaconf.Configuration('nginx')

        config.read(config_path)
        server_xpath = 'server[%i]' % len(config.get_list('server'))
        config.add('server', '')
        config.add('%s/listen' % server_xpath, '80')
        config.add('%s/listen' % server_xpath, '443')  # TODO: we can have a custom port to listen
        config.add('%s/server_name' % server_xpath, addr)
        # TODO: add some typical server info MORE

        # TODO: for each destination create location block
        for i, (location, backend_name) in location_and_backend_name:
            location_xpath = '%s/location' % server_xpath
            config.add(location_xpath, location)

            location_xpath = '%s[%i]' % (location_xpath, i + 1)
            config.add('%s/proxy_pass' % location_xpath, 'http://%s' % backend_name)  # TODO: take backend from somewhere instead of dest.id_
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
        
        destinations = self._parse_roles(roles)
        destinations.extend(self._parse_servers(servers))
        grouped_destinations = self._group_destinations(destinations)
        location_and_backend_name = self.extend_app_servers_config(grouped_destinations)
        self.extend_https_config(addr, location_and_backend_name)  # TODO: add other params
