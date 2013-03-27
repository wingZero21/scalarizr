from __future__ import with_statement

import os

from scalarizr import rpc
from scalarizr.bus import bus
import scalarizr.libs.metaconf as metaconf
from scalarizr.config import BuiltinBehaviours

CNF_SECTION = BuiltinBehaviours.WWW
APP_INC_PATH = 'app_include_path'


class NginxAPI(object):

    def _get_role_servers(self, role_id):  # Maybe move this method to other class/place
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
            role['servers'] = self.get_role_servers(role['id'])
            destinations.append(role)

        return destinations

    def parse_servers(self, servers):
        destinations = []
        for server in servers:
            pass  # TODO:
        return destinations

    def get_role_servers(self, role_id):
        # TODO:
        return ['tst']

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

    def extend_app_servers_config(self, addr, destinations):
        ini = bus.cnf.rawini
        config_dir = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH))
        config_path = os.path.join(config_dir, 'app-servers.include')
        config = metaconf.Configuration('nginx')

        config.read(config_path)
        sorted_destinations = sorted(destinations,
                                     key=lambda x: x.get('location'),
                                     reverse=True)

        # Making backend dicts from destinations with similar location
        grouped_destinations = [sorted_destinations[0:1]]
        if not grouped_destinations[0][0].get('location'):
            grouped_destinations[0][0]['location'] = '/'
        for dest in sorted_destinations[1:]:
            if not dest.get('location'):
                dest['location'] = '/'
            if grouped_destinations[-1][0]['location'] == dest['location']:
                grouped_destinations[-1].append(dest)
            else:
                grouped_destinations.append([dest])

        for backend_destinations in grouped_destinations:
            # TODO: delete backends from initial config, that have similar name as new
            backend = self._make_backend_conf('%s_%s' % (addr, grouped_destinations[0]['location']), backend_destinations)
            config.extend(backend)


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
        
        destinations = self._parse_roles(roles)
        destinations.extend(self._parse_servers(servers))

        self.extend_app_servers_config(destinations)
        self.extend_https_config(addr, destinations)  # TODO: add other params
