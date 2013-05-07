from __future__ import with_statement

import os
from fnmatch import fnmatch
from telnetlib import Telnet
import time

from scalarizr import rpc
from scalarizr.bus import bus
from scalarizr.libs import metaconf
from scalarizr.node import __node__
from scalarizr.util import initdv2
from scalarizr.util import system2

# import StringIO
# str_fp = StringIO.StringIO()
# self.app_servers_inc.write_fp(str_fp, close=False)
# raise BaseException('%s' % str_fp.getvalue())

# TODO(uty): Get rid of these consts
APP_INC_PATH = 'app_include_path'
HTTPS_INC_PATH = 'https_include_path'


__nginx__ = __node__['nginx']


class NginxInitScript(initdv2.ParametrizedInitScript):
    _nginx_binary = None

    def __init__(self):
        self._nginx_binary = __nginx__['binary_path']

        pid_file = None
        '''
        Saw on 8.04:
        --pid-path=/var/run/nginx
        but actual pid-file is /var/run/nginx.pid
        try:
                nginx = software.whereis('nginx')
                if nginx:
                        out = system2((nginx[0], '-V'))[1]
                        m = re.search("--pid-path=(.*?)\s", out)
                        if m:
                                        pid_file = m.group(1)
        except:
                pass
        '''

        initdv2.ParametrizedInitScript.__init__(self,
                                                'nginx',
                                                '/etc/init.d/nginx',
                                                pid_file=pid_file,
                                                socks=[initdv2.SockParam(80)])

    # TODO(uty): inherit and extend start() with workers check (ps -C nginx --noheaders).
    # remove socks check

    def status(self):
        status = initdv2.ParametrizedInitScript.status(self)
        if not status and self.socks:
            ip, port = self.socks[0].conn_address
            telnet = Telnet(ip, port)
            telnet.write('HEAD / HTTP/1.0\n\n')
            if 'server: nginx' in telnet.read_all().lower():
                return initdv2.Status.RUNNING
            return initdv2.Status.UNKNOWN
        return status

    def configtest(self, path=None):
        args = '%s -t' % self._nginx_binary
        if path:
            args += '-c %s' % path

        out = system2(args, shell=True)[1]
        if 'failed' in out.lower():
            raise initdv2.InitdError("Configuration isn't valid: %s" % out)

    def stop(self):
        if not self.running:
            return True
        ret = initdv2.ParametrizedInitScript.stop(self)
        time.sleep(1)
        return ret

    def restart(self):
        self.configtest()
        ret = initdv2.ParametrizedInitScript.restart(self)
        time.sleep(1)
        return ret

# TODO(uty): add start/stop/restart methods to control nginx service with API
class NginxAPI(object):

    _instance = None
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(NginxAPI, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, app_inc_dir=None, https_inc_dir=None):
        self.service = NginxInitScript()
        # self.service.start()

        self.backend_table = {}

        if not app_inc_dir:
            app_inc_dir = os.path.dirname(__nginx__[APP_INC_PATH])
        self.app_inc_path = os.path.join(app_inc_dir, 'app-servers.include')
        self.app_servers_inc = metaconf.Configuration('nginx')
        if os.path.exists(self.app_inc_path):
            self.app_servers_inc.read(self.app_inc_path)
        else:
            open(self.app_inc_path, 'w').close()

        if not https_inc_dir:
            https_inc_dir = os.path.dirname(__nginx__[HTTPS_INC_PATH])
        self.https_inc_path = os.path.join(https_inc_dir, 'https.include')
        self.https_inc = metaconf.Configuration('nginx')
        if os.path.exists(self.https_inc_path):
            self.https_inc.read(self.https_inc_path)
        else:
            open(self.https_inc_path, 'w').close()

        self._make_error_pages_include()


    def _make_error_pages_include(self):

        def _add_static_location(config, location, expires=None):
            xpath = 'location'
            locations_num = len(config.get_list(xpath))
            config.add(xpath, location)

            xpath = '%s[%i]' % (xpath, locations_num + 1)

            if expires:
                config.add('%s/expires' % xpath, expires)
            config.add('%s/root' % xpath, '/usr/share/scalr/nginx/html')

        error_pages_dir = os.path.dirname(__nginx__[APP_INC_PATH])
        self.error_pages_inc = os.path.join(error_pages_dir,
                                            'error-pages.include')

        error_pages_conf = metaconf.Configuration('nginx')
        _add_static_location(error_pages_conf, '/500.html', '0')
        _add_static_location(error_pages_conf, '/502.html', '0')
        _add_static_location(error_pages_conf, '/noapp.html')
        error_pages_conf.write(self.error_pages_inc)

    def _clear_nginx_includes(self):
        with open(self.app_inc_path, 'w') as fp:
            fp.write('')
        with open(self.https_inc_path, 'w') as fp:
            fp.write('')
        self.app_servers_inc.read(self.app_inc_path)
        self.https_inc.read(self.https_inc_path)

    def restart_service(self):
        if self.service.status() == initdv2.Status.NOT_RUNNING:
            self.service.start()
        else:
            self.service.reload()

    @rpc.service_method
    def recreate_proxying(self, proxy_list):
        self._clear_nginx_includes()
        self.backend_table = []

        for proxy_parms in proxy_list:
            self.add_proxy(restart_service=False, **proxy_parms)

        self.restart_service()

    def get_role_servers(self, role_id):
        """ Method is used to get role servers from scalr """
        if type(role_id) is int:
            role_id = str(role_id)

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

    def update_ssl_certificate(self, ssl_certificate_id, cert, key, cacert):
        """
        Updates ssl certificate. Returns paths to updated or created .key and
        .crt files
        """
        if cacert:
            cert = cert + '\n' + cacert
        if ssl_certificate_id:
            ssl_certificate_id = '_' + str(ssl_certificate_id)
        else:
            ssl_certificate_id = ''

        keys_dir_path = os.path.join(bus.etc_path, "private.d/keys")
        if not os.path.exists(keys_dir_path):
            os.mkdir(keys_dir_path)

        cert_path = os.path.join(keys_dir_path, 'https%s.crt' % ssl_certificate_id)
        with open(cert_path, 'w') as fp:
            fp.write(cert)

        key_path = os.path.join(keys_dir_path, 'https%s.key' % ssl_certificate_id)
        with open(key_path, 'w') as fp:
            fp.write(key)

        return (cert_path, key_path)

    def _fetch_ssl_certificate(self, ssl_certificate_id):
        """
        Gets ssl certificate and key from Scalr, writes them to files and
        returns paths to files.
        """
        queryenv = bus.queryenv_service
        cert, key, cacert = queryenv.get_ssl_certificate(ssl_certificate_id)
        return self.update_ssl_certificate(ssl_certificate_id,
                                           cert,
                                           key,
                                           cacert)

    def _normalize_roles_arg(self, roles):
        """
        Parses list of roles. Role can be either int (role id)
        or dictionary. Dictionary example:

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
        if not roles:
            return []

        destinations = []
        for r in roles:
            role = {}
            if type(r) is str:
                role['id'] = role
            elif type(r) is int:
                role['id'] = str(r)
            else:
                # assuming that r is dict by default
                role.update(r)
                if type(role['id']) is int:
                    role['id'] = str(role['id'])
            role['servers'] = self.get_role_servers(role['id'])
            destinations.append(role)

        return destinations

    def _normalize_servers_arg(self, servers):
        """
        Parses list of servers. Servers can be either str (server ip)
        or dictionary. Dictionary example:

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
        if not servers:
            return []

        destinations = []
        for s in servers:
            server = {}
            if type(s) is str:
                server['servers'] = [s]
            else:
                # assuming that s is dict by default
                server.update(s)
                server['servers'] = [server['host']]
            destinations.append(server)

        return destinations

    def _group_destinations(self, destinations):
        """
        Groups destinations by location in list of lists.
        If no location defined assumes that it'r '/' location.
        """
        if not destinations:
            return []

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
        """
        Adds backend to app-servers config, but without writing it to file.
        """
        backend = self._make_backend_conf(name,
                                          destinations,
                                          port=port,
                                          ip_hash=ip_hash,
                                          max_fails=max_fails,
                                          fail_timeout=fail_timeout)
        self.app_servers_inc.append_conf(backend)

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
            config.add('upstream/ip_hash', '')

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

    def _add_backends(self,
                      hostname,
                      grouped_destinations,
                      port=None,
                      ip_hash=True,
                      max_fails=None,
                      fail_timeout=None):
        """
        Makes backend for each group of destinations and writes it to
        app-servers config file.

        Returns tuple of pairs with location and backend names:
        [[dest1, dest2], [dest3]] -> ((location1, name1), (location2, name2))

        Tuple of pairs is used instead of dict, because we need to keep order 
        saved.

        Name of backend is construct by pattern:

            ```hostname`[_`location`][__`role_id1`[_`role_id2`[...]]]``

        Example:

            ``test.com_somepage_123_345``
        """
        locations_and_backends = ()
        # making backend configs for each group
        for backend_destinations in grouped_destinations:
            # TODO: delete backends from initial config, that have similar name as new
            location = backend_destinations[0]['location']

            # Find role ids that will be used in backend
            role_ids = set([dest.get('id') for dest in backend_destinations])
            role_ids.discard(None)
            role_namepart = '_'.join(role_ids)

            if location.startswith('/'):
                location = location[1:]

            name = '%s%s__%s' % (hostname, 
                                 ('_' + location.replace('/', '_')).rstrip('_'),
                                 role_namepart)
            name = name.rstrip('_')

            self._add_backend(name,
                              backend_destinations,
                              port=port,
                              ip_hash=ip_hash,
                              max_fails=max_fails,
                              fail_timeout=fail_timeout)

            locations_and_backends += ((location or '/', name),)

        return locations_and_backends

    def _make_redirector_conf(self, hostname, port, ssl_port):
        """
        Makes config (metaconf.Configuration object) for server section of
        nginx config that is used to redirect http to https
        """
        config = metaconf.Configuration('nginx')
        config.add('server', '')

        config.add('server/listen', str(port))
        config.add('server/server_name', hostname + '_redirector')

        redirect_regex = '^(.*)$ https://localhost:%s$1 permanent' % (ssl_port)
        config.add('server/rewrite', redirect_regex)

        return config

    def _add_noapp_handler(self, config):
        """ Adding proxy to noapp.html location if no app servers are found """
        config.add('server/if', '( $remote_addr = 127.0.0.1 )')
        config.add('server/if/rewrite', '^(.*)$ /noapp.html last')
        config.add('server/if/return', '302')

    def _make_server_conf(self,
                          hostname,
                          locations_and_backends,
                          port='80',
                          ssl=False,
                          ssl_port=None,
                          ssl_certificate_id=None):
        """
        Makes config (metaconf.Configuration object) for server section of
        nginx config
        """
        config = metaconf.Configuration('nginx')
        config.add('server', '')

        if port:
            config.add('server/listen', str(port))

        # Configuring ssl
        if ssl:
            config.add('server/listen', '%s ssl' % (ssl_port or '443'))
            config.add('server/ssl', 'on')
            ssl_cert_path, ssl_cert_key_path = self._fetch_ssl_certificate(ssl_certificate_id)
            config.add('server/ssl_certificate', ssl_cert_path)
            config.add('server/ssl_certificate_key', ssl_cert_key_path)

            # TODO: move next hardcoded strings to some constants
            config.add('server/ssl_session_timeout', '10m')
            config.add('server/ssl_session_cache', 'shared:SSL:10m')
            config.add('server/ssl_protocols', 'SSLv2 SSLv3 TLSv1')
            config.add('server/ssl_ciphers', 'ALL:!ADH:!EXPORT56:RC4+RSA:+HIGH:+MEDIUM:+LOW:+SSLv2:+EXP')
            config.add('server/ssl_prefer_server_ciphers', 'on')

        config.add('server/server_name', hostname)

        self._add_noapp_handler(config)

        # Adding locations leading to defined backends
        for i, (location, backend_name) in enumerate(locations_and_backends):
            location_xpath = 'server/location'
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

            if location is '/':
                config.add('%s/error_page' % location_xpath, '500 501 = /500.html')
                config.add('%s/error_page' % location_xpath, '502 503 504 = /502.html')

        config.add('server/include', self.error_pages_inc)

        return config

    def _add_nginx_server(self,
                        hostname,
                        locations_and_backends,
                        port='80',
                        http=True,
                        ssl=False,
                        ssl_port=None,
                        ssl_certificate_id=None):
        """
        Adds server to https config, but without writing it to file.
        """
        if not http:
            redirector_conf = self._make_redirector_conf(hostname,
                                                         port,
                                                         ssl_port)
            self.https_inc.append_conf(redirector_conf)

        server_config = self._make_server_conf(hostname,
                                               locations_and_backends,
                                               port if http else None,
                                               ssl,
                                               ssl_port,
                                               ssl_certificate_id)
        self.https_inc.append_conf(server_config)   

    # TODO(uty): name can be a set of wildcards, ie: *.example.com www.example.*. 
    # i think its better use sha1 of proxy name for backend name 
    def add_proxy(self,
                  name,
                  roles=[],
                  servers=[],
                  port='80',
                  http=True,
                  ssl=False,
                  ssl_port=None,
                  ssl_certificate_id=None,
                  backend_port=None,
                  backend_ip_hash=False,
                  backend_max_fails=None,
                  backend_fail_timeout=None,
                  reread_conf=True,
                  restart_service=True):
        """
        Adds proxy
        """
        destinations = self._normalize_roles_arg(roles)
        destinations.extend(self._normalize_servers_arg(servers))

        grouped_destinations = self._group_destinations(destinations)
        if not grouped_destinations:
            raise BaseException('No servers or roles given', servers, roles)

        if reread_conf:
            self.app_servers_inc.read(self.app_inc_path)
            self.https_inc.read(self.https_inc_path)

        locations_and_backends = self._add_backends(name,
                                                    grouped_destinations,
                                                    port=backend_port,
                                                    ip_hash=backend_ip_hash,
                                                    max_fails=backend_max_fails,
                                                    fail_timeout=backend_fail_timeout)

        for backend_destinations, (_, backend_name) \
            in zip(grouped_destinations, locations_and_backends):
            self.backend_table[backend_name] = backend_destinations

        self._add_nginx_server(name,
                             locations_and_backends,
                             port=port,
                             http=http,
                             ssl=ssl,
                             ssl_port=ssl_port,
                             ssl_certificate_id=ssl_certificate_id)

        self.app_servers_inc.write(self.app_inc_path)
        self.https_inc.write(self.https_inc_path)

        if restart_service:
            self.restart_service()

    def _remove_backend(self, name):
        """
        Removes backend with given name from app-servers config.
        """
        xpath = metaconf.xpath_of(self.app_servers_inc, 'upstream', name)
        self.app_servers_inc.remove(xpath)

    def _remove_nginx_server(self, name):
        """
        Removes server from https.include config. Also removes used backends.
        """
        for i, _ in enumerate(self.https_inc.get_list('server')):

            server_xpath = 'server[%i]' % (i + 1)
            server_name = self.https_inc.get('%s/server_name' % server_xpath)

            if name == server_name or name == server_name + '_redirector':
                location_xpath = '%s/location' % server_xpath
                location_qty = len(self.https_inc.get_list(location_xpath))
                
                for i in xrange(location_qty):
                    xpath = location_xpath + ('[%i]' % i)
                    backend = self.https_inc.get(xpath + '/proxy_pass')
                    backend = backend.replace('http://', '')
                    self._remove_backend(backend)

                self.https_inc.remove(server_xpath)

    @rpc.service_method
    def remove_proxy(self, name, restart_service=True):
        """
        Removes proxy with given name. Removes created server and its backends.
        """
        self.https_inc.read(self.https_inc_path)
        self.app_servers_inc.read(self.app_inc_path)

        self._remove_nginx_server(name)

        # remove each backend that were in use by this proxy from backend_table
        for backend_name in self.backend_table:
            if name in backend_name:
                self.backend_table.pop(backend_name)

        self.https_inc.write(self.https_inc_path)
        self.app_servers_inc.write(self.app_inc_path)
        if restart_service:
            self.restart_service()

    @rpc.service_method
    def make_proxy(self, hostname, **kwds):
        """
        RPC method for adding or updating proxy configuration.
        """
        try:
            # trying to apply changes
            self.https_inc.read(self.https_inc_path)
            self.app_servers_inc.read(self.app_inc_path)

            self.https_inc.write(self.https_inc_path + '.bak')
            self.app_servers_inc.write(self.app_inc_path + '.bak')

            self._remove_nginx_server(hostname)

            self.add_proxy(hostname, reread_conf=False, **kwds)

        except:
            # undo changes
            self.https_inc.read(self.https_inc_path + '.bak')
            self.app_servers_inc.read(self.app_inc_path + '.bak')
            self.https_inc.write(self.https_inc_path)
            self.app_servers_inc.write(self.app_inc_path)
            raise

    # TODO: use this method in backend conf making or smth.
    def _server_to_str(self, server):
        if type(server) is unicode:
            return str(server)
        if type(server) is str:
            return server

        result = server['host']
        if 'port' in server:
            result = '%s:%s' % (result, server['port'])

        if 'backup' in server and server['backup']:
            result = '%s %s' % (result, 'backup')

        _max_fails = server.get('max_fails')
        if _max_fails:
            result = '%s %s' % (result, 'max_fails=%i' % _max_fails)

        _fail_timeout = server.get('fail_timeout')
        if _fail_timeout:
            result = '%s %s' % (result, 'fail_timeout=%is' % _fail_timeout)

        if 'down' in server:
            result = '%s %s' % (result, 'down')

        return result

    @rpc.service_method
    def add_server(self, backend, server, update_conf=True, restart_service=True):
        """
        Adds server to backend with given name pattern.
        Parameter server can be dict or string (ip addr)
        """
        if update_conf:
            self.app_servers_inc.read(self.app_inc_path)

        xpath = metaconf.xpath_of(self.app_servers_inc,
                                 'upstream',
                                 backend + '*')

        server = self._server_to_str(server)
        already_added = metaconf.xpath_of(self.app_servers_inc,
                                         '%s/server' % xpath,
                                         server)
        if not already_added:
            self.app_servers_inc.add('%s/server' % xpath, server)

        if update_conf:
            self.app_servers_inc.write(self.app_inc_path)
        if restart_service:
            self.restart_service()

    @rpc.service_method
    def remove_server(self, backend, server, update_conf=True, restart_service=True):
        """
        Removes server from backend with given name pattern.
        Parameter server can be dict or string (ip addr)
        """
        if update_conf:
            self.app_servers_inc.read(self.app_inc_path)

        if type(server) is dict:
            server = server['host']

        backend_xpath = metaconf.xpath_of(self.app_servers_inc,
                                         'upstream',
                                         backend + '*')
        server_xpath = metaconf.xpath_of(self.app_servers_inc,
                                        '%s/server' % backend_xpath,
                                        server + '*')
        if server_xpath:
            self.app_servers_inc.remove(server_xpath)

        if update_conf:
            self.app_servers_inc.write(self.app_inc_path)
        if restart_service:
            self.restart_service()

    @rpc.service_method
    def add_server_to_role(self, 
                           server,
                           role_id,
                           update_conf=True, 
                           restart_service=True):
        """
        Adds server to each backend that uses given role. If role isn't used in
        any backend, does nothing
        """
        if type(role_id) is not str:
            role_id = str(role_id)

        if update_conf:
            self.app_servers_inc.read(self.app_inc_path)

        config_updated = False
        for backend_name, backend_destinations in self.backend_table.items():
            for dest in backend_destinations:
                if dest.get('id') == role_id and server not in dest['servers']:
                    srv = {'host': server}
                    # taking server parameters
                    srv.update(dest)
                    srv.pop('servers')
                    srv.pop('id')
                    
                    self.add_server(backend_name, srv, False, False)
                    dest['servers'].append(server)
                    config_updated = True

        if config_updated:
            if update_conf:
                self.app_servers_inc.write(self.app_inc_path)
            if restart_service:
                self.restart_service()

    @rpc.service_method
    def remove_server_from_role(self,
                                server,
                                role_id,
                                update_conf=True,
                                restart_service=True):
        """
        Removes server from each backend that uses given role. If role isn't
        used in any backend, does nothing
        """
        if type(role_id) is not str:
            role_id = str(role_id)

        if update_conf:
            self.app_servers_inc.read(self.app_inc_path)

        config_updated = False
        for backend_name, backend_destinations in self.backend_table.items():
            for dest in backend_destinations:
                if dest.get('id') == role_id and server in dest['servers']:
                    self.remove_server(backend_name, server, False, False)
                    dest['servers'].remove(server)
                    config_updated = True

        if config_updated:
            if update_conf:
                self.app_servers_inc.write(self.app_inc_path)
            if restart_service:
                self.restart_service()


    @rpc.service_method
    def remove_server_from_all_backends(self,
                                        server,
                                        update_conf=True,
                                        restart_service=True):
        """
        Method is used to remove stand-alone servers, that aren't belong
        to any role. If role isn't used in any backend, does nothing
        """
        if update_conf:
            self.app_servers_inc.read(self.app_inc_path)

        config_updated = False
        for backend_name, backend_destinations in self.backend_table.items():
            for dest in backend_destinations:
                if server in dest['servers']:
                    self.remove_server(backend_name, server, False, False)
                    dest['servers'].remove(server)
                    config_updated = True

        if config_updated:
            if update_conf:
                self.app_servers_inc.write(self.app_inc_path)
            if restart_service:
                self.restart_service()
