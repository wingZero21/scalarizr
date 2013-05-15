'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
@author: spike
@author: uty
'''

from __future__ import with_statement

# Core components
from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours, ScalarizrState
from scalarizr.service import CnfController
from scalarizr.handlers import HandlerError, ServiceCtlHandler
from scalarizr.messaging import Messages
from scalarizr.api import service as preset_service
from scalarizr.node import __node__
from scalarizr.api.nginx import NginxAPI
from scalarizr.api.nginx import NginxInitScript

# Libs
from scalarizr.libs.metaconf import Configuration, NoPathError
from scalarizr.util import cached, firstmatched,\
        validators, software, initdv2, disttool
from scalarizr.linux import iptables
from scalarizr.services import BaseConfig, PresetProvider

# Stdlibs
import os, logging, shutil
from datetime import datetime
import ConfigParser
import cStringIO


BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.WWW
CNF_NAME = BEHAVIOUR
CNF_SECTION = BEHAVIOUR

BIN_PATH = 'binary_path'
APP_PORT = 'app_port'
HTTPS_INC_PATH = 'https_include_path'
APP_INC_PATH = 'app_include_path'
UPSTREAM_APP_ROLE = 'upstream_app_role'

__nginx__ = __node__['nginx']


initdv2.explore('nginx', NginxInitScript)


# Nginx behaviours configuration options
class NginxOptions(Configurator.Container):
    '''
    www behaviour
    '''
    cnf_name = CNF_NAME

    class binary_path(Configurator.Option):
        '''
        Path to nginx binary
        '''
        name = CNF_SECTION + '/binary_path'
        required = True

        @property
        @cached
        def default(self):
            return firstmatched(lambda p: os.access(p, os.F_OK | os.X_OK),
                                ('/usr/sbin/nginx', '/usr/local/nginx/sbin/nginx'), '')

        @validators.validate(validators.executable)
        def _set_value(self, v):
            Configurator.Option._set_value(self, v)

        value = property(Configurator.Option._get_value, _set_value)


    class app_port(Configurator.Option):
        '''
        App role port
        '''
        name = CNF_SECTION + '/app_port'
        default = '80'
        required = True

        @validators.validate(validators.portnumber())
        def _set_value(self, v):
            Configurator.Option._set_value(self, v)

        value = property(Configurator.Option._get_value, _set_value)


    class app_include_path(Configurator.Option):
        '''
        App upstreams configuration file path.
        '''
        name = CNF_SECTION + '/app_include_path'
        default = '/etc/nginx/app-servers.include'
        required = True

    class https_include_path(Configurator.Option):
        '''
        HTTPS configuration file path.
        '''
        name = CNF_SECTION + '/https_include_path'
        default = '/etc/nginx/https.include'
        required = True


def get_handlers():
    return [NginxHandler()]


class NginxCnfController(CnfController):
    def __init__(self):
        nginx_conf_path = os.path.join(os.path.dirname(__nginx__[APP_INC_PATH]), 'nginx.conf')
        CnfController.__init__(self, BEHAVIOUR, nginx_conf_path, 'nginx', {"on":'1',"'off'":'0','off':'0'})

    @property
    def _software_version(self):
        return software.software_info('nginx').version


class NginxHandler(ServiceCtlHandler):

    backends_xpath = "upstream[@value='backend']/server"
    localhost = '127.0.0.1:80'

    def __init__(self):
        self._cnf = bus.cnf
        ServiceCtlHandler.__init__(self, BEHAVIOUR, initdv2.lookup('nginx'), NginxCnfController())

        self._logger = logging.getLogger(__name__)
        self.preset_provider = NginxPresetProvider()
        self.api = NginxAPI()
        preset_service.services[BEHAVIOUR] = self.preset_provider

        bus.define_events("nginx_upstream_reload")
        bus.on(init=self.on_init, reload=self.on_reload)
        self.on_reload()

    def on_init(self):
        bus.on(start=self.on_start,
               before_host_up=self.on_before_host_up,
               host_init_response=self.on_host_init_response)

        self._insert_iptables_rules()

        if __node__['state'] == ScalarizrState.BOOTSTRAPPING:
            self._stop_service('Configuring')

    def on_reload(self):
        self._queryenv = bus.queryenv_service

        self._nginx_binary = __nginx__[BIN_PATH]
        self._https_inc_path = __nginx__[HTTPS_INC_PATH]
        self._app_inc_path = __nginx__[APP_INC_PATH]
        self._app_port = __nginx__[APP_PORT]
        try:
            self._upstream_app_role = __nginx__[UPSTREAM_APP_ROLE]
        except ConfigParser.Error:
            self._upstream_app_role = None

    def on_host_init_response(self, message):
        if hasattr(message, BEHAVIOUR):
            data = getattr(message, BEHAVIOUR)
            if data and 'preset' in data:
                self.initial_preset = data['preset'].copy()
            if data and 'proxies' in data:
                self._proxies = data['proxies'].copy()
            else:
                self._proxies = None


    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return BEHAVIOUR in behaviour and \
                (message.name == Messages.HOST_UP or \
                message.name == Messages.HOST_DOWN or \
                message.name == Messages.BEFORE_HOST_TERMINATE or \
                message.name == Messages.VHOST_RECONFIGURE or \
                message.name == Messages.UPDATE_SERVICE_CONFIGURATION)

    def get_initialization_phases(self, hir_message):
        self._phase = 'Configure Nginx'
        # self._step_update_vhosts = 'Update virtual hosts'
        # self._step_reload_upstream = 'Reload upstream'
        self._step_setup_proxying = 'Setup proxying'
        self._step_copy_error_pages = 'Copy default html error pages'

        return {'before_host_up': [{
                    'name': self._phase,
                    # 'steps': [self._step_update_vhosts, self._step_reload_upstream]
                    'steps': [self._step_copy_error_pages,
                              self._step_setup_proxying]}]}

    def on_start(self):
        if __node__['state'] == 'running':
            # self._update_vhosts()
            # self._reload_upstream()

            role_params = self._queryenv.list_farm_role_params(__node__['farm_role_id'])
            if role_params and 'proxies' in role_params:
                self.api.recreate_proxying(role_params['proxies'])

    def on_before_host_up(self, message):
        # with bus.initialization_op as op:
        #     with op.phase(self._phase):
        #         with op.step(self._step_update_vhosts):
        #             self._update_vhosts()

        #         with op.step(self._step_reload_upstream):
        #             self._reload_upstream()

        with bus.initialization_op as op:
            with op.phase(self._phase):

                with op.step(self._step_copy_error_pages):
                    self._copy_error_pages()

                with op.step(self._step_setup_proxying):
                    if self._proxies:
                        self.api.recreate_proxying(self._proxies)
                    else:
                        # default behaviour
                        roles_for_proxy = []
                        if __nginx__['upstream_app_role']:
                            roles_for_proxy = [__nginx__['upstream_app_role']]
                        else:
                            roles_for_proxy = self.get_all_app_roles()
                        self.make_default_proxy(roles_for_proxy)
                    self._logger.debug('Updating main config')
                    self._update_main_config()

        bus.fire('service_configured',
                 service_name=SERVICE_NAME,
                 preset=self.initial_preset)

    def on_HostUp(self, message):
        # self._reload_upstream()
        server = ''
        role_id = message.farm_role_id
        behaviours = message.behaviour
        if message.cloud_location == __node__['cloud_location']:
            server = message.local_ip
        else:
            server = message.remote_ip

        self._logger.debug('on host up backend table is %s' % self.api.backend_table)
        # Assuming backend `backend` can be only in default behaviour mode
        if self._in_default_mode():
            upstream_role = __nginx__['upstream_app_role']
            if (upstream_role and upstream_role == role_id) or \
                (not upstream_role and BuiltinBehaviours.APP in behaviours):

                for default_backend in ['backend', 'backend.ssl']:
                    if default_backend not in self.api.backend_table:
                        continue

                    self.api.remove_server(default_backend, '127.0.0.1',
                                           restart_service=False,
                                           update_backend_table=True)
                    self._logger.debug('adding new app server %s to default '
                                        'backend' % server)
                    self.api.add_server(default_backend, server,
                                         update_backend_table=True)
                    
        else:
            self._logger.debug('adding new app server %s to backends that are '
                               'using role %s' % (server, role_id))
            self.api.add_server_to_role(server, role_id)
        self._logger.debug('after host up backend table is %s' % self.api.backend_table)


    def on_HostDown(self, message):
        # self._reload_upstream()
        server = ''
        role_id = message.farm_role_id
        behaviours = message.behaviour
        if message.cloud_location == __node__['cloud_location']:
            server = message.local_ip
        else:
            server = message.remote_ip

        self._logger.debug('on host down backend table is %s' % self.api.backend_table)
        self._logger.debug('removing server %s from backends' % server)
        # Assuming backend `backend` can be only in default behaviour mode
        if self._in_default_mode():
            upstream_role = __nginx__['upstream_app_role']
            if (upstream_role and upstream_role == role_id) or \
                (not upstream_role and BuiltinBehaviours.APP in behaviours):

                self._logger.debug('removing server %s from default backend' %
                                   server)

                for default_backend in ['backend', 'backend.ssl']:
                    if default_backend not in self.api.backend_table:
                        continue
                    if len(self.api.backend_table[default_backend][0]['servers']) == 1:
                        self._logger.debug('adding localhost to default backend')
                        self.api.add_server(default_backend, '127.0.0.1',
                                            restart_service=False,
                                            update_backend_table=True)
                    self.api.remove_server(default_backend, server, 
                                           update_backend_table=True)

        else:
            self._logger.debug('trying to remove server %s from backends that '
                               'are using role %s' % (server, role_id))
            self.api.remove_server_from_role(server, role_id)
        self._logger.debug('after host down backend table is %s' % self.api.backend_table)

    def on_BeforeHostTerminate(self, message):
        # if not os.access(self._app_inc_path, os.F_OK):
        #     self._logger.debug('File %s not exists. Nothing to do', self._app_inc_path)
        #     return

        # include = Configuration('nginx')
        # include.read(self._app_inc_path)

        # server_ip = '%s:%s' % (message.local_ip or message.remote_ip, self._app_port)
        # backends = include.get_list(self.backends_xpath)
        # if server_ip in backends:
        #     include.remove(self.backends_xpath, server_ip)
        #     # Add 127.0.0.1 If it was the last backend
        #     if len(backends) == 1:
        #         include.add(self.backends_xpath, self.localhost)

        # include.write(self._app_inc_path)
        # self._reload_service('%s is to be terminated' % server_ip)

        # do the same as in on_HostDown?
        # self.on_HostDown(message) #?
        pass

    def on_VhostReconfigure(self, message):
        # self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
        # self._update_vhosts()
        # self._reload_upstream(True)
        # if ssl certificate is updated:
        #     write new ssl certificate
        #     self.api._restart_service()

        # TODO: maybe uncomment next 9 lines if message actually contains
        #       vhost name and its ssl status
        # self._logger.debug('Trying to update ssl certificate on vhost reconfigure')
        # cert, key, cacert = self._queryenv.get_https_certificate()
        # self._logger.debud('Got cert: \n%s\n\nkey:\n%s\n\ncacert:\n%s\n' % 
        #                    (cert, key, cacert))
        # if cert and key:
        #     self.api.update_ssl_certificate(None, cert, key, cacert)
        #     self.api.enable_ssl()
        # else:
        #     self.api.disable_ssl()
        if self._in_default_mode():
            self._logger.debug('before vhost reconf backend table is %s' % self.api.backend_table)
            roles_for_proxy = []
            if __nginx__['upstream_app_role']:
                roles_for_proxy = [__nginx__['upstream_app_role']]
            else:
                roles_for_proxy = self.get_all_app_roles()
            self.make_default_proxy(roles_for_proxy)
            self._logger.debug('after vhost reconf backend table is %s' % self.api.backend_table)

    def on_SSLCertificateUpdate(self, message):
        ssl_cert_id = message.id  # TODO: check datastructure
        private_key = message.private_key
        certificate = message.certificate
        cacertificate = message.cacertificate
        self.api.update_ssl_certificate(ssl_cert_id,
                                        certificate,
                                        private_key,
                                        cacertificate)
        self.api._restart_service()

    def _in_default_mode(self):
        return 'backend' in self.api.backend_table or \
            'backend.ssl' in self.api.backend_table

    def _copy_error_pages(self):
        pages_source = '/usr/share/scalr/nginx/html/'
        pages_destination = '/usr/share/nginx/html/'

        current_dir = ''
        for d in pages_destination.split(os.path.sep)[1:-1]:
            current_dir = current_dir + '/' + d
            if not os.path.exists(current_dir):
                os.makedirs(current_dir)

        if not os.path.exists(pages_destination + '500.html'):
            shutil.copy(pages_source + '500.html', pages_destination)
        if not os.path.exists(pages_destination + '502.html'):
            shutil.copy(pages_source + '502.html', pages_destination)
        if not os.path.exists(pages_destination + 'noapp.html'):
            shutil.copy(pages_source + 'noapp.html', pages_destination)

    def make_default_proxy(self, roles):
        received_vhosts = self._queryenv.list_virtual_hosts()
        ssl_present = any(vhost.https for vhost in received_vhosts)
        nossl_present = any(not vhost.https for vhost in received_vhosts)
        self._logger.debug('Making default proxy with ssl is %s' % ssl_present)
        servers = []
        for role in roles:
            if type(role) is str:
                servers.extend(self.api.get_role_servers(role))
            else:
                cl = __node__['cloud_location']
                servers_ips = [h.internal_ip if cl == h.cloud_location else
                               h.external_ip
                               for h in role.hosts]
                servers.extend(servers_ips)

        if not servers:
            self._logger.debug('No app roles in farm, making mock backend')
            servers = [{'host': '127.0.0.1',
                        'port': '80'}]

        self._logger.debug('backend table is %s' % self.api.backend_table)
        if nossl_present:
            self.api.make_proxy('backend',
                                servers=servers,
                                ssl=False,
                                backend_ip_hash=True,
                                hash_backend_name=False)
        if ssl_present:
            self.api.make_proxy('backend.ssl',
                                servers=servers,
                                ssl=True,
                                backend_ip_hash=True,
                                hash_backend_name=False)
        self._logger.debug('After making proxy backend table is %s' % self.api.backend_table)
        self._logger.debug('Default proxy is made')

    def get_all_app_roles(self):
        return self._queryenv.list_roles(behaviour=BuiltinBehaviours.APP)

    def _test_config(self):
        self._logger.debug("Testing new configuration")
        try:
            self._init_script.configtest()
        except initdv2.InitdError, e:
            self._logger.error("Configuration error detected: %s Reverting configuration." % str(e))

            if os.path.isfile(self._app_inc_path):
                shutil.move(self._app_inc_path, self._app_inc_path+".junk")
            else:
                self._logger.debug('%s does not exist', self._app_inc_path)
            if os.path.isfile(self._app_inc_path+".save"):
                shutil.move(self._app_inc_path+".save", self._app_inc_path)
            else:
                self._logger.debug('%s does not exist', self._app_inc_path+".save")
        else:
            self._reload_service()

    def _dump_config(self, obj):
        output = cStringIO.StringIO()
        obj.write_fp(output, close = False)
        return output.getvalue()

    def _update_main_config(self):
        config_dir = os.path.dirname(self.api.app_inc_path)
        nginx_conf_path = os.path.join(config_dir, 'nginx.conf')

        config = None
        try:
            config = Configuration('nginx')
            config.read(nginx_conf_path)
        except (Exception, BaseException), e:
            raise HandlerError('Cannot read/parse nginx main configuration file: %s' % str(e))

        self._logger.debug('Update main configuration file')
        dump = self._dump_config(config)

        include_list = config.get_list('http/include')
        if not self.api.app_inc_path in include_list:
            config.add('http/include', self.api.app_inc_path)
        if not self.api.https_inc_path in include_list:
            config.add('http/include', self.api.https_inc_path)

        self._logger.debug('removing http/server section')
        try:
            config.remove('http/server')
        except (ValueError, IndexError):
            self._logger.debug('no http/server section')
        
        if disttool.is_debian_based():
        # Comment /etc/nginx/sites-enabled/*
            try:
                i = config.get_list('http/include').index('/etc/nginx/sites-enabled/*')
                config.comment('http/include[%d]' % (i+1,))
                self._logger.debug('comment site-enabled include')
            except (ValueError, IndexError):
                self._logger.debug('site-enabled include already commented')
        elif disttool.is_redhat_based():
            def_host_path = '/etc/nginx/conf.d/default.conf'
            if os.path.exists(def_host_path):
                default_host = Configuration('nginx')
                default_host.read(def_host_path)
                default_host.comment('server')
                default_host.write(def_host_path)

        if dump == self._dump_config(config):
            self._logger.debug("Main nginx config wasn`t changed")
        else:
            # Write new nginx.conf
            if not os.path.exists(nginx_conf_path + '.save'):
                shutil.copy(nginx_conf_path, nginx_conf_path + '.save')
            config.write(nginx_conf_path)
            self.api._restart_service()
            
    def _reload_upstream(self, force_reload=False):

        backend_include = Configuration('nginx')
        if os.path.exists(self._app_inc_path):
            backend_include.read(self._app_inc_path)
            try:
                backend_include.get("upstream[@value='backend']")
            except NoPathError:
                # Sorry, metaconf, backend_include.add() doesn't work as expected
                with open(self._app_inc_path, 'a') as fp:
                    fp.write('\nupstream backend {')
                    fp.write('\n    ip_hash;')
                    fp.write('\n}')
                backend_include = Configuration('nginx')
                backend_include.read(self._app_inc_path)
        else:
            backend_include.read(os.path.join(bus.share_path, 'nginx/app-servers.tpl'))

        # Create upstream hosts configuration
        if not self._upstream_app_role:
            kwds = dict(behaviour=BuiltinBehaviours.APP)
        else:
            kwds = dict(role_name=self._upstream_app_role)
        list_roles = self._queryenv.list_roles(**kwds)
        servers = []

        for app_serv in list_roles:
            for app_host in app_serv.hosts :
                server_str = '%s:%s' % (app_host.internal_ip or app_host.external_ip, self._app_port)
                servers.append(server_str)
        self._logger.debug("QueryEnv returned list of app servers: %s" % servers)

        # Add cloudfoundry routers
        for role in self._queryenv.list_roles(behaviour=BuiltinBehaviours.CF_ROUTER):
            for host in role.hosts:
                servers.append('%s:%s' % (host.internal_ip or host.external_ip, 2222))

        for entry in backend_include.get_list(self.backends_xpath):
            for server in servers:
                if entry.startswith(server):
                    self._logger.debug("Server %s already in upstream list" % server)
                    servers.remove(server)
                    break
            else:
                self._logger.debug("Removing old entry %s from upstream list" % entry)
                backend_include.remove(self.backends_xpath, entry)

        for server in servers:
            self._logger.debug("Adding new server %s to upstream list" % server)
            backend_include.add(self.backends_xpath, server)

        if not backend_include.get_list(self.backends_xpath):
            self._logger.debug("Scalr returned empty app hosts list. Adding localhost only")
            backend_include.add(self.backends_xpath, self.localhost)
        self._logger.info('Upstream servers: %s', ' '.join(backend_include.get_list(self.backends_xpath)))

        # Https configuration
        # openssl req -new -x509 -days 9999 -nodes -out cert.pem -keyout cert.key
        if os.access(self._https_inc_path, os.F_OK) \
                        and os.access(self._cnf.key_path("https.crt"), os.F_OK) \
                        and os.access(self._cnf.key_path("https.key"), os.F_OK):
            self._logger.debug('Add https include %s', self._https_inc_path)
            backend_include.set('include', self._https_inc_path, force=True)
        else:
            self._logger.debug('Removing https include %s', self._https_inc_path)
            try:
                backend_include.remove('include', self._https_inc_path)
            except NoPathError:
                pass


        old_include = None
        if os.path.isfile(self._app_inc_path):
            self._logger.debug("Reading old configuration from %s" % self._app_inc_path)
            old_include = Configuration('nginx')
            old_include.read(self._app_inc_path)

        if old_include \
                        and not force_reload \
                        and     backend_include.get_list(self.backends_xpath) == old_include.get_list(self.backends_xpath) :
            self._logger.debug("nginx upstream configuration unchanged")
        else:
            self._logger.debug("nginx upstream configuration was changed")

            if os.access(self._app_inc_path, os.F_OK):
                self._logger.debug('Backup file %s as %s', self._app_inc_path, self._app_inc_path + '.save')
                shutil.move(self._app_inc_path, self._app_inc_path+".save")

            self._logger.debug('Write new %s', self._app_inc_path)
            backend_include.write(self._app_inc_path)

            self._update_main_config()

            self._test_config()

        bus.fire("nginx_upstream_reload")



    def _insert_iptables_rules(self, *args, **kwargs):
        if iptables.enabled():
            iptables.FIREWALL.ensure([
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "80"},
                    {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": "443"},
            ])


    def _update_vhosts(self):
        self._logger.debug("Requesting virtual hosts list")
        received_vhosts = self._queryenv.list_virtual_hosts()
        self._logger.debug("Virtual hosts list obtained (num: %d)", len(received_vhosts))

        https_config = ''
        cert_path = self._cnf.key_path("https.crt")
        pk_path = self._cnf.key_path("https.key")
        ssl_present = any(vhost.https for vhost in received_vhosts)

        if ssl_present:
            https_certificate = self._queryenv.get_https_certificate()

            if https_certificate[0]:
                cert = https_certificate[0]
                self._logger.debug('Writing ssl cert')
                with open(cert_path, 'w') as fp:
                    fp.write(cert)
            else:
                self._logger.error('Scalr returned empty SSL Cert')
                return

            if len(https_certificate)>1 and https_certificate[1]:
                pk = https_certificate[1]
                self._logger.debug('Writing ssl key')
                with open(pk_path, 'w') as fp:
                    fp.write(pk)
            else:
                self._logger.error('Scalr returned empty SSL Cert')
                return

            if https_certificate[2]:
                cert = https_certificate[2]
                self._logger.debug('Appending CA cert to cert file')
                with open(cert_path, 'a') as fp:
                    fp.write('\n' + https_certificate[2])
        else:
            self._logger.debug('No SSL vhosts obtained. Removing old SSL keys.')
            for key_path in (cert_path, pk_path):
                if os.path.exists(key_path):
                    os.remove(key_path)
                    self._logger.debug('%s deleted' % key_path)

        if received_vhosts:

            for vhost in received_vhosts:
                if vhost.hostname and vhost.type == 'nginx': #and vhost.https
                    raw = vhost.raw.replace('/etc/aws/keys/ssl/https.crt',cert_path)
                    raw = raw.replace('/etc/aws/keys/ssl/https.key',pk_path)
                    https_config += raw + '\n'

            if https_config:
                if os.path.exists(self._https_inc_path):
                    file_content = None
                    with open(self._https_inc_path, 'r') as fp:
                        file_content = fp.read()
                    if file_content:
                        time_suffix = str(datetime.now()).replace(' ','.')
                        shutil.move(self._https_inc_path, self._https_inc_path + time_suffix)

                with open(self._https_inc_path, 'w') as fp:
                    fp.write(https_config)

        else:
            self._logger.debug('Scalr returned empty virtualhost list. Removing junk files.')
            if os.path.exists(self._https_inc_path):
                os.remove(self._https_inc_path)
                self._logger.debug('%s deleted' % self._https_inc_path)

        if https_config:
            if os.path.exists(self._https_inc_path) \
                            and open(self._https_inc_path, 'r').read():
                time_suffix = str(datetime.now()).replace(' ','.')
                shutil.move(self._https_inc_path, self._https_inc_path + time_suffix)

            self._logger.debug('Writing virtualhosts to https.include')
            with open(self._https_inc_path, 'w') as fp:
                fp.write(https_config)



class NginxConf(BaseConfig):

    config_type = 'www'
    config_name = 'nginx.conf'


class NginxPresetProvider(PresetProvider):

    def __init__(self):

        nginx_conf_path = os.path.join(os.path.dirname(__nginx__[APP_INC_PATH]), 'nginx.conf')
        config_mapping = {'nginx.conf':NginxConf(nginx_conf_path)}
        service = initdv2.lookup('nginx')
        PresetProvider.__init__(self, service, config_mapping)
