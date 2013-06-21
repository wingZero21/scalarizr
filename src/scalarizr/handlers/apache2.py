from __future__ import with_statement
'''
Created on Jun 10, 2013

@author: Dmytro Korsakov
'''

from __future__ import with_statement

# Core
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.service import CnfController
from scalarizr.api import service as preset_service
from scalarizr.handlers import HandlerError, ServiceCtlHandler,
from scalarizr.messaging import Messages

# Libs
from scalarizr.libs.metaconf import Configuration, ParseError, MetaconfError
from scalarizr.util import disttool, software
from scalarizr.util import initdv2
from scalarizr.linux import coreutils
from scalarizr.services import PresetProvider, BaseConfig

# Stdlibs
import logging
import pwd



class ApacheCnfController(CnfController):

    def __init__(self):
        CnfController.__init__(self, BEHAVIOUR, APACHE_CONF_PATH, 'apache', {'1':'on','0':'off'})

    @property
    def _software_version(self):
        return software.software_info('apache').version


def get_handlers ():
    return [ApacheHandler()]

def reload_apache_conf(f):
    def g(self,*args):
        self._config = Configuration('apache')
        try:
            self._config.read(self._httpd_conf_path)
        except (OSError, MetaconfError, ParseError), e:
            raise HandlerError('Cannot read Apache config %s : %s' % (self._httpd_conf_path, str(e)))
        f(self,*args)
    return g


class ApacheHandler(ServiceCtlHandler):

    _config = None
    _logger = None
    _queryenv = None
    _cnf = None
    '''
    @type _cnf: scalarizr.config.ScalarizrCnf
    '''

    def __init__(self):
        self._logger = logging.getLogger(__name__)
        ServiceCtlHandler.__init__(self, SERVICE_NAME, initdv2.lookup('apache'), ApacheCnfController())
        self.preset_provider = ApachePresetProvider()
        preset_service.services[BEHAVIOUR] = self.preset_provider
        bus.on(init=self.on_init, reload=self.on_reload)
        bus.define_events(
                'apache_rpaf_reload'
        )
        self.on_reload()


    def on_init(self):
        bus.on(
                start = self.on_start,
                before_host_up = self.on_before_host_up,
                host_init_response = self.on_host_init_response
        )

        self._logger.debug('State: %s', self._cnf.state)
        self._insert_iptables_rules()
        if self._cnf.state == ScalarizrState.BOOTSTRAPPING:
            self._logger.debug('Bootstrapping routines')
            self._stop_service('Configuring')


    def on_reload(self):
        self._queryenv = bus.queryenv_service
        self._cnf = bus.cnf
        self._httpd_conf_path = APACHE_CONF_PATH
        self._config = Configuration('apache')
        self._config.read(self._httpd_conf_path)


    def on_host_init_response(self, message):
        if hasattr(message, BEHAVIOUR):
            data = getattr(message, BEHAVIOUR)
            if data and 'preset' in data:
                self.initial_preset = data['preset'].copy()


    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return BEHAVIOUR in behaviour and \
                (message.name == Messages.VHOST_RECONFIGURE or \
                message.name == Messages.UPDATE_SERVICE_CONFIGURATION or \
                message.name == Messages.HOST_UP or \
                message.name == Messages.HOST_DOWN or \
                message.name == Messages.BEFORE_HOST_TERMINATE)

    def get_initialization_phases(self, hir_message):
        self._phase = 'Configure Apache'
        self._step_update_vhosts = 'Update virtual hosts'
        self._step_reload_rpaf = 'Reload RPAF'

        return {'before_host_up': [{
                'name': self._phase,
                'steps': [self._step_update_vhosts, self._step_reload_rpaf]
        }]}

    def on_start(self):
        if self._cnf.state == ScalarizrState.RUNNING:
            self._update_vhosts()
            self._rpaf_reload()

    def on_before_host_up(self, message):

        with bus.initialization_op as op:
            with op.phase(self._phase):
                with op.step(self._step_update_vhosts):
                    self._update_vhosts()
                with op.step(self._step_reload_rpaf):
                    self._rpaf_reload()
                bus.fire('service_configured', service_name=SERVICE_NAME, preset=self.initial_preset)

    def on_HostUp(self, message):
        if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
            self._rpaf_modify_proxy_ips([message.local_ip], operation='add')

    def on_HostDown(self, message):
        if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
            self._rpaf_modify_proxy_ips([message.local_ip], operation='remove')

    on_BeforeHostTerminate = on_HostDown

    @reload_apache_conf
    def on_VhostReconfigure(self, message):
        pass

class ApacheConf(BaseConfig):

    config_type = 'app'
    config_name = 'apache2.conf' if disttool.is_debian_based() else 'httpd.conf'


class ApachePresetProvider(PresetProvider):

    def __init__(self):
        service = initdv2.lookup('apache')
        config_mapping = {'apache.conf':ApacheConf(APACHE_CONF_PATH)}
        PresetProvider.__init__(self, service, config_mapping)


    def rollback_hook(self):
        try:
            pwd.getpwnam('apache')
            uname = 'apache'
        except:
            uname = 'www-data'
        for obj in self.config_data:
            coreutils.chown_r(obj.path, uname)
