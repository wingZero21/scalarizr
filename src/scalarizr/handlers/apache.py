
from __future__ import with_statement
'''
Created on Dec 25, 2009

@author: Dmytro Korsakov
@author: marat
'''

import pwd
import logging

from scalarizr.bus import bus
from scalarizr.api import apache
from scalarizr.util import disttool
from scalarizr.linux import coreutils
from scalarizr.handlers import Handler
from scalarizr.messaging import Messages
from scalarizr.api import service as preset_service
from scalarizr.services import PresetProvider, BaseConfig
from scalarizr.config import BuiltinBehaviours, ScalarizrState


LOG = logging.getLogger(__name__)
BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.APP


def get_handlers ():
    return [ApacheHandler()]



class ApacheHandler(Handler):

    _queryenv = None

    def __init__(self):
        self.webserver = apache.ApacheWebServer()
        self.api = apache.ApacheAPI()
        self.preset_provider = ApachePresetProvider()
        preset_service.services[BEHAVIOUR] = self.preset_provider
        bus.on(init=self.on_init, reload=self.on_reload)
        bus.define_events('apache_rpaf_reload')
        self.on_reload()


    def on_init(self):
        bus.on(
                start = self.on_start,
                before_host_up = self.on_before_host_up,
                host_init_response = self.on_host_init_response
        )
        if self._cnf.state == ScalarizrState.BOOTSTRAPPING:
            self.webserver.init_service()


    def on_reload(self):
        self._queryenv = bus.queryenv_service
        self._cnf = bus.cnf


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


    def on_host_init_response(self, message):
        pass

    def on_before_host_up(self, message):
        with bus.initialization_op as op:
            with op.phase(self._phase):
                with op.step(self._step_update_vhosts):
                    self.api.reload_vhosts()
                with op.step(self._step_reload_rpaf):
                    self._rpaf_reload()
                bus.fire('service_configured', service_name=SERVICE_NAME, preset=self.initial_preset)


    def on_start(self):
        if self._cnf.state == ScalarizrState.RUNNING:
            self.api.reload_vhosts()
            self._rpaf_reload()


    def on_HostUp(self, message):
        if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
            self.webserver.mod_rpaf.add([message.local_ip])
            self.webserver.service.reload('Applying new RPAF proxy IPs list')


    def on_HostDown(self, message):
        if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
            self.webserver.mod_rpaf.remove([message.local_ip])
            self.webserver.service.reload('Applying new RPAF proxy IPs list')


    def on_VhostReconfigure(self, message):
        self.api.reload_vhosts()


    def _rpaf_reload(self):
        lb_hosts = []
        for role in self._queryenv.list_roles(behaviour=BuiltinBehaviours.WWW):
            for host in role.hosts:
                lb_hosts.append(host.internal_ip)
        self.webserver.mod_rpaf.update(lb_hosts)
        self.webserver.service.reload('Applying new RPAF proxy IPs list')
        bus.fire('apache_rpaf_reload')


    on_BeforeHostTerminate = on_HostDown


class ApacheConf(BaseConfig):

    config_type = 'app'
    config_name = 'apache2.conf' if disttool.is_debian_based() else 'httpd.conf'


class ApachePresetProvider(PresetProvider):

    def __init__(self):
        webserver = apache.ApacheWebServer()
        config_mapping = {'apache.conf':ApacheConf(apache.APACHE_CONF_PATH)}
        PresetProvider.__init__(self, webserver.service, config_mapping)


    def rollback_hook(self):
        try:
            pwd.getpwnam('apache')
            uname = 'apache'
        except:
            uname = 'www-data'
        for obj in self.config_data:
            coreutils.chown_r(obj.path, uname)