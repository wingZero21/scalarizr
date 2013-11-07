"""
Created on Dec 25, 2009

@author: Dmytro Korsakov
@author: marat
"""

import pwd
import logging

from scalarizr.bus import bus
from scalarizr.api import apache
from scalarizr.node import __node__
from scalarizr.linux import coreutils
from scalarizr.handlers import Handler
from scalarizr.messaging import Messages
from scalarizr.util import disttool, initdv2
from scalarizr.api import service as preset_service
from scalarizr.services import PresetProvider, BaseConfig
from scalarizr.config import BuiltinBehaviours, ScalarizrState


LOG = logging.getLogger(__name__)
BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.APP

__apache__ = __node__['apache']


def get_handlers():
    return [ApacheHandler()]


class ApacheHandler(Handler):

    _queryenv = None
    _initial_preset = None
    _initial_v_hosts = None
    _service_name = SERVICE_NAME

    def __init__(self):
        Handler.__init__(self)

        self.api = apache.ApacheAPI()

        self.preset_provider = ApachePresetProvider()
        preset_service.services[BEHAVIOUR] = self.preset_provider
        self._initial_preset = None
        self._initial_v_hosts = []

        self._queryenv = bus.queryenv_service

        bus.on(init=self.on_init)
        bus.define_events('apache_rpaf_reload')

    def on_init(self):
        bus.on(
            start=self.on_start,
            before_host_up=self.on_before_host_up,
            host_init_response=self.on_host_init_response,
            before_reboot_finish=self.on_before_reboot_finish,
        )

    def on_VhostReconfigure(self, message):
        """
        Message is deprecated since Scalr 4.4.0
        @param message: None
        """
        self.api.reload_virtual_hosts()

    def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
        return BEHAVIOUR in behaviour and (
            message.name == Messages.VHOST_RECONFIGURE or
            message.name == Messages.UPDATE_SERVICE_CONFIGURATION or
            message.name == Messages.HOST_UP or
            message.name == Messages.HOST_DOWN or
            message.name == Messages.BEFORE_HOST_TERMINATE)

    def on_host_init_response(self, message):
        if 'apache' in message.body:
            apache_data = message.body['apache']

            for virtual_host_data in apache_data.get('virtual_hosts', []):
                virtual_host_data['ssl'] = bool(int(virtual_host_data['ssl']))

                if not virtual_host_data['ssl']:
                    virtual_host_data['ssl_certificate_id'] = None  # Handling '0'

                self._initial_v_hosts.append(virtual_host_data)

            if 'preset' in apache_data:
                self._initial_preset = apache_data['preset']

    def on_before_host_up(self, message):
        self.api.init_service()

        if self._initial_v_hosts:
            LOG.debug('Configuring VirtualHosts: %s' % self._initial_v_hosts)
            applied_vhosts = self.api.reconfigure(self._initial_v_hosts)
            LOG.info('%s Virtual Hosts configured.' % len(applied_vhosts))

        self._rpaf_reload()

        bus.fire('service_configured', service_name=SERVICE_NAME, preset=self._initial_preset)

    def on_start(self):
        if __node__['state'] == ScalarizrState.RUNNING:
            try:
                self._rpaf_reload()
            except initdv2.InitdError, e:
                if 'not running' in str(e) and not self.api.service.running:
                    self.api.start_service()


    def on_before_reboot_finish(self, *args, **kwargs):
        self.api.reload_virtual_hosts()

    def on_HostUp(self, message):
        if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
            apache.ModRPAF.add([message.local_ip])
            self.api.reload_service('Applying new RPAF proxy IPs list')

    def on_HostDown(self, message):
        if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
            apache.ModRPAF.remove([message.local_ip])
            self.api.reload_service('Applying new RPAF proxy IPs list')

    def _rpaf_reload(self):
        lb_hosts = []
        for role in self._queryenv.list_roles(behaviour=BuiltinBehaviours.WWW):
            for host in role.hosts:
                lb_hosts.append(host.internal_ip)
        apache.ModRPAF.update(lb_hosts)
        self.api.reload_service('Applying new RPAF proxy IPs list')
        bus.fire('apache_rpaf_reload')

    on_BeforeHostTerminate = on_HostDown


class ApacheConf(BaseConfig):

    config_type = 'app'
    config_name = 'apache2.conf' if disttool.is_debian_based() else 'httpd.conf'


class ApachePresetProvider(PresetProvider):

    def __init__(self):
        api = apache.ApacheAPI()
        config_mapping = {'apache.conf': ApacheConf(apache)}
        PresetProvider.__init__(self, api.service, config_mapping)

    def rollback_hook(self):
        try:
            pwd.getpwnam('apache')
            uname = 'apache'
        except:
            uname = 'www-data'
        for obj in self.config_data:
            coreutils.chown_r(obj.path, uname)
