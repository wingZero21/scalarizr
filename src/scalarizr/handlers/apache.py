from __future__ import with_statement
'''
Created on Dec 25, 2009

@author: Dmytro Korsakov
@author: marat
'''

import os
import logging

from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.config import BuiltinBehaviours
from scalarizr.handlers import ServiceCtlHandler
from scalarizr.api import apache


LOG = logging.getLogger(__name__)
BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.APP


def get_handlers ():
    return [ApacheHandler()]



class ApacheHandler(ServiceCtlHandler):

    _queryenv = None


    def __init__(self):
        self.webserver = apache.ApacheWebServer()
        self.api = apache.ApacheAPI()
        bus.on(init=self.on_init, reload=self.on_reload)
        bus.define_events('apache_rpaf_reload')
        self.on_reload()


    def on_init(self):
        bus.on(
                start = self.on_start,
                before_host_up = self.on_before_host_up,
                host_init_response = self.on_host_init_response
        )
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


    def on_start(self):
        pass


    def on_before_host_up(self, message):
        self.update_vhosts()


    def on_HostUp(self, message):
        pass


    def on_HostDown(self, message):
        pass


    def on_VhostReconfigure(self, message):
        self.update_vhosts()


    def update_vhosts(self):
        received_vhosts = self._queryenv.list_virtual_hosts()
        LOG.debug('Received list of virtual hosts: %s' % str(received_vhosts))
        LOG.debug('List of currently served virtual hosts: %s' % str(self.webserver.list_served_vhosts()))

        '''
        if [vh.https for vh in received_vhosts if vh.https]:
            pass

        self._filter_vhosts_dir([self.get_vhost_filename(vh.hostname, vh.https) for vh in received_vhosts])
        '''

        for vhost_data in received_vhosts:
            hostname = vhost_data.hostname
            port = 443 if vhost_data.https else 80
            body = vhost_data.raw.replace('/etc/aws/keys/ssl', self.webserver.cert_path)
            if vhost_data.https:
                #prepare SSL Cert
                pass
            else:
                vhost = apache.ApacheVirtualHost(hostname, port, body)
                vhost.ensure()


    def _filter_vhosts_dir(self, white_list):
        pass


    def get_vhost_filename(self, hostname, ssl=False):
        end = apache.VHOST_EXTENSION if not ssl else '-ssl' + apache.VHOST_EXTENSION
        return os.path.join(bus.etc_path, apache.VHOSTS_PATH, hostname + end)


    on_BeforeHostTerminate = on_HostDown

