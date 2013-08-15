from __future__ import with_statement
'''
Created on Oct 24, 2011

@author: marat
'''

from __future__ import with_statement

import logging
import os
import sys

from scalarizr import linux
from scalarizr.node import __node__
from scalarizr.bus import bus
from scalarizr.util import system2
from scalarizr.util.software import which
from scalarizr.handlers import Handler

__import__('chef.api')
ChefAPI = sys.modules['chef.api'].ChefAPI


LOG = logging.getLogger(__name__)
CLIENT_CONF_TPL = '''
log_level        :info
log_location     STDOUT
chef_server_url  '%(server_url)s'
environment      '%(environment)s'
validation_client_name '%(validator_name)s'
node_name        '%(node_name)s'
'''


def get_handlers():
    return (ChefHandler(), )


class ChefHandler(Handler):
    def __init__(self):
        bus.on(init=self.on_init)
        self.on_reload()

    def on_init(self, *args, **kwds):
        bus.on(
                host_init_response=self.on_host_init_response,
                before_host_up=self.on_before_host_up,
                reload=self.on_reload
        )

    def on_reload(self):
        _is_win = linux.os.windows_family
        self._chef_client_bin = None
        self._chef_data = None
        self._client_conf_path = _is_win and r'C:\chef\client.rb' or '/etc/chef/client.rb'
        self._validator_key_path = _is_win and r'C:\chef\validation.pem' or '/etc/chef/validation.pem'
        self._client_key_path = _is_win and r'C:\chef\client.pem' or '/etc/chef/client.pem'
        self._json_attributes_path = _is_win and r'C:\chef\first-run.json' or '/etc/chef/first-run.json'
        self._with_json_attributes = False
        self._platform = bus.platform
        self._global_variables = {}


    def get_initialization_phases(self, hir_message):
        if 'chef' in hir_message.body:
            self._phase_chef = 'Bootstrap node with Chef'
            self._step_register_node = 'Register node'
            self._step_execute_run_list = 'Execute run list'
            return {'before_host_up': [{
                    'name': self._phase_chef,
                    'steps': [self._step_register_node,     self._step_execute_run_list]
            }]}

    def on_host_init_response(self, message):
        global_variables = message.body.get('global_variables') or []
        for kv in global_variables:
            self._global_variables[kv['name']] = kv['value'] or ''

        if 'chef' in message.body and message.body['chef']:
            if linux.os.windows_family:
                self._chef_client_bin = r'C:\opscode\chef\bin\chef-client.bat'
            else:
                self._chef_client_bin = which('chef-client')   # Workaround for 'chef' behavior enabled, but chef not installed

            self._chef_data = message.chef.copy()
            if not self._chef_data.get('node_name'):
                self._chef_data['node_name'] = self.get_node_name()
            self._with_json_attributes = self._chef_data.get('json_attributes')


    def on_before_host_up(self, msg):
        if not self._chef_data:
            return

        with bus.initialization_op as op:
            with op.phase(self._phase_chef):
                try:
                    with op.step(self._step_register_node):
                        # Create client configuration
                        _dir = os.path.dirname(self._client_conf_path)
                        if not os.path.exists(_dir):
                            os.makedirs(_dir)
                        with open(self._client_conf_path, 'w+') as fp:
                            fp.write(CLIENT_CONF_TPL % self._chef_data)
                        os.chmod(self._client_conf_path, 0644)

                        # Delete client.pem
                        if os.path.exists(self._client_key_path):
                            os.remove(self._client_key_path)

                        # Write validation cert
                        with open(self._validator_key_path, 'w+') as fp:
                            fp.write(self._chef_data['validator_key'])

                        if self._with_json_attributes:
                            with open(self._json_attributes_path, 'w+') as fp:
                                fp.write(self._chef_data['json_attributes'])

                        # Register node
                        LOG.info('Registering Chef node')
                        try:
                            self.run_chef_client(first_run=True)
                        finally:
                            os.remove(self._validator_key_path)
                            if self._with_json_attributes:
                                os.remove(self._json_attributes_path)

                    with op.step(self._step_execute_run_list):
                        LOG.info('Executing run list')

                        LOG.debug('Initializing Chef API client')
                        node_name = self._chef_data['node_name'].encode('ascii')
                        chef = ChefAPI(self._chef_data['server_url'], self._client_key_path, node_name)

                        LOG.debug('Loading node')
                        node = chef['/nodes/%s' % node_name]

                        LOG.debug('Updating run_list')
                        node['run_list'] = [u'role[%s]' % self._chef_data['role']]
                        chef.api_request('PUT', '/nodes/%s' % node_name, data=node)

                        LOG.debug('Applying run_list')
                        self.run_chef_client()

                        msg.chef = self._chef_data

                finally:
                    self._chef_data = None


    def run_chef_client(self, first_run=False):
        cmd = [self._chef_client_bin]
        if first_run and self._with_json_attributes:
            cmd += ['--json-attributes', self._json_attributes_path]
        environ = {
            'SCALR_INSTANCE_INDEX': __node__['server_index'],
            'SCALR_FARM_ID': __node__['farm_id'],
            'SCALR_ROLE_ID': __node__['role_id'],
            'SCALR_FARM_ROLE_ID': __node__['farm_role_id'],
            'SCALR_BEHAVIORS': ','.join(__node__['behavior']),
            'SCALR_SERVER_ID': __node__['server_id']
        }
        environ.update(os.environ)
        environ.update(self._global_variables)
        if linux.os.windows_family:
            # Windows env should contain only strings, unicode is not an option
            environ = dict((str(x), str(y)) for x, y in environ.items())

        system2(cmd, 
            close_fds=linux.os.linux_family,
            log_level=logging.INFO,
            preexec_fn=linux.os.linux_family and os.setsid or None,
            env=environ
        )


    def get_node_name(self):
        return '%s-%s' % (self._platform.name, self._platform.get_public_ip())
