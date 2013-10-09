from __future__ import with_statement
'''
Created on Oct 24, 2011

@author: marat
'''

import os
import sys
import time
import json
import signal
import logging

from scalarizr import linux
from scalarizr.node import __node__
from scalarizr.bus import bus
from scalarizr.util import system2, initdv2, PopenError
from scalarizr.util.software import which
from scalarizr.handlers import Handler

if linux.os.windows_family:
    import win32service
    import win32serviceutil


WIN_SERVICE_NAME = 'chef-client'
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


PID_FILE = '/var/run/chef-client.pid'

class ChefInitScript(initdv2.ParametrizedInitScript):
    _default_init_script = '/etc/init.d/chef-client'

    def __init__(self):
        super(ChefInitScript, self).__init__('chef', None, PID_FILE)


    def start(self, env=None):
        self._env = env or os.environ
        super(ChefInitScript, self).start()


    # Uses only pid file, no init script involved
    def _start_stop_reload(self, action):
        chef_client_bin = which('chef-client')
        if action == "start":
            if not self.running:
                # Stop default chef-client init script
                if os.path.exists(self._default_init_script):
                    system2((self._default_init_script, "stop"), close_fds=True, preexec_fn=os.setsid, raise_exc=False)

                cmd = (chef_client_bin, '--daemonize', '--logfile', '/var/log/chef-client.log', '--pid', PID_FILE)
                try:
                    out, err, rcode = system2(cmd, close_fds=True, preexec_fn=os.setsid, env=self._env)
                except PopenError, e:
                    raise initdv2.InitdError('Failed to start chef: %s' % e)

                if rcode:
                    raise initdv2.InitdError('Chef failed to start daemonized. Return code: %s\nOut:%s\nErr:%s' %
                                             (rcode, out, err))

        elif action == "stop":
            if self.running:
                with open(self.pid_file) as f:
                    pid = int(f.read().strip())
                try:
                    os.getpgid(pid)
                except OSError:
                    os.remove(self.pid_file)
                else:
                    os.kill(pid, signal.SIGTERM)

    def restart(self):
        self._start_stop_reload("stop")
        self._start_stop_reload("start")

initdv2.explore('chef', ChefInitScript)


class ChefHandler(Handler):
    def __init__(self):
        bus.on(init=self.on_init)
        self.on_reload()

    def on_init(self, *args, **kwds):
        bus.on(
                host_init_response=self.on_host_init_response,
                before_host_up=self.on_before_host_up,
                reload=self.on_reload,
                start=self.on_start
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
        self._init_script = initdv2.lookup('chef')


    def on_start(self):
        if 'running' == __node__['state']:
            queryenv = bus.queryenv_service
            farm_role_params = queryenv.list_farm_role_params(__node__['farm_role_id'])
            params_dict = farm_role_params['params'].get('chef')
            if params_dict:
                daemonize = int(params_dict.get('daemonize', False))
                if daemonize:
                    self.run_chef_client(daemonize=True)


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
            self._global_variables[kv['name']] = kv['value'].encode('utf-8') if kv['value'] else ''

        if 'chef' in message.body and message.body['chef']:
            if linux.os.windows_family:
                self._chef_client_bin = r'C:\opscode\chef\bin\chef-client.bat'
            else:
                self._chef_client_bin = which('chef-client')   # Workaround for 'chef' behavior enabled, but chef not installed

            self._chef_data = message.chef.copy()
            if not self._chef_data.get('node_name'):
                self._chef_data['node_name'] = self.get_node_name()
            self._daemonize = self._chef_data.get('daemonize')

            self._with_json_attributes = self._chef_data.get('json_attributes')
            self._with_json_attributes = json.loads(self._with_json_attributes) if self._with_json_attributes else {}

            self._run_list = self._chef_data.get('run_list')
            if self._run_list:
                self._with_json_attributes['run_list'] = self._run_list
            elif self._chef_data.get('role'):
                self._with_json_attributes['run_list'] = ["role[%s]" % self._chef_data['role']]

            if linux.os.windows_family:
                try:
                    # Set startup type to 'manual' for chef-client service
                    hscm = win32service.OpenSCManager(None, None, win32service.SC_MANAGER_ALL_ACCESS)
                    try:
                        hs = win32serviceutil.SmartOpenService(hscm, WIN_SERVICE_NAME, win32service.SERVICE_ALL_ACCESS)
                        try:
                            snc = win32service.SERVICE_NO_CHANGE
                            # change only startup type
                            win32service.ChangeServiceConfig(hs, snc, win32service.SERVICE_DEMAND_START,
                                                                snc, None, None, 0, None, None, None, None)
                        finally:
                            win32service.CloseServiceHandle(hs)
                    finally:
                        win32service.CloseServiceHandle(hscm)

                    win32serviceutil.StopService(WIN_SERVICE_NAME)

                except:
                    e = sys.exc_info()[1]
                    self._logger.warning('Could not stop chef service: %s' % e)


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

                        # Register node
                        LOG.info('Registering Chef node')
                        try:
                            self.run_chef_client()
                        finally:
                            os.remove(self._validator_key_path)

                    if self._with_json_attributes:
                        try:
                            with op.step(self._step_execute_run_list):
                                with open(self._json_attributes_path, 'w+') as fp:
                                    json.dump(self._with_json_attributes, fp)

                                LOG.debug('Applying run_list')
                                self.run_chef_client(with_json_attributes=True)
                                msg.chef = self._chef_data
                        finally:
                            os.remove(self._json_attributes_path)

                    if self._daemonize:
                        with op.step('Running chef-client in daemonized mode'):
                            self.run_chef_client(daemonize=True)
                finally:
                    self._chef_data = None


    def run_chef_client(self, with_json_attributes=False, daemonize=False):
        if daemonize:
            if linux.os.windows_family:
                self._logger.info('Starting chef-client service')
                win32serviceutil.StartService(WIN_SERVICE_NAME)
            else:
                self._init_script.start(env=self._environ_variables)
            return

        cmd = [self._chef_client_bin]
        if with_json_attributes:
            cmd += ['--json-attributes', self._json_attributes_path]
        system2(cmd,
            close_fds=not linux.os.windows_family,
            log_level=logging.INFO,
            preexec_fn=not linux.os.windows_family and os.setsid or None,
            env=self._environ_variables
        )

    @property
    def _environ_variables(self):
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
        return environ

    def get_node_name(self):
        return __node__.get('hostname') or '{0}-{1}-{2}'.format(self._platform.name, self._platform.get_public_ip(), time.time())
