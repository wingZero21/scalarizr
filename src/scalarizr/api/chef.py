import os
import signal

from scalarizr import rpc
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton, software, initdv2
from scalarizr import exceptions


class ChefInitScript(initdv2.ParametrizedInitScript):
    _default_init_script = '/etc/init.d/chef-client'

    def __init__(self):
        self._env = None
        super(ChefInitScript, self).__init__('chef', None, '/var/run/chef-client.pid')


    def start(self, env=None):
        self._env = env or os.environ
        super(ChefInitScript, self).start()


    # Uses only pid file, no init script involved
    def _start_stop_reload(self, action):
        chef_client_bin = linux.which('chef-client')
        if action == "start":
            if not self.running:
                # Stop default chef-client init script
                if os.path.exists(self._default_init_script):
                    linux.system(
                        (self._default_init_script, "stop"), 
                        close_fds=True, 
                        preexec_fn=os.setsid, 
                        raise_exc=False
                    )

                cmd = (chef_client_bin, '--daemonize', '--logfile', 
                        '/var/log/chef-client.log', '--pid', self.pid_file)
                try:
                    out, err, rcode = linux.system(cmd, close_fds=True, 
                                preexec_fn=os.setsid, env=self._env)
                except linux.LinuxError, e:
                    raise initdv2.InitdError('Failed to start chef: %s' % e)

                if rcode:
                    msg = (
                        'Chef failed to start daemonized. '
                        'Return code: %s\nOut:%s\nErr:%s'
                        )
                    raise initdv2.InitdError(msg % (rcode, out, err))

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


class ChefAPI(object):

    __metaclass__ = Singleton
    last_check = False

    def __init__(self):
        self.service = ChefInitScript()

    @rpc.command_method
    def start_service(self):
        self.service.start()

    @rpc.command_method
    def stop_service(self):
        self.service.stop()

    @rpc.command_method
    def reload_service(self):
        self.service.reload()

    @rpc.command_method
    def restart_service(self):
        self.service.restart()

    @rpc.command_method
    def get_service_status(self):
        return self.service.status()

    @classmethod
    def check_software(cls, installed_packages=None):
        try:
            ChefAPI.last_check = False
            if linux.os.debian_family or linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_dependency(['chef'], installed_packages)
            else:
                raise exceptions.UnsupportedBehavior('chef',
                    "'chef' behavior is only supported on " +\
                    "Debian, RedHat or Oracle operating system family"
                )
            ChefAPI.last_check = True
        except pkgmgr.DependencyError as e:
            software.handle_dependency_error(e, 'chef')

