import logging

from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import system2, initdv2, disttool
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton
from scalarizr import exceptions
from scalarizr.api import BehaviorAPI


LOG = logging.getLogger(__name__)


class MemcachedAPI(BehaviorAPI):

    __metaclass__ = Singleton

    behavior = 'memcached'

    def __init__(self):
        self.service = MemcachedInitScript()

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
    def do_check_software(cls, installed_packages=None):
        pkgmgr.check_dependency(['memcached'], installed_packages)

    @classmethod
    def do_handle_check_software_error(cls, e):
        raise exceptions.UnsupportedBehavior(cls.behavior, e)


class MemcachedInitScript(initdv2.ParametrizedInitScript):

    def __init__(self):
        pid_file = None
        if disttool.is_redhat_based():
            pid_file = "/var/run/memcached/memcached.pid"
        elif disttool.is_debian_based():
            pid_file = "/var/run/memcached.pid"

        initd_script = '/etc/init.d/memcached'
        if not os.path.exists(initd_script):
            msg = "Cannot find Memcached init script at %s. Make sure that memcached is installed"
            raise HandlerError(msg % initd_script)

        initdv2.ParametrizedInitScript.__init__(self,
                'memcached', initd_script, pid_file, socks=[initdv2.SockParam(11211)])

initdv2.explore('memcached', MemcachedInitScript)

