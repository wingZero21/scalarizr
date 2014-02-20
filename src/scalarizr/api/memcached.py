import logging

from scalarizr import rpc
from scalarizr.handlers.memcached import MemcachedInitScript
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton, software
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class MemcachedAPI(object):

    __metaclass__ = Singleton
    last_check = False

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
    def check_software(cls, installed_packages=None):
        try:
            MemcachedAPI.last_check = False
            if linux.os.debian_family or linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_dependency(['memcached'], installed_packages)
            else:
                raise exceptions.UnsupportedBehavior('memcached',
                    "'memcached' behavior is only supported on " +\
                    "Debian, RedHat or Oracle operating system family"
                )
            MemcachedAPI.last_check = True
        except pkgmgr.DependencyError as e:
            software.handle_dependency_error(e, 'memcached')

