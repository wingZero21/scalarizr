import logging

from scalarizr import rpc
from scalarizr.handlers.memcached import MemcachedInitScript
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton, software
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

