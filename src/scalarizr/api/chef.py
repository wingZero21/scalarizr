from scalarizr import rpc
from scalarizr.handlers.chef import ChefInitScript
from scalarizr.util import Singleton


class ChefAPI(object):

    __metaclass__ = Singleton

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
