from scalarizr.util.cryptotool import pwgen
from scalarizr.services.rabbitmq import rabbitmq as rabbitmq_sgt
from scalarizr.services import rabbitmq as rabbitmq_module
from scalarizr import rpc
from scalarizr.util import Singleton

class RabbitMQAPI(object):

    __metaclass__ = Singleton

    def __init__(self):
        self.service = rabbitmq_module.RabbitMQInitScript()
        self.rabbitmq = rabbitmq_sgt

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

    @rpc.command_method
    def reset_password(self, new_password=None):
        """
        Reset password for RabbitMQ user 'scalr_master'. Return new password
        """
        if not new_password:
            new_password = pwgen(10)
        self.rabbitmq.check_master_user(new_password)
        return new_password
