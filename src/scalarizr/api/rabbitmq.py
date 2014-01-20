'''
Created on Feb 25, 2011

@author: uty
'''


from scalarizr.util.cryptotool import pwgen
from scalarizr.services.rabbitmq import rabbitmq as rabbitmq_svc
from scalarizr import rpc


class RabbitMQAPI(object):

    def __init__(self):
        self.service = rabbitmq_svc.RabbitMQInitScript()

    @rpc.command_method
    def start_service(self):
        self.service.start()

    @rpc.command_method
    def stop_service(self):
        self.service.stop()

    @rpc.command_method
    def reload(self):
        self.service.reload()

    @rpc.command_method
    def restart(self):
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
        rabbitmq_svc.check_master_user(new_password)
        return new_password
