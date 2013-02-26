'''
Created on Feb 25, 2011

@author: uty
'''


from scalarizr.util.cryptotool import pwgen
from scalarizr.services.rabbitmq import rabbitmq as rabbitmq_svc


class RabbitMQAPI:

    def reset_password(self, new_password=None):
        """ Reset password for RabbitMQ user 'scalr'. Return new password  """
        if not new_password:
            new_password = pwgen(10)
        rabbitmq_svc.set_user_password('scalr', new_password)
        return new_password
