'''
Created on Feb 25, 2011

@author: uty
'''


from scalarizr.util.cryptotool import pwgen
from scalarizr.services.rabbitmq import rabbitmq as rabbitmq_svc
from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import software
from scalarizr.util import Singleton


class RabbitMQAPI:

    __metaclass__ = Singleton

    @rpc.command_method
    def reset_password(self, new_password=None):
        """
        Reset password for RabbitMQ user 'scalr_master'. Return new password
        """
        if not new_password:
            new_password = pwgen(10)
        rabbitmq_svc.check_master_user(new_password)
        return new_password


    @classmethod
    def check_software(cls, installed=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        if os_name == 'ubuntu':
            if os_vers >= '12':
                software.check_software(['rabbitmq-server>=3.0,<3.2'], installed)
            elif os_vers >= '10':
                software.check_software(['rabbitmq-server>=2.6,<2.7'], installed)
            else:
                raise software.SoftwareError('Unsupported version of operating system')
        elif os_name == 'debian':
            software.check_software(['rabbitmq-server>=3.0,<3.2'], installed)
        elif os_name == 'centos':
            if os_vers >= '6':
                software.check_software(['rabbitmq>=3.1,<3.2', 'erlang'], installed)
            elif os_vers >= '5':
                raise software.SoftwareError("'rabbitmq' doesn't supported")
        elif os_name == 'redhat':
            if os_vers >= '6':
                software.check_software(['rabbitmq>=3.1,<3.2', 'erlang'], installed)
            elif os_vers >= '5':
                raise software.SoftwareError("'rabbitmq' doesn't supported")
        elif os_name == 'amazon':
            software.check_software(['rabbitmq>=3.1,<3.2', 'erlang'], installed)
        else:
            raise software.SoftwareError('Unsupported operating system')

