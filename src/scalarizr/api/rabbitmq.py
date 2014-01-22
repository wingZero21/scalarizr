'''
Created on Feb 25, 2011

@author: uty
'''


from scalarizr.util.cryptotool import pwgen
from scalarizr.services.rabbitmq import rabbitmq as rabbitmq_svc
from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import Singleton, software
from scalarizr.linux import pkgmgr
from scalarizr import exceptions


class RabbitMQAPI:

    __metaclass__ = Singleton
    last_check = False

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
    def check_software(cls, installed_packages=None):
        try:
            RabbitMQAPI.last_check = False
            os_name = linux.os['name'].lower()
            os_vers = linux.os['version']
            if os_name == 'ubuntu':
                if os_vers >= '12':
                    pkgmgr.check_dependency(['rabbitmq-server>=3.0,<3.2'], installed_packages)
                elif os_vers >= '10':
                    pkgmgr.check_dependency(['rabbitmq-server>=2.6,<2.7'], installed_packages)
            elif os_name == 'debian':
                pkgmgr.check_dependency(['rabbitmq-server>=3.0,<3.2'], installed_packages)
            elif linus.os.redhat_family:
                if os_vers >= '6':
                    pkgmgr.check_dependency(['rabbitmq>=3.1,<3.2', 'erlang'], installed_packages)
                elif os_vers >= '5':
                    raise exceptions.UnsupportedBehavior('rabbitmq',
                            "RabbitMQ doesn't supported on %s-5" % linux.os['name'])
            else:
                raise exceptions.UnsupportedBehavior('rabbitmg',
                        "'rabbitmq' behavior is only supported on " +\
                        "Debian and RedHat operating system family"
                )
            RabbitMQAPI.last_check = True
        except pkgmgr.DependencyError as e:
            software.handle_dependency_error(e, 'rabbitmq')

