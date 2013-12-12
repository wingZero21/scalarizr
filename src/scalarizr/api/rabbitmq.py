'''
Created on Feb 25, 2011

@author: uty
'''


from scalarizr.util.cryptotool import pwgen
from scalarizr.services.rabbitmq import rabbitmq as rabbitmq_svc
from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import Singleton
from scalarizr.linux import pkgmgr
from scalarizr import exceptions


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
        try:
            if os_name == 'ubuntu':
                if os_vers >= '12':
                    pkgmgr.check_dependency(['rabbitmq-server>=3.0,<3.2'], installed)
                elif os_vers >= '10':
                    pkgmgr.check_dependency(['rabbitmq-server>=2.6,<2.7'], installed)
            elif os_name == 'debian':
                pkgmgr.check_dependency(['rabbitmq-server>=3.0,<3.2'], installed)
            elif linus.os.redhat_family:
                if os_vers >= '6':
                    pkgmgr.check_dependency(['rabbitmq>=3.1,<3.2', 'erlang'], installed)
                elif os_vers >= '5':
                    raise exceptions.UnsupportedBehavior('rabbitmq',
                            "RabbitMQ doesn't supported on %s-5" % linux.os['name'])
            else:
                raise exceptions.UnsupportedBehavior('rabbitmg',
                        "'rabbitmq' behavior is only supported on " +\
                        "Debian and RedHat operating system family"
                        )
        except pkgmgr.NotInstalled as e:
            raise exceptions.UnsupportedBehavior('rabbitmq', 
                    'RabbitMQ %s is not installed on %s' % (e.args[1], linux.os['name']))
        except pkgmgr.VersionMismatch as e:
            raise exceptions.UnsupportedBehavior('rabbitmq', str(
                    'RabbitMQ {} is not supported on {}. ' +\
                    'Supported: ' +\
                    'RabbitMQ >=2.6,<2.7 on Ubuntu-10.04, >=3.0,<3.2 on Ubuntu-12.04, ' +\
                    'RabbitMQ >=3.0,<3.2 on Debian, ' +\
                    'RabbitMQ >=3.1,<3.2 on CentOS-6, RedHat-6 and Amazon'
                    ).format(e.args[1], linux.os['name']))

