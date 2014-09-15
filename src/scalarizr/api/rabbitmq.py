from scalarizr.util.cryptotool import pwgen
from scalarizr.services.rabbitmq import rabbitmq as rabbitmq_sgt
from scalarizr.services import rabbitmq as rabbitmq_module
from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import Singleton
from scalarizr.linux import pkgmgr
from scalarizr import exceptions
from scalarizr.api import BehaviorAPI


class RabbitMQAPI(BehaviorAPI):
    """
    Basic API for managing RabbitMQ service.

    Namespace::

        rabbitmq
    """
    __metaclass__ = Singleton

    behavior = 'rabbitmq'

    def __init__(self):
        self.service = rabbitmq_module.RabbitMQInitScript()
        self.rabbitmq = rabbitmq_sgt

    @rpc.command_method
    def start_service(self):
        """
        Starts RabbitMQ service.

        Example::

            api.rabbitmq.start_service()
        """
        self.service.start()

    @rpc.command_method
    def stop_service(self):
        """
        Stops RabbitMQ service.

        Example::

            api.rabbitmq.stop_service()
        """
        self.service.stop()

    @rpc.command_method
    def reload_service(self):
        """
        Reloads RabbitMQ configuration.

        Example::

            api.rabbitmq.reload_service()
        """
        self.service.reload()

    @rpc.command_method
    def restart_service(self):
        """
        Restarts RabbitMQ service.

        Example::

            api.rabbitmq.restart_service()
        """
        self.service.restart()

    @rpc.command_method
    def get_service_status(self):
        """
        Checks RabbitMQ service status.

        RUNNING = 0
        DEAD_PID_FILE_EXISTS = 1
        DEAD_VAR_LOCK_EXISTS = 2
        NOT_RUNNING = 3
        UNKNOWN = 4

        :return: Status num.
        :rtype: int


        Example::

            >>> api.rabbitmq.get_service_status()
            0
        """
        return self.service.status()

    @rpc.command_method
    def reset_password(self, new_password=None):
        """
        Resets password for RabbitMQ user 'scalr_master'.

        :param new_password: New password. If not provided, 10-character string will be generated.
        :type new_password: str

        :returns: New password.
        :rtype: str

        """
        if not new_password:
            new_password = pwgen(10)
        self.rabbitmq.check_master_user(new_password)
        return new_password

    @classmethod
    def do_check_software(cls, installed_packages=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        if os_name == 'ubuntu':
            if os_vers >= '12':
                pkgmgr.check_dependency(['rabbitmq-server>=3.0,<3.4'], installed_packages)
            elif os_vers >= '10':
                pkgmgr.check_dependency(['rabbitmq-server>=2.6,<2.7'], installed_packages)
        elif os_name == 'debian':
            pkgmgr.check_dependency(['rabbitmq-server>=3.0,<3.4'], installed_packages)
        elif linux.os.redhat_family:
            if os_vers >= '6':
                pkgmgr.check_dependency(['rabbitmq-server>=3.1,<3.4', 'erlang'], installed_packages)
            elif os_vers >= '5':
                raise exceptions.UnsupportedBehavior(cls.behavior,
                        "RabbitMQ doesn't supported on %s-5" % linux.os['name'])
        else:
            raise exceptions.UnsupportedBehavior(
                    cls.behavior, "Unsupported os family {0}".format(linux.os['family']))

