import sys

from scalarizr.bus import bus
from scalarizr.adm.command import Command
from scalarizr.adm.command import CommandError
from scalarizr.node import __node__
from scalarizr.util import initdv2
from scalarizr.api.apache import ApacheInitScript
from scalarizr.handlers.chef import ChefInitScript
from scalarizr.services.haproxy import HAProxyInitScript
from scalarizr.handlers.memcached import MemcachedInitScript
from scalarizr.services.mysql import MysqlInitScript
from scalarizr.api.nginx import NginxInitScript
from scalarizr.services.postgresql import PgSQLInitScript
from scalarizr.services.rabbitmq import RabbitMQInitScript


service_scripts = {
    'apache': ApacheInitScript,
    'chef': ChefInitScript,
    'haproxy': HAProxyInitScript,
    'mariadb': MysqlInitScript,
    'memcached': MemcachedInitScript,
    # 'mongodb',
    'mysql': MysqlInitScript,
    'nginx': NginxInitScript,
    'percona': MysqlInitScript,
    'postgresql': PgSQLInitScript,
    'rabbitmq': RabbitMQInitScript,
    # 'redis',
    # 'tomcat': ,  # TODO: make ParametrizedInitScript subclass for tomcat
}


class Service(Command):
    """
    Usage:
        service (start | stop | status) redis [(<index> | --port=<port>)]
        service (start | stop | status) mongodb [(mongos | mongod | 
            configsrv | configsrv-2 | configsrv-3 | arbiter)]
        service (start | stop | status) <service>
    """

    def _start_service(self, service, **kwds):
        script = service_scripts[service]()
        try:
            script.start()
        except initdv2.InitdError, e:
            print 'Service start failed.\n' + e.message
            return int(CommandError())

    def _stop_service(self, service, **kwds):
        script = service_scripts[service]()
        try:
            script.stop()
        except initdv2.InitdError, e:
            print 'Service stop failed.\n' + e.message
            return int(CommandError())

    def _display_service_status(self, service, **kwds):
        script = service_scripts[service]()
        status = script.status()
        status_string = ' is stopped'
        if status == initdv2.Status.RUNNING:
            status_string = ' is running'
        elif status == initdv2.Status.UNKNOWN:
            status_string = ' has unknown status'
        print service + status_string

    def __call__(self, 
                 start=False,
                 stop=False,
                 status=False,
                 service=None,

                 redis=False,
                 index=None,
                 port=None,

                 mongodb=False,
                 mongos=False,
                 mongod=False,
                 configsrv=False,
                 configsrv_2=False,
                 configsrv_3=False,
                 arbiter=False,
                 ):
        if redis:
            service = 'redis'
        elif mongodb:
            mongo_component = ((mongos and 'mongos') or
                               (mongod and 'mongod') or
                               (configsrv and 'configsrv') or
                               (configsrv_2 and 'configsrv-2') or
                               (configsrv_3 and 'configsrv-3') or
                               (arbiter and 'arbiter'))
            # TODO: finish

        if service not in service_scripts:
            raise CommandError('Unknown service/behavior.')

        if start:
            self._start_service(service, index=index, port=port)
        elif stop:
            self._stop_service(service, index=index, port=port)
        elif status:
            self._display_service_status(service, index=index, port=port)


commands = [Service]
