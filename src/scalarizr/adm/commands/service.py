import sys

from scalarizr.bus import bus
from scalarizr.adm.command import Command
from scalarizr.adm.command import CommandError
from scalarizr.node import __node__
from scalarizr.util import initdv2
from scalarizr.api.apache import ApacheAPI
from scalarizr.api.chef import ChefAPI
from scalarizr.api.haproxy import HAProxyAPI
from scalarizr.api.memcached import MemcachedAPI
from scalarizr.api.mysql import MySQLAPI
from scalarizr.api.nginx import NginxAPI
from scalarizr.api.postgresql import PostgreSQLAPI
from scalarizr.api.rabbitmq import RabbitMQAPI
from scalarizr.api.redis import RedisAPI
from scalarizr.api.tomcat import TomcatAPI

service_apis = {
    'apache': ApacheAPI,
    'chef': ChefAPI,
    'haproxy': HAProxyAPI,
    'mariadb': MySQLAPI,
    'memcached': MemcachedAPI,
    # 'mongodb',
    'mysql': MySQLAPI,
    'nginx': NginxAPI,
    'percona': MySQLAPI,
    'postgresql': PostgreSQLAPI,
    'rabbitmq': RabbitMQAPI,
    'redis': RedisAPI,
    'tomcat': TomcatAPI, `
}


class Service(Command):
    """
    Usage:
        service (start | stop | status) redis [(<index> | --port=<port>)]
        service (start | stop | status) mongodb [(mongos | mongod | 
            configsrv | configsrv-2 | configsrv-3 | arbiter)]
        service (start | stop | status) <service>
    """

    aliases = ['s']

    def _start_service(self, service, **kwds):
        api = service_apis[service]()
        for key, value in kwds.items():
            if value == None:
                kwds.pop(key)
        try:
            api.start_service(**kwds)
        except (BaseException, Exception), e:
            print 'Service start failed.\n%s' % e
            return int(CommandError())

    def _stop_service(self, service, **kwds):
        api = service_apis[service]()
        for key, value in kwds.items():
            if value == None:
                kwds.pop(key)
        try:
            api.stop_service(**kwds)
        except (BaseException, Exception), e:
            print 'Service stop failed.\n%s' % e
            return int(CommandError())

    def _display_service_status(self, service):
        api = service_apis[service]()
        status = api.get_service_status()

        if service == 'redis':
            return self._print_redis_status(status)

        status_string = ' is stopped'
        code = 3
        if status == initdv2.Status.RUNNING:
            status_string = ' is running'
            code = 0
        elif status == initdv2.Status.UNKNOWN:
            status_string = ' has unknown status'
            code = 4
        print service + status_string
        return code

    def _print_redis_status(self, statuses):
        if not statuses:
            print 'No redis configuration found.'
            return 0
        for port, status in statuses.items():
            status_string = 'stopped'
            if status == initdv2.Status.RUNNING:
                status_string = 'running'
            print '- port: %s\n  status: %s' % (port, status_string)
        return 0

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
            if index:
                if '-' in index:
                    _start, _end = map(int, index.split('-'))
                    index = range(_start-1, _end)
                else:
                    index = int(index) - 1
            elif port and '-' in port:
                _start, _end = map(int, port.split('-'))
                port = range(_start-1, _end)
        elif mongodb:
            mongo_component = ((mongos and 'mongos') or
                               (mongod and 'mongod') or
                               (configsrv and 'configsrv') or
                               (configsrv_2 and 'configsrv-2') or
                               (configsrv_3 and 'configsrv-3') or
                               (arbiter and 'arbiter'))
            # TODO: finish

        if service not in service_apis:
            raise CommandError('Unknown service/behavior.')

        if start:
            return self._start_service(service, indexes=index, ports=port)
        elif stop:
            return self._stop_service(service, indexes=index, ports=port)
        elif status:
            return self._display_service_status(service)


commands = [Service]
