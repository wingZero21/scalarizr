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
    'tomcat': TomcatAPI,
}


class Service(Command):
    """
    Scalarizr service control.

    Usage:
        service redis (start | stop | status) [(<index> | --port=<port>)]
        service <service> (start | stop | status)

    Options:
      -p <port>, --port=<port>         
    """
    # TODO: add usage for mongo:
    # service mongodb (start | stop | status) [(mongos | mongod | 
    #        configsrv | configsrv-2 | configsrv-3 | arbiter)]
    
    # status return codes
    RUNNING_RETURN_CODE = 0
    STOPPED_RETURN_CODE = 3
    UNKNOWN_RETURN_CODE = 4
    MIXED_RETURN_CODE = 150

    aliases = ['s']

    def _start_service(self, service, **kwds):
        api = service_apis[service]()
        for key, value in kwds.items():
            if value == None:
                kwds.pop(key)
        try:
            print 'Starting %s' % service
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
            print 'Stopping %s' % service
            api.stop_service(**kwds)
        except (BaseException, Exception), e:
            print 'Service stop failed.\n%s' % e
            return int(CommandError())

    def _display_service_status(self, service, **kwds):
        api = service_apis[service]()
        for key, value in kwds.items():
            if value == None:
                kwds.pop(key)
        status = api.get_service_status(**kwds)

        if service == 'redis':
            return self._print_redis_status(status)

        status_string = ' is stopped'
        code = self.STOPPED_RETURN_CODE
        if status == initdv2.Status.RUNNING:
            status_string = ' is running'
            code = self.RUNNING_RETURN_CODE
        elif status == initdv2.Status.UNKNOWN:
            status_string = ' has unknown status'
            code = self.UNKNOWN_RETURN_CODE
        print service + status_string
        return code

    def _print_redis_status(self, statuses):
        if not statuses:
            print 'No redis configuration found.'
            return self.STOPPED_RETURN_CODE

        for port, status in statuses.items():
            status_string = 'stopped'
            if status == initdv2.Status.RUNNING:
                status_string = 'running'
            print '- port: %s\n  status: %s' % (port, status_string)

        overall_status = set(statuses.values())
        if len(overall_status) > 1:
            return self.MIXED_RETURN_CODE
        if overall_status.pop() == initdv2.Status.RUNNING:
            return self.RUNNING_RETURN_CODE
        return self.STOPPED_RETURN_CODE

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
            print 'Unknown service/behavior.'
            return self.UNKNOWN_RETURN_CODE

        if service not in __node__['behavior']:
            print 'Not installed service/behavior.'
            return self.UNKNOWN_RETURN_CODE

        if start:
            return self._start_service(service, indexes=index, ports=port)
        elif stop:
            return self._stop_service(service, indexes=index, ports=port)
        elif status:
            return self._display_service_status(service, indexes=index, ports=port)


commands = [Service]
