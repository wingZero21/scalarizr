import sys

from scalarizr.bus import bus
from scalarizr.adm.command import Command
from scalarizr.adm.command import CommandError
from scalarizr.adm.command import TAB_SIZE
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


service_to_behavior = {
    'nginx': 'www',
    'mysql': 'mysql2',
    'apache': 'app'
}


class ReturnCode:
    RUNNING = 0
    STOPPED = 3
    UNKNOWN = 4
    MIXED = 150


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

    aliases = ['s']

    def help(self):
        doc = super(Service, self).help()
        services = [(' '*TAB_SIZE) + s for s in service_apis.keys()]
        return doc + '\nSupported services:\n' + '\n'.join(services)

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
        code = ReturnCode.STOPPED
        if status == initdv2.Status.RUNNING:
            status_string = ' is running'
            code = ReturnCode.RUNNING
        elif status == initdv2.Status.UNKNOWN:
            status_string = ' has unknown status'
            code = ReturnCode.UNKNOWN
        print service + status_string
        return code

    def _print_redis_status(self, statuses):
        if not statuses:
            print 'No redis configuration found.'
            return ReturnCode.STOPPED

        for port, status in statuses.items():
            status_string = 'stopped'
            if status == initdv2.Status.RUNNING:
                status_string = 'running'
            print '- port: %s\n  status: %s' % (port, status_string)

        overall_status = set(statuses.values())
        if len(overall_status) > 1:
            return ReturnCode.MIXED
        if overall_status.pop() == initdv2.Status.RUNNING:
            return ReturnCode.RUNNING
        return ReturnCode.STOPPED

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
            return ReturnCode.UNKNOWN

        if service not in __node__['behavior'] and \
            service_to_behavior.get(service) not in __node__['behavior']:
            print 'Not installed service/behavior.'
            return ReturnCode.UNKNOWN

        if start:
            return self._start_service(service, indexes=index, ports=port)
        elif stop:
            return self._stop_service(service, indexes=index, ports=port)
        elif status:
            return self._display_service_status(service, indexes=index, ports=port)


commands = [Service]
