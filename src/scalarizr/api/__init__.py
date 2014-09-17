import sys
import logging

from scalarizr import linux
from scalarizr import exceptions
from scalarizr.linux import pkgmgr

LOG = logging.getLogger(__name__)

api_routes = {
    'apache': 'scalarizr.api.apache.ApacheAPI',
    'app': 'scalarizr.api.apache.ApacheAPI',
    'chef': 'scalarizr.api.chef.ChefAPI',
    'haproxy': 'scalarizr.api.haproxy.HAProxyAPI',
    'mariadb': 'scalarizr.api.mariadb.MariaDBAPI',
    'memcached': 'scalarizr.api.memcached.MemcachedAPI',
    'mongodb': 'scalarizr.api.mongodb.MongoDBAPI',
    'mysql': 'scalarizr.api.mysql.MySQLAPI',
    'mysql2': 'scalarizr.api.mysql.MySQLAPI',
    'nginx': 'scalarizr.api.nginx.NginxAPI',
    'operation': 'scalarizr.api.operation.OperationAPI',
    'percona': 'scalarizr.api.percona.PerconaAPI',
    'postgresql': 'scalarizr.api.postgresql.PostgreSQLAPI',
    'rabbitmq': 'scalarizr.api.rabbitmq.RabbitMQAPI',
    'redis': 'scalarizr.api.redis.RedisAPI',
    'sysinfo': 'scalarizr.api.system.SystemAPI',
    'system': 'scalarizr.api.system.SystemAPI',
    'storage': 'scalarizr.api.storage.StorageAPI',
    'service': 'scalarizr.api.service.ServiceAPI',
    'tomcat': 'scalarizr.api.tomcat.TomcatAPI',
    'www': 'scalarizr.api.nginx.NginxAPI',
}


class DependencyError(Exception):
    pass


class BehaviorAPI(object):

    software_supported = False
    behavior = None

    @classmethod
    def check_software(cls, system_packages=None):
        try:
            if linux.os.windows and cls.behavior != 'chef':
                raise exceptions.UnsupportedBehavior(cls.behavior, (
                    "'{beh}' behavior is only supported on "
                    "Linux operation systems").format(beh=cls.behavior)
                )
            installed = cls.do_check_software(system_packages=system_packages)
            cls.software_supported = True
            msg = '{0}: Available. Installed version: {1}.'
            msg = msg.format(cls._software_name, installed[0][1])
            LOG.debug(msg)
        except:
            cls.software_supported = False
            e = sys.exc_info()[1]
            if isinstance(e, exceptions.UnsupportedBehavior):
                LOG.debug(e.args[1])
                raise
            elif isinstance(e, pkgmgr.NotInstalledError):
                msg = '{0}: Unavailable. Not installed.'.format(cls._software_name)
                LOG.debug(msg)
                raise exceptions.UnsupportedBehavior(cls.behavior, msg)
            elif isinstance(e, pkgmgr.VersionMismatchError):
                msg = '{0}: Unavailable. Installed version {1} is not supported by Scalr on {2} {3}.'
                packages = list()
                for package in e.args[0]:
                    if package[1]:
                        packages.append('{0}-{1}'.format(package[0], package[1]))
                    else:
                        packages.append(package[0])
                msg = msg.format(cls._software_name, ','.join(packages), linux.os['name'], linux.os['version'])
                LOG.debug(msg)
                raise exceptions.UnsupportedBehavior(cls.behavior, msg)
            elif isinstance(e, DependencyError):
                msg = '{0}: Unavailable. Installed, but missing additional dependencies: {1}.'
                packages = list()
                for package in e.args[0]:
                    if package[1]:
                        packages.append('{0} {1}'.format(package[0], package[1]))
                    else:
                        packages.append(package[0])
                msg = msg.format(cls._software_name, ','.join(packages))
                LOG.debug(msg)
                raise exceptions.UnsupportedBehavior(cls.behavior, msg)
            else:
                raise sys.exc_info()[0], sys.exc_info()[1], sys.exc_info()[2]

    @classmethod
    def do_check_software(cls, system_packages=None):
        raise NotImplementedError()

