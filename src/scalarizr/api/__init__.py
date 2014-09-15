import sys

from scalarizr import linux
from scalarizr import exceptions
from scalarizr.linux import pkgmgr

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


class BehaviorAPI(object):

    software_supported = False
    behavior = None

    @classmethod
    def check_software(cls, installed_packages=None):
        try:
            if linux.os.windows and cls.behavior != 'chef':
                raise exceptions.UnsupportedBehavior(cls.behavior, (
                    "'{beh}' behavior is only supported on "
                    "Linux operation systems").format(beh=cls.behavior)
                )
            cls.do_check_software(installed_packages=installed_packages)
            cls.software_supported = True
        except:
            cls.software_supported = False
            e = sys.exc_info()[1]
            if isinstance(e, exceptions.UnsupportedBehavior):
                raise
            elif isinstance(e, pkgmgr.NotInstalledError):
                pkgs = e.args[0]
                tmp = []
                for pkg in pkgs:
                    if pkg[1]:
                        tmp.append('{pkg} {ver}'.format(pkg=pkg[0], ver=pkg[1]))
                    else:
                        tmp.append('{pkg}'.format(pkg=pkg[0]))
                if len(tmp) > 1:
                    msg = '{0} are not installed'.format(' or '.join(tmp))
                else:
                    msg = '{0} is not installed'.format(' or '.join(tmp))
                raise exceptions.UnsupportedBehavior(cls.behavior, msg)
            elif isinstance(e, pkgmgr.ConflictError):
                pkg, ver = e.args[0], e.args[1]
                msg = '{pkg}-{ver} conflicts on {os}'.format(
                        pkg=pkg, ver=ver, os=linux.os['name'])
                raise exceptions.UnsupportedBehavior(cls.behavior, msg)
            else:
                cls.do_handle_check_software_error(e)    

    @classmethod
    def do_check_software(cls, installed_packages=None):
        raise NotImplementedError()

    @classmethod
    def do_handle_check_software_error(cls, e):
        if isinstance(e, pkgmgr.VersionMismatchError):
            msg = []
            for pkg in e.args[0]:
                name, ver, req_ver = pkg
                msg.append((
                    '{name}-{ver} is not supported. Install {name} {req_ver}'
                    ).format(name=name, ver=ver, req_ver=req_ver))
            raise exceptions.UnsupportedBehavior(cls.behavior, '\n'.join(msg))
        else:
            raise exceptions.UnsupportedBehavior(cls.behavior, e)

