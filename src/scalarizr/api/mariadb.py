from __future__ import with_statement

import logging
from scalarizr import linux
from scalarizr.api import mysql
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class MariaDBAPI(mysql.MySQLAPI):

    __metaclass__ = Singleton

    def __init__(self):
        super(MariaDBAPI, self).__init__()


    @classmethod
    def check_software(cls, installed=None):
        try:
            if linux.os.debian_family:
                pkgmgr.check_dependency(
                        ['mariadb-client>=5.5,<5.6', 'mariadb-server>=5.5,<5.6'],
                        installed,
                        ['mysql-client']
                        )
            elif linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_dependency(
                        ['MariaDB-client>=5.5,<5.6', 'MariaDB-server>=5.5,<5.6', 'gpg'],
                        installed,
                        ['mysql']
                        )
            else:
                raise exceptions.UnsupportedBehavior('mariadb',
                        "'mariadb' behavior is only supported on " +\
                        "Debian, RedHat or Oracle operating system family"
                        )
        except pkgmgr.NotInstalled as e:
            raise exceptions.UnsupportedBehavior('mariadb', 
                    'MariaDB %s is not installed on %s' % (e.args[1], linux.os['name']))
        except pkgmgr.DependencyConflict as e:
            raise exceptions.UnsupportedBehavior('mariadb',
                    'MariaDB conflicts with %s-%s on %s' % (e.args[0], e.args[1], linux.os['name'])
        except pkgmgr.VersionMismatch as e:
            raise exceptions.UnsupportedBehavior('maria', str(
                    'MariaDB {} is not supported on {}. ' +\
                    'Supported: ' +\
                    'MariaDB >=5.5,<5.6 on Debian, RedHat or Oracle oprationg system family'
                    ).format(e.args[1], linux.os['name']))

