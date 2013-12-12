from __future__ import with_statement

import logging
from scalarizr import linux
from scalarizr.api import mysql
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class PerconaAPI(mysql.MySQLAPI):

    __metaclass__ = Singleton

    def __init__(self):
        super(MariaDBAPI, self).__init__()


    @classmethod
    def check_software(cls, installed=None):
        try:
            def check_any(pkgs, conflicting):
                for _ in pkgs:
                    try:
                        pkgmgr.check_dependency(_, installed, conflicting)
                        break
                    except:
                        continue
                else:
                    raise

            if linux.os.debian_family:
                check_any(
                        [
                            ['percona-server-client-5.1', 'percona-server-server-5.1'],
                            ['percona-server-client-5.5', 'percona-server-server-5.5'],
                        ],
                        ['mysql-server', 'mysql-client']
                        )
            elif linux.os.redhat_family or linux.os.oracle_family:
                check_any(
                        [
                            ['Percona-Server-client-5.1', 'Percona-Server-server-5.1'],
                            ['Percona-Server-client-5.5', 'Percona-Server-server-5.5'],
                        ],
                        ['mysql']
                        )
            else:
                raise exceptions.UnsupportedBehavior('percona',
                        "'percona' behavior is only supported on " +\
                        "Debian, RedHat or Oracle operating system family"
                        )
        except pkgmgr.NotInstalled as e:
            raise exceptions.UnsupportedBehavior('percona', 
                    'Percona >=5.1,<5.6 is not installed on %s' % linux.os['name'])
        except pkgmgr.VersionMismatch as e:
            raise exceptions.UnsupportedBehavior('Percona', str(
                    'Percona {} is not supported on {}. ' +\
                    'Supported: ' +\
                    'Percona >=5.1,<5.6 on Debian, RedHat or Oracle oprationg system family'
                    ).format(e.args[1], linux.os['name']))

