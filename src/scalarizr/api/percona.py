from __future__ import with_statement

import logging
from scalarizr import linux
from scalarizr.api import mysql
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton, software
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class PerconaAPI(mysql.MySQLAPI):

    __metaclass__ = Singleton
    last_check = False

    def __init__(self):
        super(PerconaAPI, self).__init__()

    @classmethod
    def check_software(cls, installed_packages=None):
        try:
            PerconaAPI.last_check = False
            if linux.os.debian_family:
                pkgmgr.check_any_dependency(
                    [
                        ['percona-server-client-5.1', 'percona-server-server-5.1'],
                        ['percona-server-client-5.5', 'percona-server-server-5.5'],
                    ],
                    installed_packages,
                    ['mysql-server', 'mysql-client']
                )
            elif linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_any_dependency(
                    [
                        ['Percona-Server-client-5.1', 'Percona-Server-server-5.1'],
                        ['Percona-Server-client-5.5', 'Percona-Server-server-5.5'],
                    ],
                    installed_packages,
                    ['mysql']
                )
            else:
                raise exceptions.UnsupportedBehavior('percona',
                    "'percona' behavior is only supported on " +\
                    "Debian, RedHat or Oracle operating system family"
                )
            PerconaAPI.last_check = True
        except pkgmgr.DependencyError as e:
            software.handle_dependency_error(e, 'percona')

