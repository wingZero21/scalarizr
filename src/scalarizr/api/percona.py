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

    behavior = 'percona'

    def __init__(self):
        super(PerconaAPI, self).__init__()

    @classmethod
    def do_check_software(cls, installed_packages=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        if os_name == 'ubuntu':
            if os_vers >= '14':
                pkgmgr.check_any_dependency(
                    [
                        ['percona-server-client-5.1', 'percona-server-server-5.1'],
                        ['percona-server-client-5.5', 'percona-server-server-5.5'],
                        ['percona-server-client-5.6', 'percona-server-server-5.6'],
                    ],
                    installed_packages,
                    ['mysql-server', 'mysql-client']
                )
        elif linux.os.debian_family:
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
                    ['Percona-Server-client-51', 'Percona-Server-server-51'],
                    ['Percona-Server-client-55', 'Percona-Server-server-55'],
                ],
                installed_packages,
                ['mysql']
            )
        else:
            raise exceptions.UnsupportedBehavior(
                    cls.behavior, "Unsupported os family {0}".format(linux.os['family']))

