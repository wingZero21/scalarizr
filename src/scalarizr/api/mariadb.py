from __future__ import with_statement

import logging
from scalarizr import linux
from scalarizr.api import mysql
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton, software
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class MariaDBAPI(mysql.MySQLAPI):

    __metaclass__ = Singleton
    last_check = False

    def __init__(self):
        super(MariaDBAPI, self).__init__()

    @classmethod
    def check_software(cls, installed_packages=None):
        try:
            MariaDBAPI.last_check = False
            if linux.os.debian_family:
                pkgmgr.check_dependency(
                    ['mariadb-client>=5.5,<5.6', 'mariadb-server>=5.5,<5.6'],
                    installed_packages,
                    ['mysql-client']
                )
            elif linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_dependency(
                    ['MariaDB-client>=5.5,<5.6', 'MariaDB-server>=5.5,<5.6', 'gpg'],
                    installed_packages,
                    ['mysql']
                )
            else:
                raise exceptions.UnsupportedBehavior('mariadb',
                    "'mariadb' behavior is only supported on " +\
                    "Debian, RedHat or Oracle operating system family"
                )
            MariaDBAPI.last_check = True
        except pkgmgr.DependencyError as e:
            software.handle_dependency_error(e, 'mariadb')

