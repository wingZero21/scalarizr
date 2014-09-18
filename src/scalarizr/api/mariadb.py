from __future__ import with_statement

import sys
import logging
from scalarizr import linux
from scalarizr.api import mysql
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton
from scalarizr import exceptions
from scalarizr.api import DependencyError


LOG = logging.getLogger(__name__)


class MariaDBAPI(mysql.MySQLAPI):

    __metaclass__ = Singleton

    behavior = 'mariadb'

    def __init__(self):
        super(MariaDBAPI, self).__init__()

    @classmethod
    def do_check_software(cls, system_packages=None):
        if linux.os.debian_family:
            requirements_main = ['mariadb-server>=5.5,<5.6']
            requirements_dependencies = ['mariadb-client>=5.5,<5.6']
        elif linux.os.redhat_family or linux.os.oracle_family:
            requirements_main = ['MariaDB-server>=5.5,<5.6']
            requirements_dependencies = ['MariaDB-client>=5.5,<5.6', 'gpg']
        else:
            raise exceptions.UnsupportedBehavior(
                    cls.behavior,
                    "Not supported on {0} os family".format(linux.os['family']))
        installed = pkgmgr.check_software(requirements_main, system_packages)[0]
        try:
            pkgmgr.check_software(requirements_dependencies, system_packages)
            return installed
        except pkgmgr.NotInstalledError:
            e = sys.exc_info()[1]
            raise DependencyError(e.args[0])

