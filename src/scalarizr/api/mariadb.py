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

    behavior = 'mariadb'

    def __init__(self):
        super(MariaDBAPI, self).__init__()

    @classmethod
    def do_check_software(cls, installed_packages=None):
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
                raise exceptions.UnsupportedBehavior(cls.behavior, (
                    "Unsupported operating system '{os}'").format(os=linux.os['name'])
                )

    @classmethod
    def do_handle_check_software_error(cls, e):
        if isinstance(e, pkgmgr.VersionMismatchError):
            pkg, ver, req_ver = e.args[0], e.args[1], e.args[2]
            msg = (
                '{pkg}-{ver} is not supported on {os}. Supported:\n'
                '\tUbuntu, Debian, CentOS, OEL, RHEL, Amazon: {req_ver}').format(
                    pkg=pkg, ver=ver, os=linux.os['name'], req_ver=req_ver)
            raise exceptions.UnsupportedBehavior(cls.behavior, msg)
        else:
            raise exceptions.UnsupportedBehavior(cls.behavior, e)

