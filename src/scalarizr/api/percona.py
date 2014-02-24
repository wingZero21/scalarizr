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
            raise exceptions.UnsupportedBehavior(cls.behavior, (
                "Unsupported operating system family '{os}'").format(os=linux.os['name'])
            )

    @classmethod
    def do_handle_check_software_error(cls, e):
        if isinstance(e, pkgmgr.VersionMismatchError):
            pkg, ver, req_ver = e.args[0], e.args[1], e.args[2]
            msg = (
                '{pkg}-{ver} is not supported on {os}. Supported:\n'
                '\tUbuntu 12.04, Debian, RedHat, Oracle: >=5.1,<5.6').format(
                        pkg=pkg, ver=ver, os=linux.os['name'])
            raise exceptions.UnsupportedBehavior(cls.behavior, msg)
        else:
            raise exceptions.UnsupportedBehavior(cls.behavior, e)

