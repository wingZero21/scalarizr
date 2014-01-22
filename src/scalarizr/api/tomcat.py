from __future__ import with_statement

import os
import logging
from scalarizr import rpc
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton, software
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class TomcatAPI(object):

    __metaclass__ = Singleton
    last_check = False

    @classmethod
    def check_software(cls, installed_packages=None):
        try:
            TomcatAPI.last_check = False
            os_name = linux.os['name'].lower()
            os_vers = linux.os['version']
            if os_name == 'ubuntu':
                if os_vers >= '12':
                    pkgmgr.check_dependency(['tomcat7', 'tomcat7-admin'], installed_packages)
                elif os_vers >= '10':
                    pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin'], installed_packages)
            elif os_name == 'debian':
                if os_vers >= '7':
                    pkgmgr.check_dependency(['tomcat7', 'tomcat7-admin'], installed_packages)
                elif os_vers >= '6':
                    pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin'], installed_packages)
            elif linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin-webapps'], installed_packages)
            else:
                raise exceptions.UnsupportedBehavior('tomcat',
                    "'tomcat' behavior is only supported on " +\
                    "Debian, RedHat and Oracle operating system family"
                )
            TomcatAPI.last_check = True
        except pkgmgr.DependencyError as e:
            software.handle_dependency_error(e, 'tomcat')

