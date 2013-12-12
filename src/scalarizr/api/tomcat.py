from __future__ import with_statement

import os
import logging
from scalarizr import rpc
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class TomcatAPI(object):

    __metaclass__ = Singleton

    @classmethod
    def check_software(cls, installed=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        try:
            if os_name == 'ubuntu':
                if os_vers >= '12':
                    pkgmgr.check_dependency(['tomcat7', 'tomcat7-admin'], installed)
                elif os_vers >= '10':
                    pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin'], installed)
            elif os_name == 'debian':
                if os_vers >= '7':
                    pkgmgr.check_dependency(['tomcat7', 'tomcat7-admin'], installed)
                elif os_vers >= '6':
                    pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin'], installed)
            elif linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin-webapps'], installed)
            else:
                raise exceptions.UnsupportedBehavior('tomcat',
                        "'tomcat' behavior is only supported on " +\
                        "Debian, RedHat and Oracle operating system family"
                        )
        except pkgmgr.NotInstalled as e:
            raise exceptions.UnsupportedBehavior('tomcat',
                    'Tomcat is not installed on %s' % linux.os['name'])
        except pkgmgr.VersionMismatch as e:
            raise exceptions.UnsupportedBehavior('tomcat', str(
                    'Tomcat {} is not supported on {}. ' +\
                    'Supported: ' +\
                    'Tomcat ==6 on Ubuntu-10.04, ==7 on Ubuntu-12.04, ' +\
                    'Tomcat ==7 on Debian, ' +\
                    'Tomcat ==6 on CentOS, RedHat, Oracle, Amazon'
                    ).format(e.args[1], linux.os['name']))

