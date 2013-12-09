from __future__ import with_statement

import os
import logging
from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import software
from scalarizr.util import Singleton


LOG = logging.getLogger(__name__)


class TomcatAPI(object):

    __metaclass__ = Singleton

    @classmethod
    def check_software(cls, installed=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        if os_name == 'ubuntu':
            if os_vers >= '12':
                software.check_software(['tomcat7', 'tomcat7-admin'], installed)
            elif os_vers >= '10':
                software.check_software(['tomcat6', 'tomcat6-admin'], installed)
            else:
                raise software.SoftwareError('Unsupported version of operating system')
        elif os_name == 'debian':
            if os_vers >= '7':
                software.check_software(['tomcat7', 'tomcat7-admin'], installed)
            elif os_vers >= '6':
                software.check_software(['tomcat6', 'tomcat6-admin'], installed)
            else:
                raise software.SoftwareError('Unsupported version of operating system')
        elif os_name in ['centos', 'redhat', 'amazon']:
            software.check_software(['tomcat6', 'tomcat6-admin-webapps'], installed)
        else:
            raise software.SoftwareError('Unsupported operating system')

