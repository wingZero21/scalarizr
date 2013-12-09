from __future__ import with_statement

import os
import logging
from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import software
from scalarizr.util import Singleton


LOG = logging.getLogger(__name__)


class PerconaAPI(object):

    __metaclass__ = Singleton

    @classmethod
    def check_software(cls, installed=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        if os_name == 'ubuntu':
            if os_vers >= '12':
                ver = '5.5'
            elif os_vers >= '10':
                ver = '5.1'
            else:
                raise software.SoftwareError('Unsupported version of operating system')
            software.check_software(
                    ['percona-server-client-%s' % ver, 'percona-server-server-%s' % ver],
                    installed,
                    excluded=['mysql-server', 'mysql-client']
                    )
        elif os_name == 'debian':
            software.check_software(
                    ['percona-server-client-5.5', 'percona-server-server-5.5'],
                    installed,
                    excluded=['mysql-server', 'mysql-client']
                    )
        elif os_name in ['centos', 'redhat', 'amazon']:
            if os_vers >= '6':
                ver = '55'
            elif os_ver >= '5':
                ver = '51'
            else:
                raise software.SoftwareError('Unsupported version of operating system')
            software.check_software(
                    ['Percona-Server-client-%s' % ver, 'Percona-Server-server-%s' % ver],
                    installed,
                    excluded=['mysql']
                    )
        else:
            raise software.SoftwareError('Unsupported operating system')

