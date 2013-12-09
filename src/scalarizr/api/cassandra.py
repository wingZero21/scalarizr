from __future__ import with_statement

import os
import logging
from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import software
from scalarizr.util import Singleton


LOG = logging.getLogger(__name__)


class CassandraAPI(object):

    __metaclass__ = Singleton

    @classmethod
    def check_software(cls, installed=None):
        os_name = linux.os['name'].lower()
        if os_name in ['ubuntu', 'debian']:
            software.check_software(['cassandra'], installed)
        elif os_name in ['centos', 'redhat', 'amazon']:
            software.check_software(['apache-cassandra'], installed)
        else:
            raise software.SoftwareError('Unsupported operating system')

