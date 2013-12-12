from __future__ import with_statement

import logging
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class MemcachedAPI(object):

    __metaclass__ = Singleton

    @classmethod
    def check_software(cls, installed=None):
        try:
            if linux.os.debian_family or linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_dependency(['memcached'], installed)
            else:
                raise exceptions.UnsupportedBehavior('memcached',
                        "'memcached' behavior is only supported on " +\
                        "Debian, RedHat or Oracle operating system family"
                        )
        except pkgmgr.NotInstalled as e:
            raise exceptions.UnsupportedBehavior('memcached',
                    'Memcached is not installed on %s' % linux.os['name'])

