from __future__ import with_statement

import logging
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton, software
from scalarizr import exceptions


LOG = logging.getLogger(__name__)


class ChefAPI(object):

    __metaclass__ = Singleton

    @classmethod
    def check_software(cls, installed_packages=None):
        try:
            if linux.os.debian_family or linux.os.redhat_family or linux.os.oracle_family:
                pkgmgr.check_dependency(['chef'], installed_packages)
            else:
                raise exceptions.UnsupportedBehavior('chef',
                    "'chef' behavior is only supported on " +\
                    "Debian, RedHat or Oracle operating system family"
                )
        except pkgmgr.DependencyError as e:
            software.handle_dependency_error(e, 'chef')

