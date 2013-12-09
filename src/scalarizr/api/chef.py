from __future__ import with_statement

import os
import logging
from scalarizr import rpc
from scalarizr import linux
from scalarizr.util import software
from scalarizr.util import Singleton


LOG = logging.getLogger(__name__)


class ChefAPI(object):

    __metaclass__ = Singleton

    @classmethod
    def check_software(cls, installed=None):
        if linux.os['family'].lower() in ['debian', 'redhat']:
            software.check_software(['chef'], installed)
        else:
            raise software.SoftwareError('Unsupported operating system')

