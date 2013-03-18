
from __future__ import with_statement

from scalarizr import rpc
import scalarizr.libs.metaconf as metaconf


class NginxAPI(object):

    @rpc.service_method
    def add_proxy(self, addr, roles, servers):
        pass
