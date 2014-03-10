
from scalarizr import rpc
from scalarizr.api.operation import OperationAPI
from scalarizr.util import Singleton


class RebundleAPI(object):

    __metaclass__ = Singleton

    def __init__(self):
        self._op_api = OperationAPI()

    @rpc.command_method
    def prepare(self, async=False):
        self._op_api.run('api.rebundle.prepare',
                         func=self._prepare,
                         async=async,
                         exclusive=True)

    @rpc.command_method
    def snapshot(self, async=False):
        self._op_api.run('api.rebundle.snapshot',
                         func=self._snapshot,
                         async=async,
                         exclusive=True)

    @rpc.command_method
    def finalize(self, async=False):
        self._op_api.run('api.rebundle.finalize',
                         func=self._finalize,
                         async=async,
                         exclusive=True)

    def _prepare(self):
        raise NotImplementedError()

    def _snapshot(self):
        raise NotImplementedError()

    def _finalize(self):
        raise NotImplementedError()
