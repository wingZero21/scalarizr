import logging

from scalarizr import rpc
from scalarizr.api.operation import OperationAPI
from scalarizr.util import Singleton
from scalarizr.util import software
from scalarizr.node import __node__
from scalarizr import linux

from scalarizr.api import image
from image.openstack import OpenStackImageAPI
from image.ec2 import EC2ImageAPI


_logger = logging.getLogger(__name__)


WALL_MESSAGE = 'Server is going to make image'


class ImageAPI(object):

    __metaclass__ = Singleton

    def __init__(self):
        self._op_api = OperationAPI()

    @rpc.command_method
    def prepare(self, async=False):
        if not system2(('which', 'wall'), raise_exc=False)[2]:
                system2(('wall'), stdin=WALL_MESSAGE, raise_exc=False)
        prepare_result = self._op_api.run('api.image.prepare',
            func=self._prepare,
            async=async,
            exclusive=True)
        result = {}
        if prepare_result:
            result['prepare_result'] = prepare_result

        result.update(software.system_info())
        return result

    @rpc.command_method
    def snapshot(self, role_name, async=False):
        cnf = bus.cnf
        saved_state = cnf.state
        image_id = None
        try:
            cnf.state = ScalarizrState.REBUNDLING
            image_id = self._op_api.run('api.image.snapshot',
                func=self._snapshot,
                async=async,
                exclusive=True)
        finally:
            cnf.state = saved_state
        return image_id

    @rpc.command_method
    def finalize(self, async=False):
        self._op_api.run('api.image.finalize',
            func=self._finalize,
            async=async,
            exclusive=True)
        _logger.info('Image created. If you imported this server to Scalr, '
                     'you can terminate Scalarizr now.')

    @rpc.command_method
    def create(self, role_name, async=True):
        create_operation = self._op_api.create('api.image.create',
            func=self._create,
            func_kwds={'role_name': role_name},
            exclusive=True)
        if async:
            create_operation.run_async()
        else:
            create_operation.run()
        return create_operation.operation_id


    def _create(self, role_name):
        prepare_result = self.prepare()
        image_id = self.snapshot()
        finalize_result = self.finalize()

        result = {'image_id': image_id}
        if prepare_result:
            result.update(prepare_result)
        if finalize_result:
            relult.update(finalize_result)
        
        return result

    def _prepare(self):
        raise NotImplementedError()

    def _snapshot(self):
        raise NotImplementedError()

    def _finalize(self):
        raise NotImplementedError()


def get_api():
    platform_name = __node__['platform'].name
    if platform_name == 'openstack':
        return OpenStackImageAPI()
    elif platform_name = 'ec2':
        return EC2ImageAPI()
    # ...
    else:
        return None
