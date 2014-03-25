import logging
import os
import shutil

from scalarizr import linux
from scalarizr import rpc
from scalarizr.api.operation import OperationAPI
from scalarizr.linux import coreutils
from scalarizr.node import __node__
from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.util import Singleton
from scalarizr.util import software
from scalarizr.util import system2

from scalarizr.api.image.delegate import ImageAPIDelegate
from scalarizr.api.image.openstack import OpenStackImageAPIDelegate
from scalarizr.api.image.rackspace import RackspaceImageAPIDelegate
from scalarizr.api.image.ec2 import EC2ImageAPIDelegate


_logger = logging.getLogger(__name__)


WALL_MESSAGE = 'Server is going to make image'


class ImageAPIError(BaseException):
    pass


class ImageAPI(object):

    __metaclass__ = Singleton

    def __init__(self):
        self._op_api = OperationAPI()
        self.delegate = None

    def init_delegate(self):
        if self.delegate:
            return

        platform_name = __node__['platform'].name
        if platform_name == 'openstack':
            self.delegate = OpenStackImageAPIDelegate()
        elif platform_name == 'ec2':
            self.delegate = EC2ImageAPIDelegate()
        elif platform_name == 'rackspace':
            self.delegate = RackspaceImageAPIDelegate()
        # ...
        else:
            self.delegate = None

    @rpc.command_method
    def prepare(self, async=False):
        self.init_delegate()
        if not system2(('which', 'wall'), raise_exc=False)[2]:
            system2(('wall'), stdin=WALL_MESSAGE, raise_exc=False)
        prepare_result = self._op_api.run('api.image.prepare',
            func=self.delegate.prepare,
            async=async,
            exclusive=True)
        result = {}
        if prepare_result:
            result['prepare_result'] = prepare_result

        result.update(software.system_info())
        return result

    @rpc.command_method
    def snapshot(self, role_name, async=False):
        self.init_delegate()
        cnf = bus.cnf
        saved_state = cnf.state
        image_id = None
        try:
            cnf.state = ScalarizrState.REBUNDLING
            image_id = self._op_api.run('api.image.snapshot',
                func=self.delegate.snapshot,
                async=async,
                exclusive=True)
        finally:
            cnf.state = saved_state
        return image_id

    @rpc.command_method
    def finalize(self, async=False):
        self.init_delegate()
        self._op_api.run('api.image.finalize',
            func=self.delegate.finalize,
            async=async,
            exclusive=True)
        _logger.info('Image created. If you imported this server to Scalr, '
                     'you can terminate Scalarizr now.')

    @rpc.command_method
    def create(self, role_name, async=True):
        """ Creates image """
        self.init_delegate()
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
        prepare_result = self.prepare(role_name)
        image_id = self.snapshot(role_name)
        finalize_result = self.finalize(role_name)

        result = {'image_id': image_id}
        if prepare_result:
            result.update(prepare_result)
        if finalize_result:
            result.update(finalize_result)
        
        return result

    def _clean(self, image_rootdir):
        # TODO: revise method, rewrite if needed
        _logger.info('Performing image cleanup')
        # Truncate logs

        _logger.debug('Truncating log files')
        logs_path = os.path.join(image_rootdir, 'var/log')
        if os.path.exists(logs_path):
            for basename in os.listdir(logs_path):
                filename = os.path.join(logs_path, basename)
                if os.path.isfile(filename):
                    try:
                        coreutils.truncate(filename)
                    except OSError, e:
                        _logger.error("Cannot truncate file '%s'. %s", filename, e)
            shutil.rmtree(os.path.join(logs_path, 'scalarizr/scripting'))

        # Cleanup users homes
        _logger.debug('Removing users activity')
        for homedir in ('root', 'home/ubuntu', 'home/scalr'):
            homedir = os.path.join(image_rootdir, homedir)
            self._clean_user_activity(homedir)
            self._clean_ssh_keys(homedir)

        # Cleanup scalarizr private data
        _logger.debug('Removing scalarizr private data')
        etc_path = os.path.join(image_rootdir, bus.etc_path[1:])
        privated = os.path.join(etc_path, "private.d")
        if os.path.exists(privated):
            shutil.rmtree(privated)
            os.mkdir(privated)

        # Sync filesystem buffers
        system2('sync')

        _logger.debug('Cleanup completed')

    def _clean_user_activity(self, homedir):
        for name in (
            ".bash_history",
            ".lesshst",
            ".viminfo",
            ".mysql_history",
            ".history",
            ".sqlite_history"):
            filename = os.path.join(homedir, name)
            if os.path.exists(filename):
                os.remove(filename)


    def _clean_ssh_keys(self, homedir):
        filename = os.path.join(homedir, '.ssh/authorized_keys')
        if os.path.exists(filename):
            _logger.debug('Removing Scalr SSH keys from %s', filename)
            with open(filename + '.tmp', 'w+') as dest:
                with open(filename) as source:
                    lines = [l for l in source if 'SCALR-ROLESBUILDER' not in l]
                    dest.writelines(lines)
            os.rename(filename + '.tmp', filename)
    
