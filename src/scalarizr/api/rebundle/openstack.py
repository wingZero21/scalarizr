import logging
import sys

from scalarizr.api.rebundle import RebundleAPI
from scalarizr.util import software
from scalarizr.node import __node__


_logger = logging.getLogger(__name__)


class OpenStackWindowsRebundler(object):

    def prepare(self, message):
        # XXX: server is terminated during sysprep.
        # we should better understand how it works
        #shutil.copy(r'C:\Windows\System32\sysprep\RunSysprep_2.cmd', r'C:\windows\system32\sysprep\RunSysprep.cmd')
        #shutil.copy(r'C:\Windows\System32\sysprep\SetupComplete_2.cmd', r'C:\windows\setup\scripts\SetupComplete.cmd')
        #linux.system((r'C:\windows\system32\sysprep\RunSysprep.cmd', ))

        return software.system_info()


class OpenStackLinuxRebundler(object):

    def prepare(self):
        if os.path.exists('/etc/udev/rules.d/70-persistent-net.rules'):
            shutil.move('/etc/udev/rules.d/70-persistent-net.rules', '/tmp')

    def snapshot(self):
        image_name = self._role_name + "-" + time.strftime("%Y%m%d%H%M%S")
        nova = __node__['openstack']['new_nova_connection']
        nova.connect()

        server_id = __node__['openstack']['server_id']
        system2("sync", shell=True)
        _logger.info('Creating server image (server_id: %s)', server_id)
        image_id = nova.servers.create_image(server_id, image_name)
        _logger.info('Server image %s created', image_id)

        result = [None]
        def image_completed():
            try:
                result[0] = nova.images.get(image_id)
                return result[0].status in ('ACTIVE', 'FAILED')
            except:
                e = sys.exc_info()[1]
                if 'Unhandled exception occurred during processing' in str(e):
                    return
                raise

        wait_until(image_completed, start_text='Polling image status', sleep=30)

        image_id = result[0].id
        if result[0].status == 'FAILED':
            raise handlers.HandlerError('Image %s becomes FAILED', image_id)
        _logger.info('Image %s completed and available for use!', image_id)
        return image_id

    def finalize(self):
        if os.path.exists('/tmp/70-persistent-net.rules'):
            shutil.move('/tmp/70-persistent-net.rules', '/etc/udev/rules.d')


class OpenStackRebundleAPI(RebundleAPI):
    
    def __init__(self):
        if linux.os.windows_family:
            self._rebundler = OpenStackWindowsRebundler()
        else:
            self._rebundler = OpenStackLinuxRebundler()

    def _prepare(self):
        return self._rebundler.prepare()

    def _snapshot(self):
        return self._rebundler.snapshot()

    def _finalize(self):
        return self._rebundler.finalize()
