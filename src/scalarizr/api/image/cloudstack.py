import os
import time
import shutil
import logging

from scalarizr.api.image import ImageAPIDelegate
from scalarizr.api.image import ImageAPIError
from scalarizr.node import __node__
from scalarizr.platform.cloudstack import voltool


_logger = logging.getLogger(__name__)


class CloudStackImageAPIDelegate(ImageAPIDelegate):
    IMAGE_MPOINT = '/mnt/img-mnt'
    IMAGE_NAME_MAXLEN = 32

    def get_os_type_id(self, conn, instance_id):
        pl = __node__['platform']
        vm = conn.listVirtualMachines(id=instance_id)[0]
        return vm.guestosid

    def snapshot(self, op, role_name):
        now = time.strftime('%Y%m%d%H%M%S')
        if len(role_name) > self.IMAGE_NAME_MAXLEN - len(now) - 1:
            image_name = role_name[0:len(now)+2] + '--' + now
        else:
            image_name = role_name + "-" + now

        pl = __node__['platform']
        conn = pl.new_cloudstack_conn()

        root_vol = None
        instance_id = pl.get_instance_id()
        for vol in conn.listVolumes(virtualMachineId=instance_id):
            if vol.type == 'ROOT':
                root_vol = vol
                break
        else:
            raise ImageAPIError("Can't find root volume for virtual machine %s" % 
                instance_id)

        instance = conn.listVirtualMachines(id=instance_id)[0]

        _logger.info('Creating ROOT volume snapshot (volume: %s)', root_vol.id)
        snap = voltool.create_snapshot(conn,
            root_vol.id,
            wait_completion=True,
            logger=_logger)
        _logger.info('ROOT volume snapshot created (snapshot: %s)', snap.id)

        _logger.info('Creating image')
        image = conn.createTemplate(image_name, 
            image_name,
            self.get_os_type_id(conn, instance_id),
            snapshotId=snap.id,
            passwordEnabled=instance.passwordenabled)
        _logger.info('Image created (template: %s)', image.id)

        return image.id

    def prepare(self, op, role_name=None):
        if os.path.exists('/etc/udev/rules.d/70-persistent-net.rules'):
            shutil.move('/etc/udev/rules.d/70-persistent-net.rules', '/tmp')

    def after_rebundle(self, op, role_name=None):
        if os.path.exists('/tmp/70-persistent-net.rules'):
            shutil.move('/tmp/70-persistent-net.rules', '/etc/udev/rules.d')
