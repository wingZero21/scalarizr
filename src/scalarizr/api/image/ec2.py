import logging
import os
import shutil
import sys
import time

from scalarizr.api.image import ImageAPIDelegate
from scalarizr.api.image import ImageAPIError
from scalarizr.util import system2
from scalarizr.linux import mount
from scalarizr.linux import rsync
from scalarizr.storage2.util import loop
from scalarizr import linux
from scalarizr.storage2 import filesystem

_logger = logging.getLogger(__name__)


class InstanceStoreImageMaker(object):
    
    def __init__(self,
        image_name,
        role_name,
        excludes=[],
        size=None,
        bucket_name=None):

        self.image_name = image_name
        self.role_name = role_name
        self.excludes = excludes
        self.size = size
        self.bucket_name = bucket_name

    def prepare_image(self):
        # prepares imiage with ec2-bundle-vol command
        pass

    def upload_image(self):
        # upload image on S3 with ec2-upload-bundle or filetransfer
        pass

    def register_image(self):
        # register image as AMI with ec2-register
        pass

    def cleanup(self):
        # remove image from the server
        pass

    def create_image(self, name):
        self.prepare_image()
        self.upload_image()
        self.register_image()
        self.cleanup()  # ?

class EBSImageMaker(object):

    def __init__(self):
        pass

    def create_image(self):
        #create imaeg with ec2-create-image or through snapshotting server first
        pass
        # OR
        self.make_snapshot()
        self.register_image()

    def cleanup(self):
        pass


class EC2ImageAPIDelegate(ImageAPIDelegate):

    def __init__(self):
        self.image_maker = None

    def _get_root_device_type(self):
        pl = __node__['platform']
        ec2_conn = pl.new_ec2_conn()
        instance_id = pl.get_instance_id()
        try:
            instance = ec2_conn.get_all_instances([instance_id])[0].instances[0]
        except IndexError:
            msg = 'Failed to find instance %s. ' \
                'If you are importing this server, check that you are doing it from the ' \
                'right Scalr environment' % instance_id
            raise ImageAPIError(msg)

        return instance.root_device_type

    def _get_root_disk(self):
        # list of all mounted devices 
        devices = coreutils.df()

        # root device partition like `df(device='/dev/sda2', ..., mpoint='/')
        root_disk = None
        for device in devices:
            if device.mpoint == '/':
                return device
        
        raise ImageAPIError("Can't find root device")

    def prepare(self, operation, role_name):
        '''
        @param message.volume_size:
                New size for EBS-root device.
                By default current EBS-root size will be used (15G in most popular AMIs)
        @param message.volume_id
                EBS volume for root device copy.
        '''

        image_name = self._role_name + "-" + time.strftime("%Y%m%d%H%M%S")

        root_device_type = self._get_root_device_type()          
        root_disk = self._get_root_disk()

        if root_device_type == 'ebs':
            # EBS-root device instance
            # detecting root device like rdev=`sda`
            rdev = None
            for el in os.listdir('/sys/block'):
                if os.path.basename(root_disk.device) in os.listdir('/sys/block/%s'%el):
                    rdev = el
                    break
            if not rdev and os.path.exists('/sys/block/%s'%os.path.basename(root_disk.device)):
                rdev = root_disk.device

            # list partition of root device 
            list_rdevparts = [dev.device for dev in devices
                if dev.device.startswith('/dev/%s' % rdev)]

            if len(list(set(list_rdevparts))) > 1:
                # size of volume in KByte
                volume_size = system2(('sfdisk', '-s', root_disk.device[:-1]),)
                # size of volume in GByte
                volume_size = int(volume_size[0].strip()) / 1024 / 1024
                #TODO: need set flag, which be for few partitions
                #copy_partition_table = True
            else:
                # if one partition we use old method
                volume_size = self._rebundle_message.body.get('volume_size')
                if not volume_size:
                    volume_size = int(root_disk.size / 1000 / 1000)

            self._strategy = self._ebs_strategy_cls(
                    self, self._role_name, image_name, self._excludes,
                    volume_size=volume_size,  # in Gb
                    volume_id=self._rebundle_message.body.get('volume_id')
            )
        else:
            self.image_maker = InstanceStoreImageMaker(
                image_name,
                role_name,
                self.excludes,
                image_size=root_disk.size / 1000,
                s3_bucket_name=self._s3_bucket_name)


    def snapshot(self, operation, role_name):
        pass

    def finalize(self, operation, role_name):
        pass
