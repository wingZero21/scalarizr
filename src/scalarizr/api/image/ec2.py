import logging
import os
import shutil
import sys
import time
import subprocess

from scalarizr.api.image import ImageAPIDelegate
from scalarizr.api.image import ImageAPIError
from scalarizr.util import system2
from scalarizr import linux
from scalarizr.linux import mount
from scalarizr.linux import rsync
from scalarizr.storage2.util import loop
from scalarizr import linux
from scalarizr.storage2 import filesystem
from scalarizr.config import ScalarizrCnf
from scalarizr.node import __node__
from scalarizr.node import base_dir as etc_dir
from scalarizr.node import private_dir
from scalarizr.linux import coreutils

_logger = logging.getLogger(__name__)


class InstanceStoreImageMaker(object):
    
    def __init__(self,
        image_name,
        environ,
        excludes=[],
        image_size=None,
        bucket_name=None,
        destination='/mnt'):

        self.image_name = image_name
        self.environ = environ
        self.excludes = excludes
        self.image_size = image_size
        self.bucket_name = bucket_name
        self.destination = destination
        self.platform = __node__['platform']

        if not excludes:
            self.excludes = [
                self.destination,
                '/selinux/*',
                '/var/lib/dhclient',
                '/var/lib/dhcp',
                '/var/lib/dhcp3']

    def prepare_image(self):
        # prepares imiage with ec2-bundle-vol command
        cmd = (
            linux.which('ec2-bundle-vol'), 
            '--arch', linux.os['arch'],
            '--size', str(self.image_size),
            '--destination', self.destination,
            '--exclude', ','.join(self.excludes),
            '--prefix', self.image_name,
            '--volume', '/',
            '--debug')
        _logger.info('Image prepare command: ' + ' '.join(cmd))
        out = linux.system(cmd, 
            env=self.environ,
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT)[0]
        _logger.info('Image prepare command out: %s' % out)


    def upload_image(self):
        # upload image on S3 with ec2-upload-bundle or filetransfer
        _logger.info('Uploading image (with ec2-upload-bundle)')
        manifest = os.path.join(self.destination, self.image_name) + '.manifest.xml'
        bucket = os.path.basename(self.platform.scalrfs.root())
        cmd = (
            linux.which('euca-upload-bundle'),
            '--bucket', bucket,
            '--manifest', manifest)
        _logger.info('Image upload command: ', ' '.join(cmd))
        out = linux.system(cmd, env=self.environ)[0]
        _logger.info('Image upload command out: %s' % out)
        return bucket, manifest

    def register_image(self, bucket, manifest):
        # register image as AMI with ec2-register
        _logger.info('Registering image')
        s3_manifest_path = '%s/%s' % (bucket, os.path.basename(manifest))
        _logger.info("Registering image '%s'", s3_manifest_path)

        ec2_conn = self.platform.new_ec2_conn()
        ami_id = ec2_conn.register_image(image_location=s3_manifest_path)

        _logger.info("Image is registered.")
        _logger.debug('Image %s is available', ami_id)
        return ami_id

    def cleanup(self):
        # remove image from the server
        linux.system('chmod 755 %s/keys/euca-*' % private_dir, shell=True)
        linux.system('rm -f %s/keys/euca-*' % private_dir, shell=True)
        linux.system('rm -f %s/%s.*' % (self.destination, self.image_name), shell=True)

    def create_image(self):
        try:
            self.prepare_image()
            bucket, manifest = self.upload_image()
            image_id = self.register_image(bucket, manifest)
            return image_id
        finally:
            self.cleanup()


class EBSImageMaker(object):

    def __init__(self, image_name, environ):
        self.image_name = image_name
        self.environ = environ

    def create_image(self):
        #create imaeg with ec2-create-image or through snapshotting server first
        platform = __node__['platform']
        instance_id = platform.get_instance_id()
        cmd = (
            linux.which('ec2-create-image'), 
            instance_id,
            '--name', self.image_name,
            '--no-reboot',
            '--debug')
        _logger.info('Image create command: ' + ' '.join(cmd))
        out = linux.system(cmd, 
            env=self.environ,
            stdout=subprocess.PIPE, 
            stderr=subprocess.STDOUT)[0]
        _logger.info('Image create command out: %s' % out)
        return out


class EC2ImageAPIDelegate(ImageAPIDelegate):

    def __init__(self):
        self.image_maker = None
        self.environ = None

    def _get_root_device_type(self):
        platform = __node__['platform']
        ec2_conn = platform.new_ec2_conn()
        instance_id = platform.get_instance_id()
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

    def _setup_environment(self):
        platform = __node__['platform']
        cnf = ScalarizrCnf(etc_dir)
        cert, pk = platform.get_cert_pk()
        access_key, secret_key = platform.get_access_keys()

        cert_path = cnf.write_key('ec2-cert.pem', cert)
        pk_path = cnf.write_key('ec2-pk.pem', pk)
        cloud_cert_path = cnf.write_key('ec2-cloud-cert.pem', platform.get_ec2_cert())

        self.environ = os.environ.copy()
        self.environ.update({
            'EC2_CERT': cert_path,
            'EC2_PRIVATE_KEY': pk_path,
            'EC2_USER_ID': platform.get_account_id(),
            'AWS_ACCESS_KEY': access_key,
            'AWS_SECRET_KEY': secret_key,
            'EC2_URL': platform.get_access_data('ec2_url')})

    def _get_s3_bucket_name(self):
        platform = __node__['platform']
        return 'scalr2-images-%s-%s' % \
            (platform.get_region(), platform.get_account_id())

    def prepare(self, operation, role_name):
        '''
        @param message.volume_size:
                New size for EBS-root device.
                By default current EBS-root size will be used (15G in most popular AMIs)
        @param message.volume_id
                EBS volume for root device copy.
        '''

        image_name = role_name + "-" + time.strftime("%Y%m%d%H%M%S")

        root_device_type = self._get_root_device_type()          
        root_disk = self._get_root_disk()

        if root_device_type == 'ebs':
            # EBS-root device instance
            # detecting root device like rdev=`sda`
            # rdev = None
            # for el in os.listdir('/sys/block'):
            #     if os.path.basename(root_disk.device) in os.listdir('/sys/block/%s'%el):
            #         rdev = el
            #         break
            # if not rdev and os.path.exists('/sys/block/%s'%os.path.basename(root_disk.device)):
            #     rdev = root_disk.device

            # # list partition of root device 
            # list_rdevparts = [dev.device for dev in devices
            #     if dev.device.startswith('/dev/%s' % rdev)]

            # if len(list(set(list_rdevparts))) > 1:
            #     # size of volume in KByte
            #     volume_size = system2(('sfdisk', '-s', root_disk.device[:-1]),)
            #     # size of volume in GByte
            #     volume_size = int(volume_size[0].strip()) / 1024 / 1024
            #     #TODO: need set flag, which be for few partitions
            #     #copy_partition_table = True
            # else:
            #     # if one partition we use old method
            #     volume_size = self._rebundle_message.body.get('volume_size')
            #     if not volume_size:
            #         volume_size = int(root_disk.size / 1000 / 1000)

            self._strategy = EBSImageMaker(
                    image_name,
                    self.environ)
        else:
            self.image_maker = InstanceStoreImageMaker(
                image_name,
                role_name,
                self.excludes,
                image_size=root_disk.size / 1000,
                s3_bucket_name=self._get_s3_bucket_name())


    def snapshot(self, operation, role_name):
        image_id = self.image_maker.create_image()
        return image_id

    def finalize(self, operation, role_name):
        pass
