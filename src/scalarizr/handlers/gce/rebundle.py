from __future__ import with_statement
from __future__ import with_statement

__author__ = 'Nick Demyanchuk'

import re
import os
import sys
import uuid
import time
import random
import logging
import shutil
import pexpect
import tempfile

from scalarizr.bus import bus
from scalarizr import storage2
from scalarizr.storage2.cloudfs import FileTransfer
from scalarizr.util import wait_until, capture_exception
from scalarizr.linux import mount, system
from scalarizr.handlers import HandlerError, rebundle as rebundle_hndlr
from scalarizr.linux.tar import Tar
from scalarizr.linux.rsync import rsync
from scalarizr.linux import coreutils


def get_handlers():
    return [GceRebundleHandler()]


LOG = logging.getLogger(__name__)

ROLEBUILDER_USER = 'scalr-rolesbuilder'

class GceRebundleHandler(rebundle_hndlr.RebundleHandler):
    exclude_dirs = set(['/tmp', '/proc', '/dev',
                                       '/mnt' ,'/var/lib/google/per-instance',
                                       '/sys', '/cdrom', '/media'])
    exclude_files = ('/etc/ssh/.host_key_regenerated',
                                     '/lib/udev/rules.d/75-persistent-net-generator.rules')

    def rebundle(self):
        rebundle_dir = tempfile.mkdtemp()

        try:
            pl = bus.platform
            proj_id = pl.get_numeric_project_id()
            proj_name = pl.get_project_id()
            cloudstorage = pl.new_storage_client()

            tmp_mount_dir = os.path.join(rebundle_dir, 'root')
            os.makedirs(tmp_mount_dir)

            image_name      = 'disk.raw'
            image_path      = os.path.join(rebundle_dir, image_name)

            root_size = coreutils.statvfs('/')['size']
            LOG.debug('Creating image file %s' % image_path)
            with open(image_path, 'w') as f:
                f.truncate(root_size + 1*1024)

            try:

                LOG.debug('Creating partition table on image')
                system(('parted', image_path, 'mklabel', 'msdos'))
                system(('parted', image_path, 'mkpart', 'primary', 'ext2', 1, str(root_size/(1024*1024))))

                # Map disk image
                out = system(('kpartx', '-av', image_path))[0]
                try:
                    loop = re.search('(/dev/loop\d+)', out).group(1)
                    root_dev_name = '/dev/mapper/%sp1' % loop.split('/')[-1]

                    LOG.info('Creating filesystem')
                    storage2.filesystem('ext4').mkfs(root_dev_name)
                    dev_uuid = uuid.uuid4()
                    system(('tune2fs', '-U', str(dev_uuid), root_dev_name))

                    mount.mount(root_dev_name, tmp_mount_dir)
                    try:
                        lines = system(('/bin/mount', '-l'))[0].splitlines()
                        exclude_dirs = set()
                        for line in lines:
                            mpoint = line.split()[2]
                            if mpoint != '/':
                                exclude_dirs.add(mpoint)

                        exclude_dirs.update(self.exclude_dirs)

                        excludes = [os.path.join(ex, '**') for ex in exclude_dirs]
                        excludes.extend(self.exclude_files)
                        excludes.extend(self._excludes)

                        LOG.info('Copying root filesystem to image')
                        rsync('/', tmp_mount_dir, archive=True,
                                                                          hard_links=True,
                                                                          times=True,
                                                                          sparse=True,
                                                                          exclude=excludes)

                        LOG.info('Cleanup image')
                        self._create_spec_devices(tmp_mount_dir)

                        LOG.debug('Removing roles-builder user')
                        sh = pexpect.spawn('/bin/sh')
                        try:
                            sh.sendline('chroot %s' % tmp_mount_dir)
                            sh.expect('#')
                            sh.sendline('userdel -rf %s' % ROLEBUILDER_USER)
                            sh.expect('#')
                        finally:
                            sh.close()

                        """ Patch fstab"""
                        fstab_path = os.path.join(tmp_mount_dir, 'etc/fstab')
                        if os.path.exists(fstab_path):
                            with open(fstab_path) as f:
                                fstab = f.read()

                            new_fstab = re.sub('UUID=\S+\s+/\s+(.*)', 'UUID=%s / \\1' % dev_uuid, fstab)

                            with open(fstab_path, 'w') as f:
                                f.write(new_fstab)

                    finally:
                        mount.umount(root_dev_name)
                finally:
                    system(('kpartx', '-d', image_path))

                LOG.info('Compressing image.')
                arch_name = '%s.tar.gz' % self._role_name.lower()
                arch_path = os.path.join(rebundle_dir, arch_name)

                tar = Tar()
                tar.create().gzip().sparse()
                tar.archive(arch_path)
                tar.add(image_name, rebundle_dir)
                system(str(tar), shell=True)

            finally:
                os.unlink(image_path)

            try:
                LOG.info('Uploading compressed image to cloud storage')
                tmp_bucket_name = 'scalr-images-%s-%s' % (random.randint(1, 1000000), int(time.time()))
                remote_path = 'gcs://%s/%s' % (tmp_bucket_name, arch_name)
                arch_size = os.stat(arch_path).st_size
                uploader = FileTransfer(src=arch_path, dst=remote_path)

                try:
                    upload_result = uploader.run()
                    if upload_result['failed']:
                        errors =  [str(failed['exc_info'][1]) for failed in upload_result['failed']]
                        raise HandlerError('Image upload failed. Errors:\n%s' % '\n'.join(errors))
                    assert arch_size == upload_result['completed'][0]['size']
                except:
                    with capture_exception(LOG):
                        objs = cloudstorage.objects()
                        objs.delete(bucket=tmp_bucket_name, object=arch_name).execute()
                    cloudstorage.buckets().delete(bucket=tmp_bucket_name).execute()
            finally:
                os.unlink(arch_path)

        finally:
            shutil.rmtree(rebundle_dir)

        try:
            goog_image_name = self._role_name.lower().replace('_', '-') + '-' + str(int(time.time()))
            LOG.info('Registering new image %s' % goog_image_name)
            # TODO: check duplicate names
            compute = pl.new_compute_client()

            # Getting this instance's kernel
            instance_id = pl.get_instance_id()
            zone = os.path.basename(pl.get_zone())
            all_instances = compute.instances().list(project=proj_id, zone=zone, fields="items(kernel,id)").execute()['items']
            try:
                kernel = filter(lambda inst: inst['id'] == instance_id, all_instances)[0]['kernel']
            except KeyError:
                # Looks like this instance was started from image, getting kernel from image
                try:
                    current_image = pl.get_image()

                    current_image_fq = current_image.split('/')
                    current_img_project = current_image_fq[1]
                    current_img_name = current_image_fq[3]
                    current_img_obj = compute.images().get(project=current_img_project,
                                                                    image=current_img_name).execute()
                    kernel = current_img_obj['preferredKernel']
                except:
                    raise HandlerError('Could not obtain kernel for this instance')
                
            image_url = 'http://storage.googleapis.com/%s/%s' % (tmp_bucket_name, arch_name)

            req_body = dict(
                    name=goog_image_name,
                    sourceType='RAW',
                    preferredKernel=kernel,
                    rawDisk=dict(
                            containerType='TAR',
                            source=image_url
                    )
            )

            req = compute.images().insert(project=proj_id, body=req_body)
            operation = req.execute()['name']

            LOG.info('Waiting for image to register')
            def image_is_ready():
                req = compute.globalOperations().get(project=proj_id, operation=operation)
                res = req.execute()
                if res['status'] == 'DONE':
                    if res.get('error'):
                        errors = []
                        for e in res['error']['errors']:
                            err_text = '%s: %s' % (e['code'], e['message'])
                            errors.append(err_text)
                        raise Exception('\n'.join(errors))
                    return True
                return False
            wait_until(image_is_ready, logger=LOG, timeout=600)

        finally:
            try:
                objs = cloudstorage.objects()
                objs.delete(bucket=tmp_bucket_name, object=arch_name).execute()
                cloudstorage.buckets().delete(bucket=tmp_bucket_name).execute()
            except:
                e = sys.exc_info()[1]
                LOG.error('Faled to remove image compressed source: %s' % e)

        return '%s/images/%s' % (proj_name, goog_image_name)


    def _create_spec_devices(self, root):
        nodes = (
                'console c 5 1',
                'null c 1 3',
                'zero c 1 5',
                'tty c 5 0',
        )

        for node in nodes:
            args = node.split()
            args[0] = os.path.join(root, 'dev', args[0])
            system(' '.join(['mknod'] + args), shell=True)
