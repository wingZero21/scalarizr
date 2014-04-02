
import os
import glob
import time
import random
import logging
import shutil
import tempfile

from scalarizr.api.image import ImageAPIDelegate
from scalarizr.api.image import ImageAPIError
from scalarizr import util
from scalarizr.util import software
from scalarizr.linux import pkgmgr 
from scalarizr.linux import os as os_dist
from scalarizr.node import __node__
from scalarizr.storage2.cloudfs import FileTransfer


_logger = logging.getLogger(__name__)


ROLEBUILDER_USER = 'scalr-rolesbuilder'


class GCEImageAPIDelegate(ImageAPIDelegate):
    exclude_dirs = (
        '/tmp',
        '/proc',
        '/dev',
        '/mnt',
        '/var/lib/google/per-instance',
        '/sys',
        '/cdrom',
        '/media',
        '/run',
        '/selinux')
    exclude_files = ('/etc/ssh/.host_key_regenerated',
        '/lib/udev/rules.d/75-persistent-net-generator.rules')

    gcimagebundle_pkg_name = 'python-gcimagebundle' if os_dist.debian_family else 'gcimagebundle'

    def _remove_bucket(self, bucket_name, object, cloudstorage):
        with util.capture_exception(_logger):
            objs = cloudstorage.objects()
            objs.delete(bucket=bucket_name, object=object).execute()
        cloudstorage.buckets().delete(bucket=bucket_name).execute()

    def _register_image(self, image_name, bucket_name, archive_name, cloudstorage):
        pl = __node__['platform']
        proj_id = pl.get_numeric_project_id()
        try:
            _logger.info('Registering new image %s' % image_name)
            compute = pl.new_compute_client()

            image_url = 'http://storage.googleapis.com/%s/%s' % (bucket_name, archive_name)

            req_body = {'name': image_name,
                'sourceType': 'RAW',
                'rawDisk': {'source': image_url}}

            req = compute.images().insert(project=proj_id, body=req_body)
            operation = req.execute()['name']

            _logger.info('Waiting for image to register')
            def image_is_ready():
                req = compute.globalOperations().get(project=proj_id, operation=operation)
                res = req.execute()
                if res['status'] == 'DONE':
                    if res.get('error'):
                        errors = []
                        for e in res['error']['errors']:
                            err_text = '%s: %s' % (e['code'], e['message'])
                            errors.append(err_text)
                        raise ImageAPIError('\n'.join(errors))
                    return True
                return False
            util.wait_until(image_is_ready, logger=_logger, timeout=600)

        finally:
            try:
                self._remove_bucket(bucket_name, archive_name, cloudstorage)
            except (Exception, BaseException), e:
                _logger.error('Faled to remove image compressed source: %s' % e)

    def _prepare_software(self):
        try:
            pkgmgr.latest(self.gcimagebundle_pkg_name)
        except (Exception, BaseException), e:
            _logger.warn('Gcimagebundle update failed: %s' % e)

        if os_dist.redhat_family:
            semanage = software.which('semanage')
            if not semanage:
                pkgmgr.installed('policycoreutils-python')
                semanage = software.which('semanage')

            util.system2((semanage, 'permissive', '-a', 'rsync_t'))

    def snapshot(self, op, role_name):
        rebundle_dir = tempfile.mkdtemp()
        archive_path = ''
        try:
            pl = __node__['platform']
            proj_id = pl.get_numeric_project_id()
            proj_name = pl.get_project_id()
            cloudstorage = pl.new_storage_client()

            root_part_path = os.path.realpath('/dev/root')
            root_part_sysblock_path = glob.glob('/sys/block/*/%s' % os.path.basename(root_part_path))[0]
            root_device = '/dev/%s' % os.path.basename(os.path.dirname(root_part_sysblock_path))

            archive_name = '%s.tar.gz' % role_name.lower()
            archive_path = os.path.join(rebundle_dir, archive_name)

            self._prepare_software()

            gcimagebundle_bin = software.which('gcimagebundle')

            out, err, code = util.system2((gcimagebundle_bin,
                '-d', root_device,
                '-e', ','.join(self.exclude_dirs),
                '-o', rebundle_dir,
                '--output_file_name', archive_name), raise_exc=False)
            if code:
                raise ImageAPIError('Gcimagebundle util returned non-zero code %s. Stderr: %s' % (code, err))

            _logger.info('Uploading compressed image to cloud storage')
            tmp_bucket_name = 'scalr-images-%s-%s' % (random.randint(1, 1000000), int(time.time()))
            remote_path = 'gcs://%s/%s' % (tmp_bucket_name, archive_name)
            arch_size = os.stat(archive_path).st_size
            uploader = FileTransfer(src=archive_path, dst=remote_path)

            try:
                upload_result = uploader.run()
                if upload_result['failed']:
                    errors = [str(failed['exc_info'][1]) for failed in upload_result['failed']]
                    raise ImageAPIError('Image upload failed. Errors:\n%s' % '\n'.join(errors))
                assert arch_size == upload_result['completed'][0]['size']
            except:
                self._remove_bucket(tmp_bucket_name, archive_name, cloudstorage)
                raise

        finally:
            shutil.rmtree(rebundle_dir)
            if os.path.exists(archive_path):
                os.remove(archive_path)

        image_name = role_name.lower().replace('_', '-') + '-' + str(int(time.time()))
        self._register_image(image_name, tmp_bucket_name, archive_name, cloudstorage)

        return '%s/images/%s' % (proj_name, image_name)

    def prepare(self, operation, role_name):
        pass

    def finalize(self, operation, role_name):
        pass
