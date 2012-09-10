__author__ = 'Nick Demyanchuk'

import re
import os
import stat
import uuid
import time
import logging
import shutil
import tempfile

from scalarizr.bus import bus
from scalarizr.storage import transfer
from scalarizr.util import filetool, system2, fstool, wait_until
from scalarizr.handlers import rebundle as rebundle_hndlr


def get_handlers():
	return [GceRebundleHandler()]


LOG = logging.getLogger(__name__)


class GceRebundleHandler(rebundle_hndlr.RebundleHandler):
	exclude_dirs = ('/tmp', '/var/run', '/var/lib/google/per-instance')
	exclude_files = ('/etc/ssh/.host_key_regenerated', )

	def rebundle(self):
		rebundle_dir = tempfile.mkdtemp()

		try:
			pl = bus.platform
			proj_id = pl.get_numeric_project_id()
			cloudstorage = pl.new_storage_client()

			tmp_mount_dir = os.path.join(rebundle_dir, 'root')
			os.makedirs(tmp_mount_dir)

			image_name	= 'disk.raw'
			image_path	= os.path.join(rebundle_dir, image_name)

			root = filter(lambda x: x.mpoint == '/', filetool.df())[0]

			""" Creating disk file """
			LOG.debug('Creating image file %s' % image_path)
			with open(image_path, 'w') as f:
				f.truncate(root.size*1024 + 1*1024)

			try:

				LOG.debug('Creating partition table on image')
				system2(('parted', image_path, 'mklabel', 'msdos'))
				system2(('parted', image_path, 'mkpart', 'primary', 'ext2', 1, str(root.size/1024)))

				# Map disk image
				out = system2(('kpartx', '-av', image_path))[0]
				try:
					loop = re.search('(/dev/loop\d+)', out).group(1)
					root_dev_name = '/dev/mapper/%sp1' % loop.split('/')[-1]

					LOG.debug('### Root dev name %s' % root_dev_name)

					LOG.debug('Creating filesystem')
					fstool.mkfs(root_dev_name, 'ext4')
					dev_uuid = uuid.uuid4()
					system2(('tune2fs', '-U', str(dev_uuid), root_dev_name))

					""" Mounting """
					fstool.mount(root_dev_name, tmp_mount_dir)
					try:
						""" Rsync """
						# Get mounts
						lines = system2(('/bin/mount', '-l'))[0].splitlines()
						exclude_dirs = []
						for line in lines:
							mpoint = line.split()[2]
							if mpoint != '/':
								exclude_dirs.append(mpoint)

						exclude_dirs.extend(self.exclude_dirs)

						rsync = filetool.Rsync()
						rsync.source('/').dest(tmp_mount_dir).sparse()
						rsync.hardlinks().archive().times()
						rsync.exclude([os.path.join(ex, '**') for ex in exclude_dirs])
						rsync.exclude(self.exclude_files)
						rsync.exclude(self._excludes)
						LOG.info('Copying root filesystem to image')
						rsync.execute()

						LOG.info('Cleanup image')
						self._create_spec_devices(tmp_mount_dir)

						""" Cleanup network """
						f_to_del_path = os.path.join(tmp_mount_dir, 'lib/udev/rules.d/75-persistent-net-generator.rules')
						if os.path.exists(f_to_del_path):
							os.remove(f_to_del_path)

						""" Patch fstab"""
						fstab_path = os.path.join(tmp_mount_dir, 'etc/fstab')
						if os.path.exists(fstab_path):
							with open(fstab_path) as f:
								fstab = f.read()

							new_fstab = re.sub('UUID=\S+\s+/\s+(.*)', 'UUID=%s / \\1' % dev_uuid, fstab)

							with open(fstab_path, 'w') as f:
								f.write(new_fstab)

					finally:
						fstool.umount(device=root_dev_name)
				finally:
					system2(('kpartx', '-d', image_path))

				""" Tar.gzipping """
				LOG.info('Compressing image.')
				arch_name = '%s.tar.gz' % self._role_name.lower()
				arch_path = os.path.join(rebundle_dir, arch_name)

				tar = filetool.Tar()
				tar.create().gzip().sparse()
				tar.archive(arch_path)
				tar.add(image_name, rebundle_dir)
				system2(str(tar), shell=True)

			finally:
				os.unlink(image_path)

			try:
				""" Hash """
				# add sha1Checksum to register request

				""" Uploading """
				uploader = transfer.Transfer(logger=LOG)
				# Make bucket name more random
				tmp_bucket_name = 'scalr-images-tmp-bucket-%s' % int(time.time())

				try:
					remote_path = 'gs://%s/' % tmp_bucket_name
					uploader.upload((arch_path,), remote_path)
				except:
					try:
						objs = cloudstorage.objects()
						objs.delete(bucket=tmp_bucket_name, object=arch_name).execute()
					except:
						pass

					cloudstorage.buckets().delete(bucket=tmp_bucket_name).execute()
					raise

			finally:
				os.unlink(arch_path)

		finally:
			shutil.rmtree(rebundle_dir)

		try:
			""" Register new image """
			LOG.info('Registering new image %s' % self._role_name.lower())
			compute = pl.new_compute_client()

			image_url = 'http://storage.googleapis.com/%s/%s' % (tmp_bucket_name, arch_name)
			req_body = dict(
				name=self._role_name.lower(),
				sourceType='RAW',
				rawDisk=dict(
					containerType='TAR',
					source=image_url
				)
			)

			req = compute.images().insert(project=proj_id, body=req_body)
			operation = req.execute()['name']

			LOG.info('Waiting for image to register')
			def image_is_ready():
				req = compute.operations().get(project=proj_id, operation=operation)
				res = req.execute()
				if res['status'] == 'DONE':
					return True
				return False

			wait_until(image_is_ready, logger=LOG, timeout=600)
		finally:
			objs = cloudstorage.objects()
			objs.delete(bucket=tmp_bucket_name, object=arch_name).execute()
			cloudstorage.buckets().delete(bucket=tmp_bucket_name).execute()

		return 'projects/%s/images/%s' % (proj_id, self._role_name.lower())


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
			system2(['mknod'] + args)








