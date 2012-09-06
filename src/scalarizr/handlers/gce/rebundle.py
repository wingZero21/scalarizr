__author__ = 'Nick Demyanchuk'

import re
import os
import sys
import uuid
import time
import logging
import tempfile

from scalarizr.bus import bus
from scalarizr.storage import transfer
from scalarizr.util import filetool, system2, fstool, wait_until
from scalarizr.handlers import rebundle as rebundle_hndlr


def get_handlers():
	return [GceRebundleHandler()]


LOG = logging.getLogger(__name__)


class GceRebundleHandler(rebundle_hndlr.RebundleHandler):
	exclude_dirs = ('/tmp', '/var/log', '/var/run', '/var/lib/google/per-instance')
	exclude_files = ('/etc/ssh/.host_key_regenerated', )

	def rebundle(self):
		# TODO: ADD LOGGING
		rebundle_dir = tempfile.mkdtemp()
		tmp_mount_dir = os.path.join(rebundle_dir, 'root')
		os.makedirs(tmp_mount_dir)

		image_name	= 'disk.raw'
		image_path	= os.path.join(rebundle_dir, image_name)


		root = filter(lambda x: x.mpoint == '/', filetool.df())[0]

		""" Creating disk file """
		with open(image_path, 'w') as f:
			f.truncate(root.size*1024*1024)

		""" Partitioning """
		# Create partition table
		system2(('parted', image_path, 'mklabel', 'msdos'))

		# Making partition
		system2(('parted', image_path, 'mkpart', 'primary', 'ext2', 1, str(root.size/1024)))

		# Map disk image
		out = system2(('kpartx', '-av', image_path))[0]
		try:
			loop = re.search('(/dev/loop\d+)', out).group(1)
			root_dev_name = '/dev/mapper/%sp1' % loop.split('/')[-1]

			LOG.debug('### Root dev name %s' % root_dev_name)

			""" Creating fs """
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
				rsync.source('/').dest(tmp_mount_dir).sparse().archive().times()
				rsync.exclude([os.path.join(ex, '**') for ex in exclude_dirs])
				rsync.exclude(self.exclude_files)
				rsync.exclude(self._excludes)
				rsync.execute()

				# TODO: Create special files

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
		arch_name = '%s.tar.gz' % self._role_name.lower()
		arch_path = os.path.join(rebundle_dir, arch_name)
		system2(('tar', 'czSf', arch_path, '-C', rebundle_dir, image_name))
		os.unlink(image_path)

		""" Hash """
		# add sha1Checksum to register request

		""" Uploading """
		uploader = transfer.Transfer(logger=LOG)
		tmp_bucket_name = 'scalr-images-tmp-bucket-%s' % int(time.time())

		pl = bus.platform
		proj_id = pl.get_numeric_project_id()
		cloudstorage = pl.new_storage_client()
		cloudstorage.buckets().insert(id=tmp_bucket_name, projectId=proj_id)

		remote_path = 'gs://%s/%s' % (tmp_bucket_name, arch_name)
		uploader.upload(arch_path, remote_path)

		""" Register new image """
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
		# TODO: delete image and bucket
		return 'projects/%s/images/%s' % (proj_id, self._role_name.lower())












