__author__ = 'Dmytro Korsakov'

import os
import time
import subprocess
import cloudfiles
import logging


def timeit(method):

	def timed(*args, **kw):
		ts = time.time()
		result = method(*args, **kw)
		te = time.time()

		print '%r (%r, %r) %2.2f sec' % \
		      (method.__name__, args, kw, te-ts)
		return result

	return timed


class CloudTest:

	PREFIX  = 'snapshot'

	def __init__(self, login, key):
		self.login = login
		self.key = key
		self.logger = logging.getLogger(__name__)

	def get_mount_point(self, loop_device):
		pass

	@timeit
	def make_shadow_copy(self, source_volume, dest_dir, chunk_size = '30M'):
		#TODO: change dest_dir to loop device name and mount it into dest_dir if needed
		#TODO: write checksum
		cmd1 = ['dd', 'if=%s' % source_volume]
		cmd2 = ['gzip']
		cmd3 = ['split', '-a','3', '-b', '%s'%chunk_size, '-', '%s/%s.gz.' % (dest_dir, self.PREFIX)]
		p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
		p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE)
		p3 = subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE)
		self.logger.info('Making shadow copy')
		output = p3.communicate()[0]
		print output

	@timeit
	def upload_files(self,location, remote_location, container_name='test_container', cleanup=True):
		#TODO: think about rotation
		conn = cloudfiles.get_connection(username=self.login, api_key=self.key, serviceNet=True)

		files = []
		for file in os.listdir(location):
			if file.startswith(self.PREFIX):
				files.append(file)

		try:
			container = conn.create_container(container_name)
		except BaseException, Exception:
			self.logger.error('Cannot create container')

		for file in files:
			self.logger.info('Uploading %s to container %s' % (file, container_name))
			full_path = os.path.join(location,file)
			basename = os.path.basename(file)
			o = container.create_object(remote_location+'/'+basename)
			o.load_from_filename(full_path)

			if cleanup:
				self.logger.info('Deleting file')
				os.remove(full_path)

		self.logger.info('done.')

	@timeit
	def download_files(self, location, remote_location, container_name='test_container', cleanup=False):
		conn = cloudfiles.get_connection(username=self.login, api_key=self.key, serviceNet=True)

		containers = conn.get_all_containers()
		if not container_name in [container.name for container in containers]:
			self.logger.error('Container %s not found' % container_name)
			return

		container = conn.get_container(container_name)
		objects = container.get_objects(path=remote_location)
		for obj in objects:
			self.logger.info('etrieving object %s' % obj.name)
			target_file = os.path.join(location, obj.name)
			obj.save_to_filename(target_file)

	def extract_from_copy(self, source_dir, dest_dir, cleanup=False):
		files = os.listdir(source_dir)
		files.sort()
		cat = ['cat']
		cat.extend(files)
		#cat 1.gz.aaa 1.gz.aab 1.gz.aac | gunzip > 1
		gunzip = ['gunzip']
		dest_path = os.path.join(dest_dir, 'extracted.img')
		dest_pointer = open(dest_path, 'w')
		#Todo: find out where to extract file end how loop device will react
		p1 = subprocess.Popen(cat, stdout=subprocess.PIPE)
		p2 = subprocess.Popen(gunzip, stdin=p1.stdout, stdout=dest_pointer)
		err = p2.communicate()[1]
		dest_pointer.close()
		if err:
			self.logger.error(err)


if __name__ == '__main__':

	login = 'rackcloud05'
	key = '27630d6e96e72fa43233a185a0518f0e'

	location = '/mnt/dest/'
	remote_location = 'backup01'

	CT = CloudTest(login, key)
	#CT.make_shadow_copy('/dev/loop0', '/mnt/dest')
	#CT.upload_files(location, remote_location)
