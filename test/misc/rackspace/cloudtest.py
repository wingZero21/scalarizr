__author__ = 'Dmytro Korsakov'

import os
import time
import subprocess
import cloudfiles
import logging


def time_it(method):

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

	@time_it
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

	@time_it
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
			base_name = os.path.basename(file)
			o = container.create_object(remote_location+'/'+base_name)
			o.load_from_filename(full_path)

			if cleanup:
				self.logger.info('Deleting file')
				os.remove(full_path)

		self.logger.info('done.')

	@time_it
	def download_files(self, location, remote_location, container_name='test_container', cleanup=False):
		#todo: cleanup source dir, find out if there is still enough free disk space
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

	@time_it
	def extract_from_copy(self, source_dir, dest_file, cleanup=False):
		#cat 1.gz.aaa 1.gz.aab 1.gz.aac | gunzip > 1
		files = [os.path.join(source_dir, file) for file in os.listdir(source_dir)]
		files.sort()
		cat = ['cat']
		cat.extend(files)
		gunzip = ['gunzip']
		dest = open(dest_file, 'w')
		#Todo: find out where to extract file
		p1 = subprocess.Popen(cat, stdout=subprocess.PIPE)
		p2 = subprocess.Popen(gunzip, stdin=p1.stdout, stdout=dest)
		err = p2.communicate()[1]
		dest.close()
		if err:
			self.logger.error(err)
		else:
			self.logger.info('Archive was successfully extracted to %s' % source_dir)


if __name__ == '__main__':

	login = 'rackcloud05'
	key = '27630d6e96e72fa43233a185a0518f0e'

	location = '/mnt/dest/'
	remote_location = 'backup01'

	CT = CloudTest(login, key)
	#CT.make_shadow_copy('/dev/loop0', '/mnt/dest')
	#CT.upload_files(location, remote_location)
	#CT.download_files(location, remote_location)
	CT.extract_from_copy('/mnt/dest', '/media/media/extracted.img')
