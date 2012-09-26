import re
import os
import sys
import Queue
import urlparse
import itertools
import tempfile
import inspect
import subprocess
import threading
import logging
import ConfigParser

from scalarizr import storage2
from scalarizr.libs import bases
from scalarizr.linux import coreutils

filesystem_types = {}
LOG = logging.getLogger(__name__)


class BaseTransfer(bases.Task):
	
	def __init__(self, src=None, dst=None, **kwds):
		'''
		:type src: string / generator / iterator
		:param src: Transfer source

		:type dst: string / generator / iterator
		:param dst: Transfer destination
		'''
		if callable(src):
			src = (item for item in src())
		else:
			if not hasattr(src, '__iter__'):
				src = [src]
			src = iter(src)
		if callable(dst):
			dst = (item for item in dst())
		elif not hasattr(dst, '__iter__'):
			dst = itertools.repeat(dst)
		else:
			dst = iter(dst)	
	
		super(BaseTransfer, self).__init__(src=src, dst=dst, **kwds)
		self.define_events('transfer_start', 'transfer_error', 'transfer_complete')
		

class Transfer(BaseTransfer):

	_url_re = re.compile(r'^[\w-]+://')

	def __init__(self, num_workers=4, retries=3, multipart=False, **kwds):
		'''
		:type num_workers: int
		:param num_workers: Number of worker threads

		:type retries: int
		:param retries: Max retries to transfer one file

		:type multipart: bool
		:param multipart: Use multipart uploading functionality of the underlying
			driver. :param:`src` should be a chunks iterator / generator

		:event transfer_start: Fired when started transfer of the particular file

		:evtype src: string
		:evparam src: Full source path

		:evtype dst: string
		:evparam dst: Full destination path

		:event transfer_complete: Fired when file transfer complete

		:event transfer_error: Fired when file transfer failed	

		@param: src transfer source path
			- str file or directory path. directory processed recursively
			- list of path strings 
			- generator function that will produce path strings 

		@param: dst transfer destination path
			- str file or directory path
			- list of path strings			
			- generator function that will produce path strings


		# XXX(marat): We should extend class from Observable
		@param: listener function or object to call when file transfer is 
		started, finished, failed, restarted.
			on_start: fn(src, dst, state='start')
			on_complete: fn(src, dst, state='complete', retry=1, transfered_bytes=1892331)
			on_error: fn(src, dst, state='error', retry=1, exc_info=(3 items tuple))
			on_restart: fn(src, dst, state='restart', retry=2) (????)

		Examples:

			Upload pathes
				Transfer('/mnt/backups/daily.tar.gz', 's3://backups/mysql/2012-09-05/', 'upload')

			Upload generator
				def files():
					yield 'part.1'
					yield 'part.2'
				Transfer(files, 's3://images/ubuntu12.04.1/', 'upload')

			Download both generators
				def src():
					yield 's3://backups/mysql/daily.tar.gz'
					yield 'rackspace-cloudfiles://backups/mysql/daily.tar.gz'

				def dst():
					yield '/backups/daily-from-s3.tar.gz'
					yield '/backups/daily-from-cloudfiles.tar.gz'
		'''
		super(Transfer, self).__init__(num_workers=num_workers, 
						num_retries=num_retries, multipart=multipart, **kwds)

		self._completed = {}
		self._failed = {}
		self._retries_queue = Queue.Queue()
		self._stop_all = threading.Event()
		self._stopped = threading.Event()
		self._gen_lock = threading.RLock()
		self._worker_lock = threading.Lock()
		self._upload_id = None

		
	def _job_generator(self):
		chunk_num = -1
		no_more = False
		while True:
			try:
				yield self._retries_queue.get_nowait()
			except Queue.Empty:
				if no_more:
					raise StopIteration
			try:
				with self._gen_lock:
						src = self.src.next()
						dst = self.dst.next()
						retry = 1
						chunk_num += 1
						yield [src, dst, retry, chunk_num]
			except StopIteration:
				no_more = True
			
			
	def _is_remote_path(self, path):
		return isinstance(path, basestring) and self._url_re.match(path)
				
				
	def _worker(self):
		driver = None
		for src, dst, retry, chunk_num in self._job_generator():
			self.fire('transfer_start', src, dst, retry, chunk_num)
			try:
				uploading = self._is_remote_path(dst)
				assert not (uploading and self._is_remote_path(src))
				assert (uploading and os.path.isfile(src)) or not uploading

				if not driver:
					rem, loc = dst, src if uploading else src, dst
					driver = cloudfs(urlparse.urlparse(rem).schema)
					with self._worker_lock:
						if self.multipart and not self._upload_id:
							chunk_size = os.path.getsize(loc)
							self._upload_id = driver.multipart_init(rem, 
															chunk_size)
					
				if uploading:
					if self.multipart:
						driver.multipart_put(self._upload_id, chunk_num, src)
					else:
						driver.put(src, dst)
					self._completed.append({
							'src': src,
							'dst': dst,
							'chunk_num': chunk_num,
							'size': os.path.getsize(src)})
				else:
					driver.get(src, dst)
					self._completed.append({
							'src': src,
							'dst': dst,
							'size': os.path.getsize(dst)})
				self.fire('transfer_complete', src, dst, retry, chunk_num)

			except AssertionError:
				self.fire('transfer_error', src, dst, retry, chunk_num, 
							sys.exc_info())
			except:
				retry += 1
				if retry <= self.retries:
					self._retries_queue.put((src, dst, retry, chunk_num))
				else:
					self._failed.append({
							'src': src,
							'dst': dst,
							'exc_info': sys.exc_info()})
				self.fire('transfer_error', src, dst, retry, chunk_num, 
							sys.exc_info())
			finally:
				if self._stop_all.isSet():
					with self._worker_lock:
						if self.multipart and self._upload_id:
							driver.multipart_abort(self._upload_id)
							self._upload_id = None
					break

		with self._worker_lock:
			if self.multipart and self._upload_id:
				driver.multipart_complete(self._upload_id)
				self._upload_id = None


	def _run(self):
		self._stop_all.clear()
		self._stopped.clear()
		try:
			# Starting threads
			pool = []
			for n in range(self.num_workers):
				worker = threading.Thread(
							name='transfer-worker-%s' % n, 
							target=self._worker)
				LOG.debug("Starting worker '%s'", worker.getName())
				worker.start()
				pool.append(worker)
			# Join workers
			for worker in pool:
				worker.join()
				LOG.debug("Worker '%s' finished", worker.getName())		
			return {
				'completed': self._completed,
				'failed': self._failed
			}
		finally:
			self._stopped.set()


	def kill(self, timeout=None):
		self._stop_all.set()
		self._stopped.wait(timeout)



class LargeTransfer(bases.Task):
	UPLOAD = 'upload'
	DOWNLOAD = 'download'
	'''
	SQL dump. File-per-database.
	---------------------------

	def src_gen():
		yield stream = mysqldump ${database}
	def dst_gen():
		yield ${database}

	s3://.../${transfer_id}/manifest.ini
							${database_1}.gz.00
							${database_1}.gz.01
							${database_2}.gz.00
							${database_2}.gz.01
							${database_2}.gz.02

	$ manifest.ini
	[snapshot]
	description = description here
	created_at = datetime
	pack_method = "pigz"

	[chunks]
	${database_1}.gz.part00 = md5sum
	${database_1}.gz.part01 = md5sum
	${database_2}.gz.part00 = md5sum


	Directory backup
	----------------

	src = '/mnt/dbbackup'
	dst = 's3://backup/key1/key2/'

	s3://backup/key1/key2/${transfer_id}/manifest.ini
	s3://backup/key1/key2/${transfer_id}/part.gz.00
	s3://backup/key1/key2/${transfer_id}/part.gz.01
	s3://backup/key1/key2/${transfer_id}/part.gz.02


	Directory restore
	-----------------

	src = s3://backup/key1/key2/eph-snap-12345678/manifest.ini
	dst = /mnt/dbbackup/

	1. Download manifest 
	2. <chunk downloader> | gunzip | tar -x -C /mnt/dbbackup
	'''
	def __init__(self, src, dst, direction,
				transfer_id=None,
				tar_it=True,
				gzip_it=True, 
				chunk_size=100, 
				try_pigz=True,
				manifest='manifest.ini',
				**kwds):
		'''
		@param src: transfer source path
			- str file or directory path. 
			- file-like object (stream)
			- generator function
		'''
		url_re = re.compile(r'^[\w-]+://')
		if isinstance(src, basestring) and url_re.match(src):
			self._up = False
		elif isinstance(dst, basestring) and url_re.match(dst):
			self._up = True
		else:
			raise ValueError('Eather src or dst should be URL-like string')
		if self._up and os.path.isdir(src) and not tar_it:
			raise ValueError('Passed src is a directory. tar_it=True expected')
		if self._up:
			if callable(src):
				src = (item for item in src())
			else:
				if not hasattr(src, '__iter__'):
					src = [src]
				src = iter(src)
			if callable(dst):
				dst = (item for item in dst())
			elif not hasattr(dst, '__iter__'):
				dst = itertools.repeat(dst)
			else:
				dst = iter(dst)

		self.direction = direction
		self.src = src
		self.dst = dst
		self.tar_it = tar_it
		self.gzip_it = gzip_it
		self.chunk_size = chunk_size
		self.try_pigz = try_pigz
		self.transfer_id = transfer_id
		self.manifest = manifest
		self._transfer = Transfer(self._src_generator, 
								self._dst_generator, **kwds)
		self._tranzit_vol = storage2.volume(
								type='tmpfs',
								mpoint=tempfile.mkdtemp())
		self._chunk_num = -1

		events = self._transfer.listeners.keys()
		self.define_events(*events)
		for ev in events:
			self._transfer.on(ev=self._proxy_event(ev))
		


	def _src_generator(self):
		'''
		Compress, split, yield out
		'''
		self._tranzit_vol.size = int(self.chunk_size * (self._transfer.num_workers) * 1.1)
		self._tranzit_vol.ensure(mkfs=True)
		try:
			if self.direction == self.UPLOAD:
				# Tranzit volume size is chunk for each worker 
				# and Ext filesystem overhead
				for src in self.src:
					prefix = self._tranzit_vol.mpoint
					stream = None
					cmd = tar = gzip = None

					if hasattr(src, 'read'):
						stream = src
						if hasattr(stream, 'name'):
							name = stream.name
						else:
							name = 'stream-%s' % hash(stream)
						prefix = os.path.join(prefix, name) + '.'

					elif os.path.isdir(src):
						prefix = os.path.join(prefix, 'part.')
						tar = cmd = subprocess.Popen(
										['/bin/tar', 'cp', '-C', src, '.'], 
										stdout=subprocess.PIPE, 
										stderr=subprocess.PIPE, 
										close_fds=True)
						stream = tar.stdout
					elif os.path.isfile(src):
						prefix = os.path.join(prefix, 
										os.path.basename(src)) + '.'
						stream = open(src)
					else:
						raise ValueError('Unsupported src: %s' % src)

					if self.gzip_it:
							prefix += 'gz.'
							gzip = cmd = subprocess.Popen(
										[self._gzip_bin(), '-5'], 
										stdin=stream, 
										stdout=subprocess.PIPE, 
										stderr=subprocess.PIPE, 
										close_fds=True)
							if tar:
								# Allow tar to receive SIGPIPE if gzip exits.
								tar.stdout.close()
							stream = gzip.stdout
					
					#dst = self.dst.next() 
					for filename in self._split(stream, prefix):
						yield filename
					if cmd:
						cmd.communicate() 
			else:
				src = self.src.next()
				src_pr = urlparse.urlparse(src)
				drv = cloudfs(src_pr.scheme)
				filename = drv.get(src, self._tranzit_vol.mpoint)
				manifest = Manifest(filename)
				os.remove(filename)
				
				if manifest.type == 'files':
					# Files 
					for name in manifest:
						for chunk in manifest.chunks(name):
							pass
				else:
					# Directory transfer
					name = iter(manifest).next()
					for chunk in manifest.chunks(name):
						# Restore
						pass
		finally:
			self._tranzit_vol.destroy()
			coreutils.remove(self._tranzit_vol.mpoint)


	def _dst_generator(self):
		while True:
			# What here?
			yield None

	def _split(self, stream, prefix):
		buf_size = 4096
		chunk_size = self.chunk_size * 1024 * 1024
		read_bytes = None		
		fp = None

		def next_chunk():
			self._chunk_num += 1
			read_bytes = 0
			fp = open(self.prefix + '%03d' % self._chunk_num)
		next_chunk()

		while True:
			size = min(buf_size, chunk_size - read_bytes)
			bytes = stream.read(size)
			if not bytes:
				if fp:
					fp.close()
				break
			read_bytes += len(bytes)
			fp.write(bytes)
			if read_bytes == chunk_size:
				fp.close()
				yield fp.name
				next_chunk()


	def _gzip_bin(self):
		if self.try_pigz and os.path.exists('/usr/bin/pigz'):
			return '/usr/bin/pigz'
		return '/bin/gzip'		


	def _proxy_event(self,event):
		def proxy(*args, **kwds):
			self.fire(event, *args, **kwds)
		return proxy


	def _run(self):
		self._transfer.run()


"""
class Manifest(object):
	def __init__(self, filename):
		self.filename = filename
		self.ini = ConfigParser.ConfigParser()
		self.ini.read(self.filename)


	def __getattr__(self, name):
		try:
			if self.__dict__['ini'].get('snapshot', name)
		except ConfigParser.NoOptionError:
			# Compatibility with old 'eph' storage manifests
			if name == 'type' and 'eph-snap' in self.filename: 
				return 'dir'
			raise AttributeError(name)


	def __setattr__(self, name, value):
		if name in dir(self):
			self.__dict__[name] = value
		else:
			self.__dict__['ini'].set('snapshot', str(value))


	def __iter__(self):
		'''
		Iterates over file names
		'''
		raise NotImplementedError()


	def chunks(self, name):
		raise NotImplementedError()
"""

def cloudfs(fstype, **driver_kwds):
	raise NotImplementedError()


class CloudFileSystem(object):

	features = {
		'multipart': False
	}

	def ls(self, path):
		raise NotImplementedError()

	def stat(self, path):
		'''
		size in bytes
		type = dir | file | container
		'''
		raise NotImplementedError()

	def put(self, src, path):
		raise NotImplementedError()

	def multipart_init(self, path, part_size):
		'''
		Returns upload_id
		'''
		raise NotImplementedError()

	def multipart_put(self, upload_id, src):
		raise NotImplementedError()

	def multipart_complete(self, upload_id):
		raise NotImplementedError()

	def multipart_abort(self, upload_id):
		raise NotImplementedError()

	def get(self, path, dst):
		raise NotImplementedError()

	def delete(self, path):
		raise NotImplementedError()


'''
bak = backup.backup(
		type='mysqldump', 
		file_per_database=True, 
		cloudfs_dir='glacier://Vault_1/')
rst = buk.run()
print rst
>>> {
	type: mysqldump
	files: [{
		size: 14503104
		path: glacier://Vault_1/?avail_zone=us-east-1&archive_id=NkbByEejwEggmBz2fTHgJrg0XBoDfjP4q6iu87-TjhqG6eGoOY9Z8i1_AUyUsuhPAdTqLHy8pTl5nfCFJmDl2yEZONi5L26Omw12vcs01MNGntHEQL8MBfGlqrEXAMPLEArchiveId
	}]

Inside LargeTransfer:

pack into single tar | gzip | split | Transfer(generator, 'glacier://Vault_1/', multipart=True)



'''
