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

from scalarizr import storage2
from scalarizr.libs import pubsub
from scalarizr.linux import coreutils

filesystem_types = {}
LOG = logging.getLogger(__name__)


class BaseTransfer(pubsub.Observable):
	
	src = None
	dst = None
	num_workers = None
	max_retry = None
	listeners = None
	
	def __init__(self, src, dst, num_workers=4, max_retry=4, listeners=None):
		
		super(BaseTransfer, self).__init__()
		
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
	
		self.src = src
		self.dst = dst
		self.num_workers = num_workers
		self.max_retry = max_retry
		self.listeners = listeners
		
		self.define_events('transfer_started', 'transfer_complete', 'transfer_error', 'transfer_stopped')		
		

class Transfer(BaseTransfer):
		
	running = None
	result = None
	failed_files = None
			
	_retries_queue = None
	_stop_all = None
	_lock = None
	

	def __init__(self, src, dst, num_workers=4, max_retry=4, listeners=None):

		'''
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
		super(Transfer, self).__init__(src, dst, num_workers, max_retry, listeners)
		
		self.result = self.failed_files = {}
		self._retries_queue = Queue.Queue()
		self._stop_all = threading.Event()
		self._lock = threading.RLock()

		
	def get_job(self):
		while True:
			try:
				with self._lock:
					s = self.src.next()
					d = self.dst.next()
					r = 1
					yield [s, d, r]
			except StopIteration:
				pass	
			try:
				yield self._retries_queue.get_nowait()
			except Queue.Empty:
				raise StopIteration()
			
			
	def is_remote_path(self, path):
		url_re = re.compile(r'^[\w-]+://')
		return isinstance(path, basestring) and url_re.match(path)
				
				
	def _worker(self, result, failed):
		for src, dst, retries in self.get_job():
			self.fire('transfer_started', src, dst)
			try:
				fs = cloudfs(src if self.is_remote_path(src) else dst)
				
				if self.is_remote_path(src):
					result[src] = fs.get(src, dst)
					
				elif self.is_remote_path(dst):
					if os.path.isdir(src):
						for path in os.path.walk(src):
							if os.path.isfile(path):
								with open(path) as fp:
									result[path] = fs.put(fp, dst)
							
					elif os.path.isfile(src):
						with open(src) as srcfp:
							result[src] = fs.put(srcfp, dst)

					else:
						raise BaseException('%s is not a file nor directory' % dst)

			except BaseException, e:
				retries += 1
				if retries < self.max_retry:
					self._retries_queue.put([src,dst,retries])
				else:
					failed[src] = str(e)
				self.fire('transfer_error', src, dst, retries, sys.exc_info())
			else:
				self.fire('transfer_complete', src, dst)
			finally:
				if self._stop_all.isSet():
					return


	def start(self):
		self._stop_all.clar()
		self.running = True
		
		
		#Starting threads
		workers = []
		for n in range(self.num_workers):
			worker = threading.Thread(name="Worker-%s" % n, target=self._worker, 
					args=(self.result, self.failed_files))
			self._logger.debug("Starting worker '%s'", worker.getName())
			worker.start()
			workers.append(worker)
		
		# Join workers
		for worker in workers:
			worker.join()
			LOG.debug("Worker '%s' finished", worker.getName())		
		self.running = False
		self.fire('transfer_complete')
		return self.src, self.dst, self.result, self.failed_files


	def kill(self):
		self._stop_all.set()
		self.running = False
		self.fire('transfer_stopped', self.src, self.dst)
		return self.src, self.dst, self.result, self.failed_files


class LargeTransfer(pubsub.Observable):
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
		self._tranzit_vol.size = 
				int(self.chunk_size * (self._transfer.num_workers) * 1.1)
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


class Manifest(object)
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


def cloudfs(fstype, **driver_kwds):
	raise NotImplementedError()


class CloudFileSystem(object):

	def ls(self, path):
		raise NotImplementedError()

	def stat(self, path):
		'''
		size in bytes
		type = dir | file | container
		'''
		raise NotImplementedError()

	def put(self, srcfp, path):
		raise NotImplementedError()

	def get(self, path, dstfp):
		raise NotImplementedError()

	def delete(self, path):
		raise NotImplementedError()

