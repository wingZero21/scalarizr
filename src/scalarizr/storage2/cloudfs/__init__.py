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
import json
import time
import hashlib
from io import BytesIO
from copy import copy
if sys.version_info[0:2] >= (2, 7):
	from collections import OrderedDict
else:
	from scalarizr.externals.collections import OrderedDict

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
		

class FileTransfer(BaseTransfer):

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


		Usage:
			==========  ==========  ==========  ====================================
			SRC         DST         MULTIPART   EXPECTED INPUT
			==========  ==========  ==========  ====================================
			str         str         -           src: path, dst: path
			iter        str         False       src: paths, dst: path
			iter        str         True        src: chunks of a file, dst: path
			iter        iter        -           src: paths, dst: paths
			==========  ==========  ==========  ====================================

		'''
		#? move to BaseTransfer
		if not (not isinstance(kwds["src"], basestring) and
				isinstance(kwds["dst"], basestring)) and multipart:
			multipart = False

		super(FileTransfer, self).__init__(num_workers=num_workers, 
						retries=retries, multipart=multipart, **kwds)

		self._completed = []
		self._failed = []
		self._retries_queue = Queue.Queue()
		self._stop_all = threading.Event()
		self._stopped = threading.Event()
		self._gen_lock = threading.RLock()
		self._worker_lock = threading.Lock()
		self._upload_id = None
		self._chunk_num = -1

		
	def _job_generator(self):
		no_more = False
		while True:
			try:
				yield self._retries_queue.get_nowait()
				continue
			except Queue.Empty:
				if no_more:
					raise StopIteration
			try:
				with self._gen_lock:
					src = self.src.next()
					dst = self.dst.next()
					retry = 0
					if self.multipart:
						self._chunk_num += 1
					yield src, dst, retry, self._chunk_num
			except StopIteration:
				no_more = True
			
			
	def _is_remote_path(self, path):
		return isinstance(path, basestring) and self._url_re.match(path)
				
				
	def _worker(self):
		driver = None
		for src, dst, retry, chunk_num in self._job_generator():
			self.fire('transfer_start', src, dst, retry, chunk_num)
			try:
				uploading = self._is_remote_path(dst) and os.path.isfile(src)
				downloading = self._is_remote_path(src) and not self._is_remote_path(dst)
				assert not (uploading and downloading)
				assert uploading or downloading

				rem, loc = (dst, src) if uploading else (src, dst)
				if not driver:
					driver = cloudfs(urlparse.urlparse(rem).scheme)
				with self._worker_lock:
					if self.multipart and not self._upload_id:
						chunk_size = os.path.getsize(loc)
						self._upload_id = driver.multipart_init(rem, chunk_size)

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
			# 'if driver' condition prevents threads that did nothing from
			# entering (could happen in case num_workers > chunks)
			if driver and self.multipart and self._upload_id:
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

	pigz_bin = '/usr/bin/pigz'
	gzip_bin = '/bin/gzip'
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
				tags=None,
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

		super(LargeTransfer, self).__init__()

		self._stop = False
		self.tags = tags
		self.multipart = kwds.get("multipart")
		self.direction = direction
		self.src = src
		self.dst = dst
		self.tar_it = tar_it
		self.gzip_it = gzip_it
		self.chunk_size = chunk_size
		self.try_pigz = try_pigz
		self.transfer_id = transfer_id
		self.manifest = manifest
		self._transfer = FileTransfer(self._src_generator, 
								self._dst_generator, **kwds)
		self._tranzit_vol = storage2.volume(
								type='tmpfs',
								mpoint=tempfile.mkdtemp())
		self._chunk_num = -1
		self._given_chunks = OrderedDict()
		self._restoration_queue = Queue.Queue()
		self._dl_lock = threading.Lock()

		events = self._transfer.listeners.keys()  #? _listeners
		self.define_events(*events)
#		for ev in events:
#			self._transfer.on(ev=self._proxy_event(ev))
		


	def _src_generator(self):
		'''
		Compress, split, yield out
		'''
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
					prefix = os.path.join(prefix, os.path.basename(src.rstrip('/'))) + '.tar.'
					#prefix = os.path.join(prefix, 'part.')
					tar = cmd = subprocess.Popen(
									#['/bin/tar', 'cp', '-C', src, '.'],
									['/bin/tar', 'cp', src],
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

			# upload manifest
			if not self.multipart:
				manifest = Manifest()
				if False:  #?
					manifest["snapshot"]["description"] = ''
				manifest["snapshot"]["pack_method"] = self._gzip_bin()[-4:] \
					if self.gzip_it else "none"
				if self.tags:
					manifest["snapshot"]["tags"] = self.tags
				manifest["chunks"] = self._given_chunks

				m_file = os.path.join(self._tranzit_vol.mpoint, self.manifest)
				manifest.write(m_file)
				yield m_file
		else:
			src = self.src.next()
			src_pr = urlparse.urlparse(src)
			drv = cloudfs(src_pr.scheme)
			filename = drv.get(src, self._tranzit_vol.mpoint)
			manifest = Manifest(filename)
			self._chunks = OrderedDict()
			chunk_group = -1
			for chunk, checksum in manifest["chunks"].iteritems():
				if chunk.endswith("000"):
					chunk_group += 1
				self._chunks[chunk] = {
					"checksum": checksum,
					"ready": False,
					"group": chunk_group,
				}
			self._unpack_bin = None
			if manifest["snapshot"]["pack_method"] in ("gzip", "pigz"):
				self._unpack_bin = self._gzip_bin()
			os.remove(filename)

			prefix = os.path.dirname(src)
			for chunk in self._chunks:
				yield prefix + '/' + chunk

			"""
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
			"""



	def _dst_generator(self):
		if self.direction == self.UPLOAD:
			for dst in self.dst:
				yield dst
			# last yield for manifest
			# TODO: this only works if dst is a dir
			else:
				yield dst
		else:
			while True:
				yield self._tranzit_vol.mpoint


	def _split(self, stream, prefix):
		buf_size = 4096
		chunk_size = self.chunk_size * 1024 * 1024
		read_bytes = 0
		self._chunk_num = -1
		fp = None

		def next_chunk():
			self._chunk_num += 1
			return open(prefix + '%03d' % self._chunk_num, 'w'), hashlib.md5()
		fp, md5sum = next_chunk()

		while True:
			size = min(buf_size, chunk_size - read_bytes)
			bytes = stream.read(size)
			if not bytes:
				if fp:
					fp.close()
				break
			read_bytes += len(bytes)
			fp.write(bytes)
			md5sum.update(bytes)
			if read_bytes == chunk_size:
				fp.close()
				self._given_chunks[os.path.basename(fp.name)] = md5sum.hexdigest()
				yield fp.name
				fp, md5sum = next_chunk()


	def _gzip_bin(self):
		if self.try_pigz and os.path.exists(self.pigz_bin):
			return self.pigz_bin
		return self.gzip_bin


	#? infinite fire loop?
	def _proxy_event(self, event):
		def proxy(*args, **kwds):
			self.fire(event, *args, **kwds)
		return proxy


	def _dl_restorer(self):
		# local dir or file
		# NOTE: file is expected only when downloading a single file!
		dst = self.dst.next()

		while True:
			group = self._restoration_queue.get()
			if group is None:
				return

			stream = BytesIO()
			for chunk, chunk_info in self._chunks.iteritems():
				if chunk_info["group"] != group:
					continue
				else:
					if chunk.endswith(".000"):
						name = chunk[:-4]
					with open(os.path.join([self._tranzit_vol.mpoint, chunk]),
							'rb') as fd:
						stream.write(fd.read())
			stream.seek(0)

			if self._unpack_bin is not None:
				gzip = subprocess.Popen([self._unpack_bin, '-d'],
					stdin=stream, stdout=subprocess.PIPE,
					stderr=subprocess.PIPE, close_fds=True)
				stream = BytesIO(gzip.communicate()[0])
				name = name[:-3]  # strip .gz

			if name.endswith(".tar"):  # tar archive
				tar = subprocess.Popen(['/bin/tar', '-x', '-C', dst],
					stdin = stream, close_fds=True)
				tar.communicate()
			else:  # file
				if os.path.isdir(dst):
					path = os.path.join(dst, name)
				else:
					path = dst
				with open(path, "w") as fd:
					fd.write(stream.read())


	def _dl_transfer_complete(self, *args, **kwargs):
		chunk = os.path.basename(args[0])
		group = self._chunks[chunk]["group"]

		with self._dl_lock:
			self._chunks[chunk]["ready"] = True

			for chunk, chunk_info in self._chunks.iteritems():
				if chunk_info["group"] != group:
					continue
				elif not chunk_info["ready"]:
					break
			else:
				self._restoration_queue.put(group)


	def _dl_transfer_error(self, *args, **kwargs):
		self._transfer.kill()
		

	def _run(self):
		self._tranzit_vol.size = int(self.chunk_size * self._transfer.num_workers * 1.1)
		self._tranzit_vol.ensure(mkfs=True)
		try:
			if self.direction == self.DOWNLOAD:
				self._transfer.on(transfer_error=self._dl_transfer_error)
				self._transfer.on(transfer_complete=self._dl_transfer_complete)
				# launch _restorer

			res = self._transfer.run()

			if self.direction == self.DOWNLOAD:
				self._restoration_queue.put(None)
				# join _restorer

		finally:
			self._tranzit_vol.destroy()
			coreutils.remove(self._tranzit_vol.mpoint)


class Manifest(object):
	"""
	Will perform JSON serialization on every dict-value when writing to file.
	Only [snapshot][tags] will be converted back to dict when parsing though.

	LargeTransfer-specific: default sections value
	"""

	def __init__(self, readfile=None, sections=("snapshot", "chunks")):
		self._predefined_sections = copy(sections)
		self.reset()
		if readfile:
			self.read(readfile)

	def reset(self):
		self.data = OrderedDict([(section, OrderedDict())
			for section in self._predefined_sections])

	def __getitem__(self, item):
		return self.data.__getitem__(item)

	def __setitem__(self, key, value):
		return self.data.__setitem__(key, value)

	def __delitem__(self, key):
		return self.data.__delitem__(key)

	def __iter__(self):
		return self.data.__iter__()

	def __contains__(self, value):
		return self.data.__contains__(value)

	def read(self, filename):
		parser = ConfigParser.ConfigParser()
		parser.read(filename)

		for section in parser.sections():
			self.data[section] = OrderedDict(parser.items(section))

		try:
			self.data["snapshot"]["tags"] = json.loads(
				self.data["snapshot"]["tags"])
		except KeyError:
			pass

	def write(self, filename):  #? accept file objects
		self.data["snapshot"]["created_at"] = str(int(time.time()))

		parser = ConfigParser.ConfigParser()

		for section in self.data:
			parser.add_section(section)
			for option, value in self.data[section].iteritems():
				if not isinstance(value, basestring):
					value = self._format_value(value)
				parser.set(section, option, value)

		with open(filename, 'w') as fd:
			parser.write(fd)

	def _format_value(self, value):
		if isinstance(value, dict):
			return json.dumps(value)
		else:
			return str(value)


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
