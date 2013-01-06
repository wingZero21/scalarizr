from __future__ import with_statement

import re
import os
import sys
import Queue
import urlparse
import itertools
import tempfile
import subprocess
import threading
import logging
import ConfigParser
import time
import hashlib
import uuid
from copy import copy
if sys.version_info[0:2] >= (2, 7):
	from collections import OrderedDict
else:
	from scalarizr.externals.collections import OrderedDict
try:
	import json
except ImportError:
	import simplejson as json

from scalarizr import storage2
from scalarizr.libs import bases
from scalarizr.linux import coreutils, pkgmgr, LinuxError


LOG = logging.getLogger(__name__)


class Error(Exception):
	pass


class DriverError(Error):
	pass


### to move

class EventInterrupt(Exception):
	pass


class InterruptibleEvent(threading._Event):
	"""
	Inheritance from threading._Event because threading.Event simply returns
	threading._Event instance.

	Use-case: One-time set-wait/wait-set cycle for two threads: one waiter and
	one setter. Allows setter to raise an exception in waiter by calling
	interrupt() instead of set().

	TODO: to make behavior more universal, all operations on self._exception
	must be covered with self.__cond acquire-release calls. Self.__cond can
	be accessed by defining self.__cond = self._Event__cond. Also properly
	overriden clear method must be added.
	"""

	def __init__(self):
		super(InterruptibleEvent, self).__init__()
		self._exception = None

	def interrupt(self, exc=None):
		""" Raise exc or default EventInterrupt() in the waiting thread """
		self._exception = exc if exc else EventInterrupt()
		return super(InterruptibleEvent, self).set()

	def wait(self, timeout=None):
		wait_ret = super(InterruptibleEvent, self).wait(timeout)
		if self._exception:
			raise self._exception
		else:
			return wait_ret

###


class namedstream(object):
	def __init__(self, stream, name):
		self._stream = stream
		self.name = name

	def __getattr__(self, name):
		if name in self.__dict__:
			return self.__dict__[name]
		return getattr(self.__dict__['_stream'], name)

	def __hasattr__(self, name):
		return hasattr(self.__dict__['_stream'], name) or \
				name in self.__dict__


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
			try:
				iter(src)
			except:
				src = [src]
		if callable(dst):
			dst = (item for item in dst())
		else:
			try:
				iter(dst)
			except:
				dst = itertools.repeat(dst)

		super(BaseTransfer, self).__init__(src=src, dst=dst, **kwds)
		self.define_events('transfer_start', 'transfer_error', 'transfer_complete')


class FileTransfer(BaseTransfer):
	# Drivers' get and put methods (multipart_put?) must
	# support report_to arg, see progress_report_cb in _worker.


	_url_re = re.compile(r'^[\w-]+://')

	def __init__(self, num_workers=8, retries=3, multipart=False, **kwds):
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
		if not (not isinstance(kwds["src"], basestring) and
				isinstance(kwds["dst"], basestring)) and multipart:
			multipart = False

		super(FileTransfer, self).__init__(num_workers=num_workers, 
						retries=retries, multipart=multipart, **kwds)
		self.define_events('progress_report')

		self._completed = []
		self._failed = []
		self._retries_queue = Queue.Queue()
		self._stop_all = threading.Event()
		self._stopped = threading.Event()
		self._gen_lock = threading.RLock()
		self._worker_lock = threading.Lock()
		self._upload_id = None
		self._chunk_num = -1
		self._multipart_result = None

		
	def _job_generator(self):
		try:
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
						chunk_num = self._chunk_num
					LOG.debug("FileTransfer yield %s %s %s %s" % (src, dst, retry, chunk_num))
					yield src, dst, retry, chunk_num
				except StopIteration:
					no_more = True
		except (StopIteration, GeneratorExit):
			raise
		except:
			LOG.debug('FileTransfer _job_generator failed: %s', 
					sys.exc_info()[1], exc_info=sys.exc_info())
			raise
			
			
	def _is_remote_path(self, path):
		return isinstance(path, basestring) and self._url_re.match(path)

				
	def _worker(self):
		driver = None
		for src, dst, retry, chunk_num in self._job_generator():

			def progress_report_cb(uploaded, total):
				self.fire("progress_report", src, dst, retry, chunk_num, uploaded, total)

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

				zero = int(time.time())
				if uploading:
					if self.multipart:
						driver.multipart_put(self._upload_id, chunk_num, src)
					else:
						driver.put(src, dst, report_to=progress_report_cb)
						LOG.debug("*** BENCH %s %s uploaded" % (int(time.time() - zero), os.path.basename(src)))
					self._completed.append({
							'src': src,
							'dst': dst,
							'chunk_num': chunk_num,
							'size': os.path.getsize(src)})
				else:
					driver.get(src, dst, report_to=progress_report_cb)
					LOG.debug("*** BENCH %s %s downloaded" % (int(time.time() - zero), os.path.basename(src)))
					self._completed.append({
							'src': src,
							'dst': dst,
							'size': os.path.getsize(dst)})
				self.fire('transfer_complete', src, dst, retry, chunk_num)

			except AssertionError:
				self.fire('transfer_error', src, dst, retry, chunk_num, 
							sys.exc_info())
			except:
				LOG.debug('FileTransfer failed %s -> %s. Error: %s', 
						src, dst, sys.exc_info()[1], 
						exc_info=sys.exc_info())
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
				self._multipart_result = driver.multipart_complete(self._upload_id)
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
				LOG.debug("Worker '%s' join...", worker.getName())
				worker.join()
				LOG.debug("Worker '%s' finished", worker.getName())
			return {
				'completed': self._completed,
				'failed': self._failed,
				'multipart_result': self._multipart_result,
			}
		finally:
			self._stopped.set()


	def kill(self, timeout=None):
		self._stop_all.set()
		self._stopped.wait(timeout)


	def kill_nowait(self):
		return self.kill(0)




class LargeTransfer(bases.Task):
	'''
	SQL dump. File-per-database.
	---------------------------

	def src_gen():
		yield stream = mysqldump ${database}
	def dst_gen():
		yield ${database}

	s3://.../${transfer_id}/manifest.json
							${database_1}.gz.00
							${database_1}.gz.01
							${database_2}.gz.00
							${database_2}.gz.01
							${database_2}.gz.02


	Directory backup
	----------------

	src = '/mnt/dbbackup'
	dst = 's3://backup/key1/key2/'

	s3://backup/key1/key2/${transfer_id}/manifest.json
	s3://backup/key1/key2/${transfer_id}/part.gz.00
	s3://backup/key1/key2/${transfer_id}/part.gz.01
	s3://backup/key1/key2/${transfer_id}/part.gz.02


	Directory restore
	-----------------

	src = s3://backup/key1/key2/eph-snap-12345678/manifest.json
	dst = /mnt/dbbackup/

	1. Download manifest 
	2. <chunk downloader> | funzip | tar -x -C /mnt/dbbackup
	'''

	# TODO: bubble exceptions

	# TODO: unlimited disk case: download all before unpacking

	# TODO: benchmarks bzip against pigz
	# http://tukaani.org/lzma/benchmarks.html

	# TODO: subprocess hang problem
	# python 2.7.3 @ ubuntu 12.04 works
	# python 2.7.2 @ ubuntu 11.10 fail
	# python 2.6.7 @ ubuntu 11.10 works
	# python 2.6.5 @ ubuntu 10.04 works
	# solutions: waiter thread that would kill everything on hang or
	# separate thread for subprocess opening? or just document it :)

	# NOTE: use directory dst for uploading.
	pigz_bin = '/usr/bin/pigz'
	gzip_bin = '/bin/gzip'

	def __init__(self, src, dst,
				transfer_id=None,
				streamer="tar",
				compressor="gzip",
				chunk_size=100, 
				try_pigz=True,
				manifest='manifest.json',
				description='',
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
		if self._up and isinstance(src, basestring) and os.path.isdir(src) and not streamer:
			raise ValueError('Passed src is a directory. streamer expected')
		if self._up:
			if callable(src):
				src = (item for item in src())
			else:
				if not hasattr(src, '__iter__') or hasattr(src, "read"):
					src = [src]
				src = iter(src)
			if callable(dst):
				dst = (item for item in dst())
			elif not hasattr(dst, '__iter__'):
				dst = itertools.repeat(dst)
			else:
				dst = iter(dst)

		super(LargeTransfer, self).__init__()

		self.description = description
		self.tags = tags
		self.multipart = kwds.get("multipart")
		self.src = src
		self.dst = dst
		self.streamer = streamer
		self.compressor = compressor
		self.chunk_size = chunk_size
		self.try_pigz = try_pigz
		self._upload_res = None
		self._restorer = None
		self._killed = False
		self._manifest_ready = InterruptibleEvent()
		self._chunks_events_access = threading.Lock()
		self.files = None
		if transfer_id is None:
			self.transfer_id = uuid.uuid4().hex
		else:
			self.transfer_id = transfer_id
		self.manifest = manifest
		self._transfer = FileTransfer(src=self._src_generator,
								dst=self._dst_generator, **kwds)
		self._tranzit_vol = storage2.volume(
								type='tmpfs',
								mpoint=tempfile.mkdtemp())

		events = self._transfer.list_events()
		self.define_events(*events)
		for ev in events:
			self._transfer.on(ev, self._proxy_event(ev))


	def _gzip_bin(self):
		if self.try_pigz:
			try:
				pkgmgr.installed("pigz")
			except LinuxError, e:
				if "No matching Packages to list" in e.err:
					try:
						pkgmgr.epel_repository()
						pkgmgr.installed("pigz")
					except:
						LOG.debug("PIGZ install with epel failed, using gzip."\
								  " Caught %s" % repr(sys.exc_info()[1]))
					else:
						return self.pigz_bin
				else:
					LOG.debug("PIGZ install failed, using gzip. Caught %s" %
							  repr(sys.exc_info()[1]))
			except:
				LOG.debug("PIGZ install failed, using gzip. Caught %s" %
						  repr(sys.exc_info()[1]))
			else:
				return self.pigz_bin
		return self.gzip_bin


	def _proxy_event(self, event):
		def proxy(*args, **kwds):
			self.fire(event, *args, **kwds)
		return proxy
		

	def _src_generator(self):
		'''
		Compress, split, yield out
		'''
		if self._up:
			# Tranzit volume size is chunk for each worker
			# and Ext filesystem overhead

			# manifest is not used in multipart uploads and is left
			# here to avoid writing 'if' everywhere
			manifest = Manifest()
			manifest["description"] = self.description
			if self.tags:
				manifest["tags"] = self.tags

			def delete_uploaded_chunk(src, dst, retry, chunk_num):
				os.remove(src)
			self._transfer.on(transfer_complete=delete_uploaded_chunk)

			for src in self.src:
				LOG.debug('src: %s, type: %s', src, type(src))
				fileinfo = {
					"name": '',
					"streamer": None,
					"compressor": None,
					"chunks": [],
				}
				prefix = self._tranzit_vol.mpoint
				stream = None
				cmd = tar = gzip = None

				if hasattr(src, 'read'):
					stream = src
					if hasattr(stream, 'name'):
						# os.pipe stream has name '<fdopen>'
						name = os.path.basename(stream.name).strip('<>')  #? can stream name end with '/'
					else:
						name = 'stream-%s' % hash(stream)
					fileinfo["name"] = name
					prefix = os.path.join(prefix, name) + '.'
				elif self.streamer and isinstance(src, basestring) and os.path.isdir(src):
					name = os.path.basename(src.rstrip('/'))
					fileinfo["name"] = name

					if self.streamer == "tar":
						fileinfo["streamer"] = "tar"
						prefix = os.path.join(prefix, name) + '.tar.'

						if src.endswith('/'):  # tar dir content
							tar_cmdargs = ['/bin/tar', 'cp', '-C', src, '.']
						else:
							parent, target = os.path.split(src)
							tar_cmdargs = ['/bin/tar', 'cp',  '-C', parent, target]

						LOG.debug("LargeTransfer src_generator TAR POPEN")
						tar = cmd = subprocess.Popen(
										tar_cmdargs,
										stdout=subprocess.PIPE,
										stderr=subprocess.PIPE,
										close_fds=True)
						LOG.debug("LargeTransfer src_generator AFTER TAR")
					elif hasattr(self.streamer, "popen"):
						fileinfo["streamer"] = str(self.streamer)
						prefix = os.path.join(prefix, name) + '.'

						LOG.debug("LargeTransfer src_generator custom streamer POPEN")
						# TODO: self.streamer.args += src
						tar = cmd = self.streamer.popen(stdin=None)
						LOG.debug("LargeTransfer src_generator after custom streamer POPEN")
					stream = tar.stdout
				elif isinstance(src, basestring) and os.path.isfile(src):
					name = os.path.basename(src)
					fileinfo["name"] = name
					prefix = os.path.join(prefix, name) + '.'

					stream = open(src)
				else:
					raise ValueError('Unsupported src: %s' % src)

				if self.compressor == "gzip":
					fileinfo["compressor"] = "gzip"
					prefix += 'gz.'
					LOG.debug("LargeTransfer src_generator GZIP POPEN")
					gzip = cmd = subprocess.Popen(
								[self._gzip_bin(), '-5'],
								stdin=stream,
								stdout=subprocess.PIPE,
								stderr=subprocess.PIPE,
								close_fds=True)
					LOG.debug("LargeTransfer src_generator AFTER GZIP")
					if tar:
						# Allow tar to receive SIGPIPE if gzip exits.
						tar.stdout.close()
					stream = gzip.stdout
				# custom compressor
				elif hasattr(self.compressor, "popen"):
					fileinfo["compressor"] = str(self.compressor)
					LOG.debug("LargeTransfer src_generator custom compressor POPEN")
					cmd = self.compressor.popen(stdin=stream)
					LOG.debug("LargeTransfer src_generator after custom compressor POPEN")
					if tar:
						tar.stdout.close()
					stream = cmd.stdout

				for filename, md5sum in self._split(stream, prefix):
					fileinfo["chunks"].append((os.path.basename(filename), md5sum))
					LOG.debug("LargeTransfer src_generator yield %s" % filename)
					yield filename
				if cmd:
					cmd.communicate()

				manifest["files"].append(fileinfo)

			# send manifest to file transfer
			if not self.multipart:
				LOG.debug("Manifest: %s" % manifest.data)
				manifest_f = os.path.join(self._tranzit_vol.mpoint, self.manifest)
				manifest.write(manifest_f)
				LOG.debug("LargeTransfer yield %s" % manifest_f)
				yield manifest_f

		elif not self._up:
			def on_transfer_error(*args):
				LOG.debug("transfer_error event, shutting down")
				self.kill()
			self._transfer.on(transfer_error=on_transfer_error)

			# The first yielded object will be the manifest, so
			# catch_manifest is a listener that's supposed to trigger only
			# once and unsubscribe itself.
			def wait_manifest(src, dst, retry, chunk_num):
				self._transfer.un('transfer_complete', wait_manifest)
				self._manifest_ready.set()
			self._transfer.on(transfer_complete=wait_manifest)


			manifest_path = self.src
			yield manifest_path

			#? except EventInterrupt: save exc and return
			self._manifest_ready.wait()

			# we should have the manifest on the tmpfs by now
			manifest_local = os.path.join(self._tranzit_vol.mpoint,
				os.path.basename(manifest_path))
			manifest = Manifest(manifest_local)
			os.remove(manifest_local)
			remote_path = os.path.dirname(manifest_path)

			# add ready and done events to each chunk without breaking the
			# chunk order
			with self._chunks_events_access:
				if not self._killed:
					self.files = copy(manifest["files"])
					for file in self.files:
						file["chunks"] = OrderedDict([(
							basename, {
								"md5sum": md5sum,
								"downloaded": InterruptibleEvent(),
								"processed": InterruptibleEvent()
							}
						) for basename, md5sum in file["chunks"]])

			# launch restorer
			if self._restorer is None:
				LOG.debug("STARTING RESTORER")
				self._restorer = threading.Thread(target=self._dl_restorer)
				self._restorer.start()

			def wait_chunk(src, dst, retry, chunk_num):
				chunk_name = os.path.basename(src)
				for file in self.files:
					if chunk_name in file["chunks"]:
						chunk = file["chunks"][chunk_name]
				chunk["downloaded"].set()
				chunk["processed"].wait()
				os.remove(os.path.join(dst, chunk_name))
			self._transfer.on(transfer_complete=wait_chunk)

			for file in self.files:
				for chunk in file["chunks"]:
					yield os.path.join(remote_path, chunk)


	def _dst_generator(self):
		if self._up:
			# last yield for manifest
			# NOTE: this only works if dst is a dir
			for dst in self.dst:
				# has sense only if not multipart
				self._upload_res = os.path.join(dst, self.transfer_id, self.manifest)

				yield os.path.join(dst, self.transfer_id, '')
		else:
			while True:
				yield self._tranzit_vol.mpoint


	def _split(self, stream, prefix):
		buf_size = 4096
		chunk_size = self.chunk_size * 1024 * 1024

		for chunk_n in itertools.count():
			chunk_name = prefix + '%03d' % chunk_n
			chunk_capacity = chunk_size
			chunk_md5 = hashlib.md5()

			zero = int(time.time())
			with open(chunk_name, 'w') as chunk:
				while chunk_capacity:
					bytes = stream.read(min(buf_size, chunk_capacity))
					if not bytes:
						break
					chunk.write(bytes)
					chunk_capacity -= len(bytes)
					chunk_md5.update(bytes)

			if chunk_capacity != chunk_size:  # non-empty chunk
				LOG.debug("*** BENCH %s %s created" % (int(time.time() - zero), os.path.basename(chunk_name)))
				yield chunk_name, chunk_md5.hexdigest()
			else:  # empty chunk
				os.remove(chunk_name)
			if chunk_capacity:  # empty or half-empty chunk, meaning stream
								# is empty
				break


	def _dl_restorer(self):
		buf_size = 4096

		for file in self.files:
			dst = self.dst

			LOG.debug("RESTORER start")
			LOG.debug("RESTORER file %s to %s" % (file["name"], dst))

			# temporary fix for overriding download manifest settings with
			# custom streamer
			if hasattr(self.streamer, "popen"):
				file["streamer"] = str(self.streamer)

			# create 'cmd' and 'stream'
			if not file["streamer"] and not file["compressor"]:
				cmd = None
				stream = open(os.path.join(dst, file["name"]), 'w')
			else:
				compressor_out = subprocess.PIPE

				if file["compressor"]:
					if not file["streamer"]:
						compressor_out = open(os.path.join(dst, file["name"]), 'w')

					if file["compressor"] == "gzip":
						LOG.debug("RESTORER unzip popen")
						cmd = subprocess.Popen([self._gzip_bin(), "-d"],
							stdin=subprocess.PIPE,
							stdout=compressor_out,
							stderr=subprocess.PIPE,
							close_fds=True)
						LOG.debug("RESTORER after unzip")
					else:  # custom compressor
						LOG.debug("RESTORER custom decompressor popen")
						cmd = self.compressor.popen(stdout=compressor_out)
						LOG.debug("RESTORER after custom decompressor popen")
					stream = cmd.stdin

				if file["streamer"]:
					if file["compressor"]:
						compressor_out = cmd.stdout

					if file["streamer"] == "tar":
						LOG.debug("RESTORER untar popen")
						cmd = subprocess.Popen(['/bin/tar', '-x', '-C', dst],
							stdin=compressor_out,
							stdout=subprocess.PIPE,
							stderr=subprocess.PIPE,
							close_fds=True)
						LOG.debug("RESTORER after untar")
					else:  # custom streamer
						LOG.debug("RESTORER custom decompressor popen")
						cmd = self.streamer.popen(stdin=compressor_out)
						LOG.debug("RESTORER after custom decompressor popen")

					if file["compressor"]:
						compressor_out.close()
					else:
						stream = cmd.stdin

			try:
				for chunk, info in file["chunks"].iteritems():

					LOG.debug("RESTORER before wait %s" % chunk)
					info["downloaded"].wait()
					zero = int(time.time())

					location = os.path.join(self._tranzit_vol.mpoint, chunk)
					with open(location, 'rb') as fd:
						while True:
							bytes = fd.read(buf_size)
							if not bytes:
								LOG.debug("RESTORER break %s" % chunk)
								LOG.debug("*** BENCH %s %s restored" % (int(time.time() - zero), chunk))
								break
							try:
								stream.write(bytes)

							except Exception, e:
								if isinstance(e, IOError) and e.errno == 32:
									LOG.debug("RESTORER encountered broken"
											  " pipe, err msg from the last"
											  " supbrocess: %s" % cmd.stderr.read())
								self.kill()
								raise

					info["processed"].set()  # this leads to chunk removal
			finally:
				stream.close()

				#? wait for unzip first in case of tar&gzip

				if cmd:
					LOG.debug("RESTORER cmd wait")
					cmd.wait()
					LOG.debug("LargeTransfer download: finished restoring")


	def _run(self):
		LOG.debug("Creating tmpfs")
		self._tranzit_vol.size = int(self.chunk_size * self._transfer.num_workers * 1.2)
		self._tranzit_vol.ensure(mkfs=True)
		try:
			res = self._transfer.run()
			LOG.debug("self._transfer finished")

			if not self._up:
				if self._restorer:
					LOG.debug("waiting restorer to finish...")
					self._restorer.join()
				return res

			elif self._up:
				if res["failed"] or self._transfer._stop_all.is_set():
					#? upload failed inside FileTransfer or it was killed
					self._s3_cleanup()
					return
				if self.multipart:
					return res["multipart_result"]
				else:
					return self._upload_res
		finally:
			LOG.debug("Destroying tmpfs")
			self._tranzit_vol.destroy()
			coreutils.remove(self._tranzit_vol.mpoint)


	def _s3_cleanup(self):
		LOG.debug("Performing S3 clean up")
		path = os.path.join(self.dst.next(), self.transfer_id)
		driver = cloudfs(urlparse.urlparse(path).scheme)

		pieces = Queue.Queue()
		for piece in driver.ls(path):
			pieces.put(piece)

		def delete():
			driver = cloudfs(urlparse.urlparse(path).scheme)
			while True:
				try:
					driver.delete(pieces.get_nowait())
				except Queue.Empty:
					return

		threads = [threading.Thread(target=delete) for i in range(min(4,
			pieces.qsize()))]  #? 4
		if threads:
			map(lambda x: x.start(), threads)
			map(lambda x: x.join(), threads)


	def kill(self):
		self._killed = True
		self._transfer.kill_nowait()

		# interrupt all events
		self._manifest_ready.interrupt()

		with self._chunks_events_access:
			if self.files:
				for file in self.files:
					for chunk, chunkinfo in file["chunks"].items():
						chunkinfo["downloaded"].interrupt()
						chunkinfo["processed"].interrupt()

		def interrupt(*args):
			raise Exception("S3 transfer was interrupted by LargeTransfer.kill()")  #?
		self._transfer.on(progress_report=interrupt)



class Manifest(object):
	"""
	manifest.json
	-------------

	{
		version: 2.0,
		description,
		tags,
		created_at,
		files: [
			{
				name,
				streamer,  # "tar" | python function | None
				compressor,  # "gzip" | python function | None
				chunks: [(basename001, md5sum)]
			}
		]
	}


	Supports reading of old ini-manifests and represents their data in the
	new-manifest style.

	Make sure to write to a file with '.json' extension.
	"""

	filename = None
	cloudfs_path = None

	def __init__(self, filename=None, cloudfs_path=None):
		self.reset()
		if filename:
			self.read(filename)
			self.filename = filename
		elif cloudfs_path:
			cfs = cloudfs(urlparse.urlparse(cloudfs_path).scheme)
			target_dir = tempfile.mkdtemp() 
			cfs.get(cloudfs_path, target_dir)
			try:
				self.read(os.path.join(target_dir, os.path.basename(cloudfs_path)))
				self.cloudfs_path = cloudfs_path
			finally:
				coreutils.remove(target_dir)

	def reset(self):
		self.data = {
			"version": 2.0,
			"description": '',
			"tags": {},
			"files": [],
		}

	def read(self, filename):
		if filename.endswith(".json"):
			self.data = self._read_json(filename)
		elif filename.endswith(".ini"):
			self.data = self._read_ini(filename)
		else:
			raise TypeError(".json or .ini manifests only, got %s" % filename)
		return self

	def write(self, filename):
		self.data["created_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
		with open(filename, 'w') as fd:
			fd.write(json.dumps(self.data) + '\n')

	def _read_json(self, filename):
		with open(filename) as fd:
			return json.load(fd)

	def _read_ini(self, filename):
		"""
		Backward compatibility with the old ini manifests.


		manifest.ini
		------------

		[snapshot]
		description = description here
		created_at = datetime
		pack_method = "pigz"

		[chunks]
		${database_1}.gz.part00 = md5sum
		${database_1}.gz.part01 = md5sum


		Chunks are parts of a single directory packed with tar and gz. They
		were stored unordered.
		"""
		parser = ConfigParser.ConfigParser()
		parser.read(filename)

		# get name using the first chunk name
		chunkname = parser.options("chunks")[0]
		chunkname = chunkname.rsplit('.', 1)[0]  # strip part number
		if chunkname.endswith(".tar.gz"):  # this should always be true
			name = chunkname[:-7]
		else:
			name = chunkname  # just in case
		streamer = "tar"
		compressor = "gzip"

		chunks = parser.items("chunks")
		chunks.sort()

		return {
			"version": 1.0,
			"description": parser.get("snapshot", "description"),
			"tags": {},
			"created_at": parser.get("snapshot", "created_at"),
			"files": [
				{
					"name": name,
					"streamer": streamer,
					"compressor": compressor,
					"chunks": chunks,
				}
			]
		}

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

	def meta():
		def fget(self):
			ret = dict((key.split('.', 1)[1], self['tags'][key]) \
				for key in self['tags'] \
				if key.startswith('meta.'))
			LOG.debug('meta: %s', ret)
			return ret

		def fset(self, meta):
			for key, value in meta.items():
				self['tags']['meta.%s' % key] = value

		return locals()
	meta = property(**meta())

	def save(self):
		if self.cloudfs_path:
			cfs = cloudfs(urlparse.urlparse(self.cloudfs_path).scheme)
			source = tempfile.mkstemp()[1] + '.json'
			self.write(source)
			try:
				cfs.put(source, self.cloudfs_path)
			finally:
				coreutils.remove(source)
		elif self.filename:
			self.write(self.filename)


cloudfs_types = {}


def cloudfs(fstype, **driver_kwds):
	if fstype not in cloudfs_types:
		__import__('scalarizr.storage2.cloudfs.%s' % fstype)
	return cloudfs_types[fstype](**driver_kwds)


class CloudFileSystem(object):

	features = {
		'multipart': False
	}

	def parseurl(self, url):
		"""
		{
			'bucket',
			'path'

		}
		{
			'container',
			'object'

		}
		"""
		raise NotImplementedError()

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
