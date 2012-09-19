import urlparse

filesystem_types = {}

class Transfer(object):
	UPLOAD = 'upload'
	DOWNLOAD = 'download'

	def __init__(self, src, dst, direction, num_workers=4, listeners=None):
		'''
		@param: src transfer source path
			- str file or directory path. directory processed recursively
			- file-like object
			- list of path strings and file-likes objects
			- generator function that will produce path strings and file-like objects 

		@param: dst transfer destination path
			- str file or directory path
			- file-like object (when direction=download)
			- list of path strings and file-likes objects			
			- generator function that will produce path strings or file-like objects 


		@param: direction 'upload' or 'download'

		# XXX(marat): We should extend class from Observable
		@param: listener function or object to call when file transfer is 
		started, finished, failed, restarted.
			on_start: fn(src, dst, state='start')
			on_complete: fn(src, dst, state='complete', retry=1, transfered_bytes=1892331)
			on_error: fn(src, dst, state='error', retry=1, exc_info=(3 items tuple))
			on_restart: fn(src, dst, state='restart', retry=2)

		Examples:

			Upload pathes
				Transfer('/mnt/backups/daily.tar.gz', 's3://backups/mysql/2012-09-05/', 'upload')

			Upload file object
				Transfer(StringIO('10.167.51.13'), 's3://directory-server/hosts/controller', 'upload')


			Upload generator
				def files():
					yield 'part.1'
					yield 'part.2'
				Transfer(files, 's3://images/ubuntu12.04.1/', 'upload')
			

			Download file-like object
				Transfer('s3://backups/mysql/2012-09-05/daily.tar.gz', StringIO(), 'download')

			Download both generators
				def src():
					yield 's3://backups/mysql/daily.tar.gz'
					yield 'rackspace-cloudfiles://backups/mysql/daily.tar.gz'

				def dst():
					yield '/backups/daily-from-s3.tar.gz'
					yield '/backups/daily-from-cloudfiles.tar.gz'
		'''
		self.src = src
		self.dst = dst
		self.direction = direction
		self.num_workers = num_workers


	def start(self):
		pass


	def kill(self):
		pass

	def join(self, timeout=None):
		pass


	@property
	def running(self):
		pass


	def result(self):
		pass



def LargeTransfer(pubsub.Observable):
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
	def __init__(self, src, dst,
				transfer_id=None,
				tar_it=True,
				gzip_it=True, 
				chunk_size=100, 
				try_pigz=True,
				manifest='manifest.ini' 
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
			else
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

		events = self._transfer._listeners.keys()
		self.define_events(*events)
		for ev in events:
			self._transfer.on(ev=self._proxy_event(ev))
		


	def _src_generator(self):
		'''
		Compress, split, yield out
		'''
		if self._up:
			# Tranzit volume size is chunk for each worker 
			# and Ext filesystem overhead
			self._tranzit_vol.size = 
					int(self.chunk_size * (self._transfer.num_workers) * 1.1)
			self._tranzit_vol.ensure(mkfs=True)
			try:
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
					while filename in self._split(stream, prefix):
						yield filename
					if cmd:
						cmd.communicate() 
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
			fp = open(self.prefix + '%03d' % chunk_num)
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


	def _proxy_event(event):
		def proxy(*args, **kwds):
			self.fire(event, *args, **kwds)
		return proxy


	def _run(self):
		self._transfer.run()


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

