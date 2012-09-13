import urlparse


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
	
	

def cloudfs(fstype, **driver_kwds):
	raise NotImplementedError()


class CloudFileSystem(object):

	def ls(self, path):
		raise NotImplementedError()

	def stat(self, path):
		raise NotImplementedError()

	def put(self, srcfp, path):
		raise NotImplementedError()

	def get(self, path, dstfp):
		raise NotImplementedError()

	def delete(self, path):
		raise NotImplementedError()
