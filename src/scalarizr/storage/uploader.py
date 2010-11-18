'''
Created on Aug 25, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.bus import bus
from scalarizr.platform import PlatformError

from Queue import Queue, Empty
from threading import Thread, Lock
from boto.s3.key import Key
from boto.exception import BotoServerError
import os, logging
import cloudfiles


class Uploader(object):
	_queue = None
	state = None
	
	def __init__(self, pool=2, max_attempts=3, logger=None):
		self._logger = logger or logging.getLogger(__name__) 
		self._queue = Queue()
		self._pool = pool
		self._max_attempts = max_attempts
	
	def upload(self, files, UploadDest, progress_cb=None):
		# Enqueue 
		for file in files:
			self._queue.put((file, 0)) 
			
		self.state = "starting"
		self._uploaders = []
		self._failed_files = []
		self._result = []
		self._failed_files_lock = Lock()
		
		#Starting threads
		for n in range(self._pool):
			uploader = Thread(name="Uploader-%s" % n, target=self._worker, 
					args=(UploadDest,))
			self._logger.debug("Starting uploader '%s'", uploader.getName())
			uploader.start()
			self._uploaders.append(uploader)
		
		# Join workers
		self.state = "in-progress"
		for uploader in self._uploaders:
			uploader.join()
			self._logger.debug("Uploader '%s' finished", uploader.getName())
		self.state = "done"
	
		if self._failed_files:
			raise PlatformError("Cannot upload several files. %s" % [", ".join(self._failed_files)])
		
		self._logger.info("Upload complete!")

		# Return tuple of all files	def set_access_data(self, access_data):
		return tuple(self._result)

	def _worker(self, upload_dest):
		self._logger.debug("queue: %s", self._queue)
		try:
			while 1:
				filename, upload_attempts = self._queue.get(False)
				try:
					self._result.append(upload_dest.put(filename))
				except UploadError, e:
					self._logger.error("Cannot upload '%s'. %s", filename, e)
					if upload_attempts < self._max_attempts:
						self._logger.debug("File '%s' will be uploaded within the next attempt", filename)
						upload_attempts += 1
						self._queue.put((filename, upload_attempts))
					else:
						try:
							self._failed_files_lock.acquire()
							self._failed_files.append(filename)
						finally:
							self._failed_files_lock.release()
		except Empty:
			return
	

class UploadDest:
	def put(self, filename):
		pass
		
		
class S3UploadDest(UploadDest):
	
	def __init__(self, bucket, acl=None, logger=None):
		self.bucket = bucket
		self.acl = acl 
		self._logger = logger or logging.getLogger(__name__)
	
	def put(self, filename):
		self._logger.info("Uploading '%s' to S3 bucket '%s'", filename, self.bucket.name)
		
		try:
			key = Key(self.bucket)
			key.name = os.path.basename(self.filename)
			
			file = open(filename, "rb")
			key.set_contents_from_file(file, policy=self.acl)
			
		except (BotoServerError, OSError), e:
			raise UploadError, e
		finally:
			file.close()
		
		return os.path.join(self.bucket.name, key.name)


class CloudFilesUploadDest(UploadDest):
	
	def __init__(self, container_name, prefix, logger=None):
		self.container_name = container_name
		self.prefix = prefix
		self._logger = logger or logging.getLogger(__name__)
		
	def put(self, filename):
		self._logger.info('Uploading %s in CloudFiles container %s' % (file, self.container_name))
		base_name = os.path.basename(filename)
		obj_path = self.prefix + '/' + base_name
		try:		
			connection = cloudfiles.get_connection(username=os.environ["username"], api_key=os.environ["api_key"], serviceNet=True)
			
			try:
				container = connection.get_container(self.container_name)
			except cloudfiles.errors.NoSuchContainer:
				container = connection.create_container(self.container_name)
				
			o = container.create_object(obj_path)
			o.load_from_filename(filename)
			
		except (cloudfiles.errors.ResponseError, OSError, Exception), e:
			raise UploadError, e
		
		return os.path.join(self.container_name, obj_path)

class UploadError(BaseException):
	pass

def location_from_region(region):
	if region == 'us-east-1' or not region:
		return ''
	elif region == 'eu-west-1':
		return 'EU'
	else: 
		return region