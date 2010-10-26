'''
Created on Aug 25, 2010

@author: marat
'''

from scalarizr.bus import bus
from scalarizr.platform import PlatformError

from Queue import Queue, Empty
from threading import Thread, Lock
from boto.s3.key import Key
from boto.exception import BotoServerError
import os, logging


class S3Uploader(object):
	_queue = None
	state = None
	
	def __init__(self, pool=2, max_attempts=3, logger=None):
		self._logger = logger or logging.getLogger(__name__) 
		self._queue = Queue()
		self._pool = pool
		self._max_attempts = max_attempts
	
	def upload(self, files, bucket, s3_conn=None, acl=None, progress_cb=None):
		if not s3_conn:
			platform = bus.platform
			s3_conn  = platform.new_s3_conn()
		# Enqueue 
		for file in files:
			self._queue.put((file, 0)) # filename, attempt, last_error
			
		self._result = [] # list of tuples (filename, ok, last_error)
		
		self.state = "starting"
		
		self._uploaders = []
		self._failed_files = []
		self._failed_files_lock = Lock()
		
		#Starting threads
		for n in range(self._pool):
			uploader = Thread(name="Uploader-%s" % n, target=self._worker, 
					args=(s3_conn, bucket, acl))
			self._logger.debug("Starting uploader '%s'", uploader.getName())
			uploader.start()
			self._uploaders.append(uploader)

		self.state = "in-progress"
		# Join workers
		for uploader in self._uploaders:
			uploader.join()
			self._logger.debug("Uploader '%s' finished", uploader.getName())
		self.state = "done"
	
		if self._failed_files:
			raise PlatformError("Cannot upload several files. %s" % [", ".join(self._failed_files)])
		
		self._logger.info("Upload complete!")

		# Return tuple of all files	def set_access_data(self, access_data):
		return tuple([os.path.join(bucket.name, file) for file in self._result])

	def _worker(self, s3_conn, bucket, acl):
		self._logger.debug("queue: %s, bucket: %s", self._queue, bucket)
		try:
			while 1:
				filename, upload_attempts = self._queue.get(False)
				try:
					self._logger.info("Uploading '%s' to S3 bucket '%s'", filename, bucket.name)
					key = Key(bucket)
					key.name = os.path.basename(filename)
					file = open(filename, "rb")
					key.set_contents_from_file(file, policy=acl)
					self._result.append(key.name)
				except (BotoServerError, OSError), e:
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
				finally:
					file.close()
		except Empty:
			return

def location_from_region(region):
	if region == 'us-east-1' or not region:
		return ''
	elif region == 'eu-west-1':
		return 'EU'
	else: 
		return region