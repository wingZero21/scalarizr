'''
Created on Aug 25, 2010

@author: marat
@author: Dmytro Korsakov
'''

import logging
from Queue import Queue, Empty
from threading import Thread, Lock

class UploadError(BaseException):
	pass

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
			raise UploadError("Cannot upload several files. %s" % [", ".join(self._failed_files)])
		
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
		

class Downloader:
	pass

class DownloadSource:
	def get(self, filename, dest):
		pass