'''
Created on Aug 25, 2010

@author: marat
@author: Dmytro Korsakov
'''

import logging
from Queue import Queue, Empty
from threading import Thread, Lock

class TransferError(BaseException):
	pass

class Transfer(object):
	_queue = None
	state = None
	
	def __init__(self, pool=2, max_attempts=3, logger=None):
		self._logger = logger or logging.getLogger(__name__) 
		self._queue = Queue()
		self._pool = pool
		self._max_attempts = max_attempts

	def upload(self, files, UploadDest, progress_cb=None):
		action = UploadDest.run('put')	
		self._transfer(files, UploadDest, action)
		
	def download(self, place, DownloadSrc):
		files = DownloadSrc.get_list_files()

		action = DownloadSrc.run('get', place)
		self._transfer(files, DownloadSrc, action)	
	
	def _transfer(self, files, UploadDest, action, progress_cb=None):
		# Enqueue 
		for file in files:
			self._queue.put((file, 0)) 
			
		self.state = "starting"
		self._workers = []
		self._failed_files = []
		self._result = []
		self._failed_files_lock = Lock()
		
		#Starting threads
		for n in range(self._pool):
			worker = Thread(name="Worker-%s" % n, target=self._worker, 
					args=(UploadDest, action))
			self._logger.debug("Starting worker '%s'", worker.getName())
			worker.start()
			self._workers.append(worker)
		
		# Join workers
		self.state = "in-progress"
		for worker in self._workers:
			worker.join()
			self._logger.debug("Worker '%s' finished", worker.getName())
		self.state = "done"
	
		if self._failed_files:
			raise TransferError("Cannot process several files. %s" % [", ".join(self._failed_files)])
		
		self._logger.info("Transfer complete!")

		# Return tuple of all files	def set_access_data(self, access_data):
		return tuple(self._result)

	def _worker(self, upload_dest, action):
		self._logger.debug("queue: %s", self._queue)
		try:
			while 1:
				filename, attempts = self._queue.get(False)
				try:
					result = action(filename)
					self._result.append(result)
				except TransferError, e:
					self._logger.error("Cannot transfer '%s'. %s", filename, e)
					if attempts < self._max_attempts:
						self._logger.debug("File '%s' will be transfered within the next attempt", filename)
						attempts += 1
						self._queue.put((filename, attempts))
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
	
	def get(self, filename, dest):
		pass
	
	def get_list_files(self):
		pass
	
	def run(self, action, dest=None):
		def _action(filename=None):
			if action == 'put':
				return self.put(filename)
			if action == 'get':
				return self.get(filename, dest)
		return _action
	