'''
Created on Mar 14, 2012

@author: marat
'''

import time
import Queue
import threading
import weakref
import logging
from weakref import WeakValueDictionary

from scalarizr.util import wait_until
import sys

LOG = logging.getLogger(__name__)

class Proxy(object):
	
	
	def __init__(self, tasks_queue):
		'''
		Ingoing tasks queue. Item is a tuple(method, client_hash, args, kwds)
		Outgoing result.  
		'''
		self.tasks_queue = tasks_queue
		self.result = None
		self.hash = self.__hash__()
		self.result_available = threading.Event()
		
		
	def _call(self, method, wait, args=None, kwds=None):
		self.result_available.clear()
		self.tasks_queue.put((method, self.__hash__(), args, kwds))
		if wait:
			self.result_available.wait()
		return self.result
	

class CursorProxy(Proxy):

	def __init__(self, tasks_queue):
		super(CursorProxy, self).__init__(tasks_queue)
		self._cursor = self._call('cursor_create', True, [self])
		
		
	def execute(self, sql, *parameters):
		return self._call('cursor_execute', True, [sql] + list(parameters))
	
	
	def fetchone(self):
		return self._call('cursor_fetchone', wait=True)


	def fetchall(self):
		return self._call('cursor_fetchall', wait=True)
	
	@property
	def rowcount(self):
		return self._call('cursor_rowcount', wait=True)

	
	def __del__(self):
		self._call('cursor_delete', wait=False)
		
	close = __del__
		

class ConnectionProxy(Proxy):
		
	def cursor(self):
		cp = CursorProxy(self.tasks_queue)
		return cp


	def executescript(self, sql):
		return self._call('executescript', True, [sql])
	
	
	def commit(self):
		#no worries, autocommit is set
		pass
	
	
	def _get_row_factory(self):
		return self._call('get_row_factory', True)
	
	
	def _set_row_factory(self,f):
		return self._call('set_row_factory', True, [f])
	
	
	def _get_text_factory(self):
		return self._call('get_text_factory', True)
	
	
	def _set_text_factory(self,f):
		return self._call('set_text_factory', True, [f])
	
	text_factory = property(_get_text_factory, _set_text_factory)
	row_factory = property(_get_row_factory, _set_row_factory)
	
		
class SqliteServer(object):
	
	def __init__(self, conn_creator):
		self.master_conn = conn_creator()
		self.master_conn.isolation_level = None
		self.single_conn_proxy = None
		self.clients = WeakValueDictionary()
		self.cursors = {}


	def connect(self):
		if not self.single_conn_proxy:
			self.single_conn_proxy = ConnectionProxy(Queue.Queue())
			self.clients[self.single_conn_proxy.__hash__()] = self.single_conn_proxy
		return self.single_conn_proxy 
	
	
	def serve_forever(self):
		while True:
			job = self.single_conn_proxy.tasks_queue.get()
			method, hash, args, kwds = '_%s' % job[0], job[1], job[2] or [], job[3] or {}
			result = getattr(self, method)(hash, *args, **kwds)
			try:
				if hash in self.clients:
					self.clients[hash].result = result
					self.clients[hash].result_available.set()
			except:
				LOG.exception('Caught exception in SQLite server loop')
	
	
	def _cursor_create(self, hash, proxy):
		self.cursors[hash] = self.master_conn.cursor()
		self.clients[hash] = proxy
		return self.cursors[hash]
		
		
	def _cursor_delete(self, hash):
		result = None
		if hash in self.cursors:
			result = self.cursors[hash].close()
			del self.cursors[hash]
		return result
		
		
	def _cursor_execute(self, hash, *args, **kwds):
		result = None
		if hash in self.cursors:
			result  = self.cursors[hash].execute(*args, **kwds)
		return result 
	
	
	def _cursor_fetchone(self, hash):
		result = None
		if hash in self.cursors:
			result = self.cursors[hash].fetchone()
		return result 
		
		
	def _cursor_fetchall(self, hash):
		result = None
		if hash in self.cursors:
			result = self.cursors[hash].fetchall()
		return result 
	
	
	def _cursor_rowcount(self, hash):
		result = None
		if hash in self.cursors:
			result = self.cursors[hash].rowcount
		return result 

	
	def _set_row_factory(self, hash, f):
		self.master_conn.row_factory = f	
	
	
	def _set_text_factory(self, hash, f):
		self.master_conn.text_factory = f	
		
		
	def _get_row_factory(self, hash):
		return self.master_conn.row_factory	
	
	
	def _get_text_factory(self, hash):
		return self.master_conn.text_factory
		
		
	def _executescript(self, hash, sql):
		LOG.debug('_executescript')
		try:
			return self.master_conn.executescript(sql)
		except:
			LOG.debug('caught', exc_info=sys.exc_info())
			raise
	
	
	
	
class SQLiteServerThread(threading.Thread):
	
	ready = None
	connection = None
	conn_creator = None
	
	def __init__(self, conn_creator):
		self.ready = False
		self.conn_creator = conn_creator
		threading.Thread.__init__(self)
		
	def run(self):
		server = SqliteServer(self.conn_creator)
		self.connection = server.connect()
		self.ready = True
		server.serve_forever()
		

def wait_for_server_thread(t):
	wait_until(lambda: t.ready == True, sleep = 0.1)
	
