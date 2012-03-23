'''
Created on Mar 14, 2012

@author: marat
'''

import Queue
import threading
import weakref
from weakref import WeakValueDictionary

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
		print 'Into queue:', method, args, kwds, '[%s]'% self.hash
		self.tasks_queue.put((method, self.__hash__(), args, kwds))
		if wait:
			self.result_available.wait()
		print 'Got result from server: ', self.result
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

	
	def __del__(self):
		self._wait = False
		self._call('cursor_delete', wait=False)
		

class ConnectionProxy(Proxy):
		
	def cursor(self):
		cp = CursorProxy(self.tasks_queue)
		print 'created new cursor proxy:', cp
		return cp


	def executescript(self, sql):
		return self._call('executescript', sql)
	
	
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
			print 'Looking for new message in server queue'
			job = self.single_conn_proxy.tasks_queue.get()
			method, hash, args, kwds = '_%s' % job[0], job[1], job[2] or [], job[3] or {}
			print 'Got new job from queue:', method, hash, args, kwds
			result = getattr(self, method)(hash, *args, **kwds)
			if hash in self.clients:
				self.clients[hash].result = result
				self.clients[hash].result_available.set()
				print 'result:', result
	
	
	def _cursor_create(self, hash, proxy):
		print 'hash=%s, proxy=%s' % (hash, proxy)
		self.cursors[hash] = self.master_conn.cursor()
		self.clients[hash] = proxy
		print 'created cursor for ', hash
		return self.cursors[hash]
		
		
	def _cursor_delete(self, hash):
		result = self.cursors[hash].close()
		del self.cursors[hash]
		print 'deleted cursor for ', hash
		return result
		
		
	def _cursor_execute(self, hash, *args, **kwds):
		return self.cursors[hash].execute(*args, **kwds)
	
	
	def _cursor_fetchone(self, hash):
		return self.cursors[hash].fetchone()
		
		
	def _cursor_fetchall(self, hash):
		return self.cursors[hash].fetchall()
			
			
	def _executescript(self, hash, sql):
		return self.connect.executescript(sql)
	
