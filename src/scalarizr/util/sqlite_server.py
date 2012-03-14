'''
Created on Mar 14, 2012

@author: marat
'''

import Queue


class Proxy(object):
	def __init__(self, tasks_queue):
		'''
		Ingoing tasks queue. Item is a tuple(method, client_hash, args, kwds)
		Outgoing results queue.  
		'''
		self.tasks_queue = tasks_queue
		self.results = Queue.Queue()
		
	def _call(self, method, args=None, kwds=None):
		self.tasks_queue.put((method, self.__hash__(), args, kwds))
		return self.results.get()

class ConnectionProxy(Proxy):
		
	def cursor(self):
		return CursorProxy(self, self.tasks_queue)


class CursorProxy(Proxy):

	def __init__(self, *args, **kwds):
		super(CursorProxy, self).__init__(*args, **kwds)
		self._call('cursor_create')
		
	def execute(self, sql, *parameters):
		return self._call('cursor_execute', [sql] + parameters)
	
	def __del__(self):
		self._call('cursor_delete')
		


class SqliteServer(object):
	def __init__(self, conn_creator):
		self.master_conn = conn_creator()
		self.master_conn.isolation_level = None
		self.single_conn_proxy = None
		self.clients = {}
		self.cursors = {}

	def connect(self):
		if not self.single_conn_proxy:
			self.single_conn_proxy = ConnectionProxy()
			self.clients[self.single_conn_proxy.__hash__()] = self.single_conn_proxy
		return self.single_conn_proxy 
	
	def serve_forever(self):
		while True:
			job = self.conn_proxy.get()
			method, hash, args, kwds = '__%s' % job[0], job[1], job[2] or [], job[3] or {}
			result = getattr(self, method)(hash, *args, **kwds)
			if hash in self.clients:
				self.clients[hash].results.put(result)
	
	def __cursor_create(self, hash):
		cur = self.master_conn.cursor()
		self.cursors[hash] = cur
		
	def __cursor_delete(self, hash):
		del self.cursors[hash]
		del self.clients[hash]
		
	def __cursor_execute(self, hash, *args, **kwds):
		return self.cursors[hash].execute(*args, **kwds)
	