'''
JSON-RPC over 0MQ API. 

Used for internal communication between Scalarizrs and Scalr 
 
- Rich features
- Minimal overhead
- Extra methods for testing and benchmarking
'''

import logging

from scalarizr import rpc

import gevent
from gevent import event, pool, baseserver
from gevent_zeromq import zmq


LOG_CATEGORY = 'scalarizr.api.zmq'
LOG = logging.getLogger(LOG_CATEGORY)


class ZmqServiceProxy(rpc.ServiceProxy):
	
	def __init__(self, endpoint, crypto_key=None):
		self.context = zmq.Context()
		super(ZmqServiceProxy, self).__init__(endpoint)
	
	@property
	def conn(self):
		try:
			return self.local.conn
		except AttributeError:
			LOG.debug('Creating new REQ connection...')
			conn = zmq.Socket(self.context, zmq.REQ)
			conn.connect(self.endpoint)
			self.local.conn = conn 
			return self.conn
	
	def exchange(self, request):
		self.conn.send(request)
		return self.conn.recv()
	
class ZmqServer3(baseserver.BaseServer):
	
	backlog = 100
	
	def set_listener(self, listener, backlog=None):
		if isinstance(listener, zmq.Socket):
			socktype = listener.getsockopt(zmq.TYPE)
			if socktype != zmq.ROUTER:
				raise TypeError('Expected a ROUTER socket: %s' % (socktype, ))
			self.socket = listener
		else:
			if not isinstance(listener, basestring):
				raise TypeError('Expected a ROUTER socket or address: %s' % (listener, ))
			if backlog is not None:
				self.backlog = backlog
			self.address = listener
	
	def pre_start(self):
		if not hasattr(self, 'socket'):
			context = zmq.Context()
			self.socket = context.socket(zmq.ROUTER)
			self.socket.setsockopt(zmq.BACKLOG, self.backlog)
			self.socket.bind(self.address)
		super(ZmqServer3, self).pre_start()
	
	def start_accepting(self):
		self.loop = gevent.spawn_later(self._handle_loop)

	def stop_accepting(self):
		pass
	
	def _handle_loop(self):
		while True:
			try:
				pass
			except:
				pass
			
	def _handle_one(self, msg):
		pass
	
class ZmqServer2(rpc.Server):
	def __init__(self, endpoint, handler, pool_size=8):
		super(ZmqServer2, self).__init__(endpoint, handler)
		self.workers = pool.Pool(pool_size)
		self._shutdown_event = event.Event()
		self._shutdown_requests = False
		
	def serve_forever(self):
		self.context = zmq.Context(1)
		
		LOG.debug('Creating ROUTER on %s', self.endpoint)
		self.frontend = zmq.Socket(self.context, zmq.ROUTER)
		self.frontend.bind(self.endpoint)

		try:
			while not self._shutdown_requests:
				self.workers.spawn(self.handle_one_request, self.frontend.recv_multipart(), len(self.workers)-1)
		finally:
			self.workers.kill()
			self.frontend.close()
			self.context.term()
		self._shutdown_event.set()	
			
		
	def handle_one_request(self, message, index):
		LOG = logging.getLogger('%s.worker.%d' % (LOG_CATEGORY, index))
		LOG.debug('recv: %s', message)
		resp = self.handler.handle_request(message[2])
		self.frontend.send_multipart(message[0:2] + [resp])
		
	def shutdown(self):
		LOG.debug('Shutdowning %s ...', self.endpoint)
		self._shutdown_requests = True
		self._shutdown_event.wait()
		LOG.debug('Shutdowned %s', self.endpoint)

	
class ZmqServer(rpc.Server):
	
	def __init__(self, endpoint, handler, pool_size=8, backend_endpoint='inproc://jsonrpc_zmq_workers'):
		super(ZmqServer, self).__init__(endpoint, handler)
		self._shutdown_requests = False
		self.backend_endpoint = backend_endpoint
		self._shutdown_event = event.Event()
		self.workers = pool.Pool(pool_size)

	
	def serve_forever(self):
		self.context = zmq.Context(1)
		
		LOG.debug('Creating ROUTER on %s', self.endpoint)
		self.frontend = zmq.Socket(self.context, zmq.ROUTER)
		self.frontend.bind(self.endpoint)
		
		LOG.debug('Creating DEALER on %s', self.backend_endpoint)
		self.backend = zmq.Socket(self.context, zmq.DEALER)
		self.backend.bind(self.backend_endpoint)
		
		LOG.debug('Spawning %d workers', self.workers.size)
		for i in range(0, self.workers.size):
			self.workers.add(gevent.spawn(self.worker, i))
		try:
			LOG.debug('Connect ROUTER to DEALER (Infinity loop)')
			self._shutdown_event.clear()
			self._queue(self.frontend, self.backend)
		finally:
			self.workers.kill()
			self.frontend.close()
			self.backend.close()
			self.context.term()
		self._shutdown_event.set()

	def shutdown(self):
		LOG.debug('Shutdowning %s ...', self.endpoint)
		self._shutdown_requests = True
		self._shutdown_event.wait()
		LOG.debug('Shutdowned %s', self.endpoint)

	def _queue(self, insock, outsock, join_timeout=0.05):
		def handle_in():
			while True:
				msg = insock.recv_multipart()
				LOG.debug('recv: %s', msg)
				outsock.send_multipart(msg)
				
		def handle_out():
			while True:
				msg = outsock.recv_multipart()
				LOG.debug('send: %s', msg)
				insock.send_multipart(msg)
				
		glets = gevent.spawn(handle_out), gevent.spawn(handle_in)
		while not self._shutdown_requests:
			gevent.joinall(glets, join_timeout)

	
	def worker(self, index):
		LOG = logging.getLogger('%s.worker.%d' % (LOG_CATEGORY, index))
		sock = zmq.Socket(self.context, zmq.REP)
		sock.connect(self.backend_endpoint)
		try:
			LOG.debug('Starting...')
			while True:
				req = sock.recv()
				LOG.debug('recv: %s', req)
				resp = self.handler.handle_request(req)
				sock.send(resp)
		finally:
			sock.close()
			LOG.debug('Shutdowned')
			

