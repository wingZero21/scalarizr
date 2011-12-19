'''
JSON-RPC over 0MQ API. 

Used for internal communication between Scalarizrs and Scalr 
 
- Rich features
- Minimal overhead
- Extra methods for testing and benchmarking
'''

import logging
import sys
import traceback

from scalarizr import rpc

import gevent
from gevent import baseserver
from gevent_zeromq import zmq


LOG_CATEGORY = 'scalarizr.api.zmq'
LOG = logging.getLogger(LOG_CATEGORY)


class ZmqServiceProxy(rpc.ServiceProxy):
	
	def __init__(self, endpoint, crypto_key=None):
		self.endpoint = endpoint		
		self.context = zmq.Context()
		super(ZmqServiceProxy, self).__init__()
	
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
		namespace = '/'.join(self.local.method[0:-1])
		self.conn.send_multipart([namespace, request])
		return self.conn.recv()
	
	
class ZmqServer(baseserver.BaseServer, rpc.Server):
	
	backlog = 100
	loop = None
	
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
		super(ZmqServer, self).pre_start()
	
	def start_accepting(self):
		self.loop = gevent.spawn_later(0, self._handle_loop)

	def stop_accepting(self):
		if self.loop:
			self.loop.kill()
	
	def _handle_loop(self):
		while True:
			#LOG.debug('blocking until message')
			req = self.socket.recv_multipart()
			self._spawn(self._handle_one, req)
			
	def _handle_one(self, req):
		LOG = logging.getLogger('%s.%s' % (LOG_CATEGORY, id(gevent.getcurrent())))
		LOG.debug('recv: %s', req[2:])
		try:
			namespace = req[2] or None
			resp = self.handle.handle_request(req[3], namespace)
			LOG.debug('send: %s', resp)
			self.socket.send_multipart(req[0:2] + [resp])
		except:
			ex = sys.exc_info()[1]
			if not isinstance(ex, gevent.GreenletExit):
				self.handle_error()

	def handle_error(self):
		traceback.print_exc()
	
	
'''
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
'''
			

