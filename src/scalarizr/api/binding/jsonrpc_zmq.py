'''
JSON-RPC over 0MQ API. 

Used for internal communication between Scalarizrs and Scalr 
 
- Rich features
- Minimal overhead
- Extra methods for testing and benchmarking
'''

from scalarizr import rpc
from gevent_zeromq import zmq

class ZmqServiceProxy(rpc.ServiceProxy):
	
	def __init__(self, endpoint, crypto_key=None):
		self.context = zmq.Context()
		super(ZmqServiceProxy, self).__init__(endpoint)
	
	@property
	def conn(self):
		try:
			return self.local.conn
		except AttributeError:
			conn = zmq.Socket(self.context, zmq.REQ)
			conn.connect()
			self.local.conn = conn 
			return self.conn
	
	def exchange(self, request):
		self.conn.send(request)
		return self.conn.recv(request)
	
	
class ZmqServer(rpc.Server):

	def serve_forever(self):
		self.context = zmq.Context()		
		self.sock = zmq.Socket(self.context, zmq.REP)
		self.sock.bind(self.endpoint)
		
		while True:
			req = self.sock.recv()
			resp = self.handler.handle_request(req)
			self.sock.send(resp)
