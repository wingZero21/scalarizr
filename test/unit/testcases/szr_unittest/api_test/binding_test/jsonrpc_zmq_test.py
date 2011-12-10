'''
Created on Dec 5, 2011

@author: marat
'''
from scalarizr import rpc
from scalarizr.api.binding import jsonrpc_zmq
import gevent

import time
import unittest
import logging


LOG = logging.getLogger('scalarizr')
ENDPOINT = 'tcp://*:7771'

class MyService(object):
	
	@rpc.service_method
	def foo_longplay(self):
		gevent.sleep(2)
		return 42
	
	@rpc.service_method
	def foo(self):
		return 'letter'


class Test(unittest.TestCase):


	def setUp(self):
		self.client = jsonrpc_zmq.ZmqServiceProxy(ENDPOINT)
		self.server = jsonrpc_zmq.ZmqServer2(ENDPOINT, rpc.ServiceHandler(MyService()))
		gevent.spawn(self.server.serve_forever)
	
	def tearDown(self):
		self.server.shutdown()
	
	def _call_method(self, method):
		start = time.time()
		LOG.info('Calling %s', method)
		getattr(self.client, method)()
		LOG.info('Call %s completed. [time: %s]', method, time.time() - start)

	def test_req_sequence(self):
		glets = []
		for i in range(0, 5):
			for method in ['foo', 'foo_longplay']:
				glets.append(gevent.spawn(self._call_method, method))
				
		gevent.joinall(glets)

	
	

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_req_sequence']
	unittest.main()