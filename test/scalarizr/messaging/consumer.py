'''
Created on Dec 7, 2009

@author: marat
'''
import unittest
import timemodule
from threading import Thread
from scalarizr.messaging.p2p import P2pMessageService, P2pMessage

class TestConsumer(unittest.TestCase):

	_consumer = None
	_producer = None
	_consumer_started = False

	def setUp(self):
		config = {"endpoint" : "http://localhost:8013"}
		service = P2pMessageService(config)
		self._consumer = service.new_consumer()
		self._producer = service.new_producer()
		
		t = Thread(target=self._start_consumer)
		t.start()
		timemodule.sleep(1)

	def _start_consumer(self):
		self._consumer.start()
	
	def tearDown(self):
		self._consumer.stop()

	def testAll(self):
		message = P2pMessage("Melody", {"a" : "b"}, {"cx" : "dd"})
		self._producer.send("test", message)


if __name__ == "__main__":
	import scalarizr.core
	unittest.main()