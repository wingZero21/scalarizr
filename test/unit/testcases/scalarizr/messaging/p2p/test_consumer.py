'''
Created on Apr 30, 2010

@author: marat
'''
import unittest
import threading
from scalarizr.bus import bus
from scalarizr.util import init_tests, configtool
from scalarizr.messaging.p2p import P2pMessageService
try:
	import time
except ImportError:
	import timemodule as time

class Test(unittest.TestCase):

	_msg_url = "http://localhost:8765"
	
	_wait = threading.Event()
	_srv = None
	_consumer = None
	_producer = None

	def setUp(self):
		gen_sect = configtool.section_wrapper(bus.config, configtool.SECT_GENERAL)
		
		self._srv = P2pMessageService(
			server_id=gen_sect.get(configtool.OPT_SERVER_ID),
			crypto_key_path=gen_sect.get(configtool.OPT_CRYPTO_KEY_PATH),
			consumer_url=self._msg_url, 
			producer_url=self._msg_url
		) 
		self._producer = self._srv.get_producer()
		self._consumer = self._srv.get_consumer()
		t = threading.Thread(target=self._start_consumer)
		t.start()
		time.sleep(1)
	
	def tearDown(self):
		self._consumer.stop()

	def _start_consumer(self):
		self._consumer.start()
		
	def test_unfail(self):
		ln = MessageListener()
		self._consumer.listeners.append(ln)
		
		msg = self._srv.new_message("raise")
		self._producer.send("q1", msg)
		self._wait.wait(1)
		self._wait.clear()
		self.assertTrue(ln.raised)

		msg = self._srv.new_message("raise_base")
		self._producer.send("q1", msg)
		self._wait.wait(1)
		self._wait.clear()
		self.assertTrue(ln.raised_base)
		
		msg = self._srv.new_message("handle")
		self._producer.send("q1", msg)
		self._wait.wait(1)
		self._wait.clear()
		self.assertTrue(ln.handled)

class MessageListener():
	raised = False
	raised_base = False
	handled = False
	
	def __call__(self, message, queue):
		if message.name == "raise":
			self.raised = True
			raise Exception("Raise Exception from message listener")
		elif message.name == "raise_base":
			self.raised_base = True
			raise BaseException("Raise BaseException from message listener")
		elif message.name == "handle":
			self.handled = True

if __name__ == "__main__":
	init_tests()
	unittest.main()