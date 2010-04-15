'''
Created on 01.02.2010

@author: shaitanich
'''
import unittest
from scalarizr.bus import bus
from scalarizr.util import log, init_tests
import logging
import logging.handlers
import time

class Message(object):
	id = None    
	name = None
	meta = {}    
	body = {}
	
class MessageProducer(object):
	
	message = None
	
	def send(self, message):
		self.message = message
		self.message.id = len(self.message.body["entries"])
		print self.message.id, "sent: ", self.message.body["entries"] 
		
class MessageService(object):
	
	message = None
	
	def new_message(self, name=None):
		self.message = Message()
		self.message.name = name
		return self.message
	
	def get_producer(self):
		return MessageProducer()
	
class Test(unittest.TestCase):
	
	_msg_service = None
	
	def setUp(self):
		bus.messaging_service = MessageService()
		self._msg_service = bus.messaging_service
		
		testHandler = log.MessagingHandler(2, '2')
		self.logger = logging.getLogger()
		self.logger.setLevel(logging.DEBUG)
		self.logger.addHandler(testHandler)
		
	def open_file0(self, path):
		return open(path, "r")
	
	def open_file(self, path):
		return self.open_file0(path)
	
	def test_send_message(self):
		self.logger.info("ALLERT-1")
		self.logger.debug("ALLERT-2")
		self.logger.error("ALLERT-3")
		try:
			self.open_file("/non/existed/path")
		except IOError, e:
			self.logger.exception(e)

		self.assertEqual(self._msg_service.message.id,2)
		time.sleep(3)
		self.assertEqual(self._msg_service.message.id,2)
		self.logger.critical("ALLERT-4")

	def tearDown(self):
		pass

if __name__ == "__main__":
	init_tests()
	unittest.main()