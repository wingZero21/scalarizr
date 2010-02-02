'''
Created on 01.02.2010

@author: shaitanich
'''
import unittest
from scalarizr.core import Bus, BusEntries
from scalarizr.util import log
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
        bus = Bus()
        bus[BusEntries.MESSAGE_SERVICE] = MessageService()
        self._msg_service = Bus()[BusEntries.MESSAGE_SERVICE]
        testHandler = log.MessagingHandler(2, '2')
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(testHandler)
        
    def test_send_message(self):
        self.logger.info("ALLERT-1")
        self.logger.debug("ALLERT-2")
        self.logger.error("ALLERT-3")
        self.assertEqual(self._msg_service.message.id,2)
        time.sleep(3)
        self.assertEqual(self._msg_service.message.id,1)
        self.logger.critical("ALLERT-4")


    def tearDown(self):
        pass


    #def testName(self):
    #    pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()