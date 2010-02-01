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
    def send(self, message):
        print len(message.body["entries"])
        return len(message.body["entries"])

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
        #message = self._msg_service.new_message("LogMessage")
        #producer = self._msg_service.get_producer()
        #print producer
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        testHandler = log.MessagingHandler(2, '2')
        logger.addHandler(testHandler)
        #logger.addHandler(mh)
        #mh.setLevel(logging.DEBUG)
        logger.info("ololo")
        time.sleep(3)
        logger.debug("ololo2")


    def tearDown(self):
        pass


    def testName(self):
        pass


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()