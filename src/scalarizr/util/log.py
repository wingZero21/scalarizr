'''
Created on 22.01.2010

@author: shaitanich
'''

import threading
import time
import sys
import atexit
import logging
import logging.handlers
from scalarizr.core import Bus, BusEntries

class MessagingHandler(logging.Handler):
    def __init__(self, num_stored_messages = 1, send_interval = "1"):
        logging.Handler.__init__(self)
        self._msg_service = Bus()[BusEntries.MESSAGE_SERVICE]
        self.messages = []
        self.num_stored_messages = num_stored_messages

        if send_interval.endswith('s'):
            self.send_interval = int(send_interval[:-1])
        elif  send_interval.endswith('min'):
            self.send_interval = int(send_interval[:-3])*60
        elif send_interval.isdigit():
            self.send_interval = int(send_interval)
        else:
            self.send_interval = 1

        self.time_point = time.time()
        
        atexit.register(self.send_messages)
        
        t = threading.Thread(target=self.timer_thread)
        #Code refactoring needed : 
        #after calling exit function from main program timer-thread throws exception  
        t.daemon = True
        t.start()

    def send_message(self):
        lock = threading.Lock()
        lock.acquire() # will block if lock is already held
        
        if [] != self.messages:
            message = self._msg_service.new_message("LogMessage")
            producer = self._msg_service.get_producer()
            
            entries = []
            for m in self.messages:
                entries.append([m.pathname, m.level, m.msg])
            
            message.body["entries"] = entries
            producer.send(message)
            
            self.messages = []
            self.time_point = time.time()
        lock.release()

    def emit(self, record):
        self.messages.append(record)
        if len(self.messages) >= self.num_stored_messages:
            self.send_message()

    def timer_thread(self):
        while 1:
            while (time.time() - self.time_point < self.send_interval) or (time.time() - self.time_point <= 1):
                time.sleep(1)
            self.send_message()