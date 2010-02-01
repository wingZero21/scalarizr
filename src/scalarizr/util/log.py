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

class MessagingHandler(logging.Handler):
    def __init__(self, num_stored_messages = 1, send_interval = "1"):
        logging.Handler.__init__(self)
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

    def send_messages(self):
        lock = threading.Lock()
        lock.acquire() # will block if lock is already held
        if [] != self.messages:
            #Insert useful function there:
            for l in self.messages:
                print "MessagingHandler:" , l
            self.messages = []
            self.time_point = time.time()
        lock.release()

    def emit(self, record):
        self.messages.append(record)
        if len(self.messages) >= self.num_stored_messages:
            self.send_messages()

    def timer_thread(self):
        while 1:
            time_delta = time.time() - self.time_point
            while (time_delta < self.send_interval) or (time_delta <= 1):
                time.sleep(1)
            self.send_messages()

if __name__ == "__main__":

    logger = logging.getLogger()
    testHandler = MessagingHandler(2,"2")
    #testHandler.setLevel(logging.CRITICAL)
    logger.addHandler(testHandler)
    # And finally a test
    logger.error('Test 1')
    time.sleep(1)
    logger.error('Test 2')
    logger.warning('Test 3')
    time.sleep(3)
    logger.error('Test 4')
    logger.critical('Test 5')