'''
Created on 22.01.2010

@author: shaitanich
'''
import atexit
import logging
import sqlite3
import threading
import traceback
import cStringIO
from scalarizr.bus import bus

try:
	import time
except ImportError:
	import timemodule as time

try:
	import cPickle as pickle
except:
	import pickle

class MessagingHandler(logging.Handler):
	def __init__(self, num_stored_messages = 1, send_interval = "1"):
		pool = bus.db
		self.conn = pool.get().get_connection()
		
		self.time_point = time.time()
		logging.Handler.__init__(self)
		self._msg_service = bus.messaging_service
		self.num_stored_messages = num_stored_messages
		
		if send_interval.endswith('s'):
			self.send_interval = int(send_interval[:-1])
		elif  send_interval.endswith('min'):
			self.send_interval = int(send_interval[:-3])*60
		elif send_interval.isdigit():
			self.send_interval = int(send_interval)
		else:
			self.send_interval = 1
		
		atexit.register(self.send_message)
		t = threading.Thread(target=self.timer_thread) 
		t.daemon = True
		t.start()

	def send_message(self):
		pool = bus.db
		connection = pool.get().get_connection()
		cur = connection.cursor()
		cur.execute("SELECT * FROM log")
		ids = []
		entries = []
		entry = {}
		
		for row in cur.fetchall():					
			entry['name'] = row['name']
			entry['level'] = row['level']
			entry['pathname'] = row['pathname']
			entry['lineno'] = row['lineno']
			entry['msg'] = row['msg']
			entry['stack_trace'] = row['stack_trace']	
			entries.append(entry)
			ids.append(str(row['id']))
		cur.close()
			
		if entries:
			message = self._msg_service.new_message("LogMessage")
			producer = self._msg_service.get_producer()
			message.body["entries"] = entries
			producer.send(message)
			connection.execute("DELETE FROM log WHERE id IN (%s)" % (",".join(ids)))
			connection.commit()
		self.time_point = time.time()

	def emit(self, record):
		msg = record.msg.__str__() % record.args if record.args else record.msg.__str__()
		stack_trace = None
		
		if record.exc_info:
			output = cStringIO.StringIO()
			traceback.print_tb(record.exc_info[2], file=output)
			stack_trace =  output.getvalue()
			output.close()			

		data = (None, record.name, record.levelname, record.pathname, record.lineno, msg, stack_trace)
		self.conn.execute('INSERT INTO log VALUES (?,?,?,?,?,?,?)', data)
		self.conn.commit()
		cur = self.conn.cursor()
		cur.execute("SELECT COUNT(*) FROM log")
		count = cur.fetchone()[0]
		cur.close()
		if count >= self.num_stored_messages:
			self.send_message()
		

	def timer_thread(self):
		while 1:
			while 1:
				time_delta = time.time() - self.time_point
				if  (time_delta > 1) and (time_delta > self.send_interval):
					break
				time.sleep(1)
			self.send_message()
			