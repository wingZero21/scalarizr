'''
Created on 22.01.2010

@author: Dmytro Korsakov
'''
import re
import atexit
import logging
import threading
import traceback
import cStringIO
from scalarizr.bus import bus

INTERVAL_RE = re.compile('((?P<minutes>\d+)min\s?)?((?P<seconds>\d+)s)?')

class MessagingHandler(logging.Handler):
	
	num_entries = None
	send_interval = None
	
	_sender_thread = None
	_send_event = None
	_stop_event = None
	
	def __init__(self, num_entries = 1, send_interval = "1s"):
		logging.Handler.__init__(self)		
		
		db = bus.db
		self._conn = db.get().get_connection()
		self._msg_service = bus.messaging_service

		self.num_entries = num_entries
		
		m = INTERVAL_RE.match(send_interval)
		self.send_interval = (int(m.group('seconds') or 0) + 60*int(m.group('minutes') or 0)) or 1
		
		self._send_event = threading.Event()
		self._stop_event = threading.Event()
		atexit.register(self._send_message)
		self._sender_thread = threading.Thread(target=self._sender)
		self._sender_thread.daemon = True
		
	def __del__(self):
		self._stop_event.set()

	def emit(self, record):
		if not self._sender_thread.isAlive():
			self._sender_thread.start()
		
		msg = str(record.msg) % record.args if record.args else str(record.msg)
		
		stack_trace = None
		if record.exc_info:
			output = cStringIO.StringIO()
			traceback.print_tb(record.exc_info[2], file=output)
			stack_trace =  output.getvalue()
			output.close()			

		data = (None, record.name, record.levelname, record.pathname, record.lineno, msg, stack_trace)
		self._conn.execute('INSERT INTO log VALUES (?,?,?,?,?,?,?)', data)
		self._conn.commit()
		
		cur = self._conn.cursor()
		cur.execute("SELECT COUNT(*) FROM log")
		count = cur.fetchone()[0]
		cur.close()
		if count >= self.num_entries:
			self._send_event.set()
			
	def _send_message(self):
		db = bus.db
		conn = db.get().get_connection()
		cur = conn.cursor()
		cur.execute("SELECT * FROM log")
		ids = []
		entries = []
		
		for row in cur.fetchall():
			entry = {}
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
			conn.execute("DELETE FROM log WHERE id IN (%s)" % (",".join(ids)))
			conn.commit()

	def _sender(self):
		while not self._stop_event.isSet():
			try:
				self._send_event.wait(self.send_interval)
				if not self._stop_event.isSet():
					self._send_message()
			finally:
				self._send_event.clear()

			