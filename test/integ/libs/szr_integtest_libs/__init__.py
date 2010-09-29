import time
import os
import signal
import re
import select
from threading import Thread, Event

class SshPool:
	def __new__(self):
		pass
	
	def add(self, name, ssh):
		pass
	
	def destroy(self, name):
		pass
	
	def destroy_pool(self):
		pass
	
	def idle_thread(self):
		'''
		NOOP ssh sessions
		'''
		pass


class LogReader:
	
	_exception = None
	_error     = ''
	
	def __init__(self):
		self.err_re = re.compile('^\d+-\d+-\d+\s+\d+:\d+:\d+,\d+\s+-\s+ERROR\s+-\s+.*?^(?P<traceback>Traceback.*?$(\n[^\d].*?$)*)?', re.M | re.S)

	def expect(self, regexp, timeframe, channel):
		self._error = ''
		self.ret = None
		break_tail = Event()

		t = Thread(target =self.reader_thread, args=(channel, regexp, break_tail))
		t.start()
		
		start_time = time.time()
		
		while time.time() - start_time < timeframe:
			time.sleep(0.1)
			if break_tail.is_set():
				if self._error:
					raise Exception('Error detected: %s' % self._error)
				if self.ret:
					return self.ret
				else:
					raise Exception('Something bad happened')
		else:
			break_tail.set()
			raise Exception('Timeout after %s' % timeframe)				
	
	def reader_thread(self, channel, regexp, break_tail):
		while not break_tail.is_set():
			rl = select.select([channel],[],[],0.0)[0]
			if len(rl) > 0:
				
				line = channel.recv(1024)
				if not line:
					self._error = 'Channel has been closed'
					break_tail.set()
					
				if re.search(self.err_re, line):
					self._error = re.search(self.err_re, line).group(0)
					break_tail.set()
					break
					
				if re.search(regexp, line):
					self.ret = re.search(regexp, line)
					break_tail.set()
					break
	
	
def expect(channel, regexp, timeframe):
	reader = LogReader()
	return reader.expect(regexp, timeframe, channel)
	