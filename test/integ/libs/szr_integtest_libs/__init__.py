import time
import os
import signal
import re
import select
from threading import Thread, Event
import paramiko

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

class SshManager:
	
	transport = None
	connected = False
	channels  = []
	
	def __init__(self, host, key, timeout = 60):
		self.host = host
		self.key = key
		if not os.path.exists(self.key):
			raise Exception("Key file '%s' doesn't exist")
		self.timeout = timeout
		self.user = 'root'
		self.ssh = paramiko.SSHClient()
		self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		
	def connect(self):
		start_time = time.time()
		while time.time() - start_time < self.timeout:
			try:
				self.ssh.connect(self.host, key_filename = self.key, username=self.user)
				break
			except:
				continue
		else:
			raise Exception("Cannot connect to server %s" % self.host)
		
		transport = self.ssh.get_transport()
		channel = transport.open_session()
		channel.get_pty()
		channel.invoke_shell()
		time.sleep(1)
		
		if channel.closed:
			raise Exception ("Can't open new session")
		
		out = ''
		while channel.recv_ready():
			out += channel.recv(1)
			
		if 	'Please login as the ubuntu user rather than root user' in out:
			self.user = 'ubuntu'
			self.connect()
		else:
			self.connected = True
		channel.close()
		
		
	def get_root_ssh_channel(self):
		
		if not self.connected:
			self.connect()
			
		if not self.transport:
			self.transport = self.ssh.get_transport()
			self.transport.set_keepalive(60)
			
		channel = self.transport.open_session()
		channel.get_pty()
		channel.invoke_shell()
		self.channels.append(channel)
		if self.user == 'ubuntu':
			channel.send('sudo -i\n')
		
		return channel

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
	