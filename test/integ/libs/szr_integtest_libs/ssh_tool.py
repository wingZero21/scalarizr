'''
Created on Dec 30, 2010

@author: spike
'''

from threading import Thread, Lock, Event
from Queue import Queue, Empty

import logging
import os
import paramiko
import re
import socket
import time

regexps = ['root@.*#',
		   'local2:.*#',
		   '\-bash\-.*#']

root_re = '|'.join(regexps)

class SshManager:
	
	transport = None
	connected = False
	channels  = []
	key = None
	password = None

	def __init__(self, host, key=None, key_pass = None, password=None, timeout = 90):
		self.host = host
		self.ip   = socket.gethostbyname(host)
		
		if key:
			key_file = os.path.expanduser(key)
			if not os.path.exists(key_file):
				raise Exception("Key file '%s' doesn't exist", key_file)
			self.key = paramiko.RSAKey.from_private_key_file(key_file, password = key_pass if key_pass else None)
		elif password:
			self.password = password
		else:
			raise Exception("Not enough data to authenticate.")
		
		self.timeout = timeout
		self.user = 'root'
		self.ssh = paramiko.SSHClient()
		self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		
	def _check_connection(self):
		if not self.connected or not self.ssh._transport.is_alive():
			self.connect()
		
	def connect(self):
		start_time = time.time()
		while time.time() - start_time < self.timeout:
			try:
				self.ssh.connect(self.host, pkey = self.key, username=self.user, password=self.password)
				break
			except (Exception, BaseException), e:
				print e
				continue
		else:
			raise Exception("Cannot connect to server %s" % self.host)
		
		transport = self.ssh.get_transport()
		transport.banner_timeout = 60
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
		self._check_connection()
			
		if not self.transport:
			self.transport = self.ssh.get_transport()
			self.transport.set_keepalive(60)
			
		channel = self.ssh.invoke_shell()
		channel.resize_pty(500, 500)
		self.channels.append(channel)
		if self.user == 'ubuntu':
			channel.send('sudo -i\n')
			
		clean_output(channel, 5)
		return channel
	
	def get_sftp_client(self):
		self._check_connection()
		return self.ssh.open_sftp()
	
	def close_all_channels(self):
		for channel in self.channels:
			channel.close()
		self.channels = []
		
		
		
def execute(channel, cmd, timeout = 60):
	
	while channel.recv_ready():
		channel.recv(1)
	bytes_amount = channel.send(cmd)
	time.sleep(0.3)
	channel.recv(bytes_amount)
	channel.send('\n')
	out = clean_output(channel, timeout)
	lines = out.splitlines()
	if len(lines) > 2:
		return '\n'.join(lines[1:-1]).strip()
	else:
		return ''
	
	
	
def clean_output(channel, timeout = 60):
	out = ''
	
	#not the best solution
	#if not channel.recv_ready():
	#	return out
	
	last_recv_time = time.time()
	while True:
		if channel.recv_ready():
			last_recv_time = time.time()
			out += channel.recv(1024)
			if re.search(root_re, out):
				break
		else:
			if time.time() - last_recv_time > timeout:
				#raise Exception('Timeout (%s sec) while waiting for root prompt. Out:\n%s' % (timeout, out))	
				raise Exception('Timeout (%s sec) while waiting for root prompt. ' % timeout)	
	return out




class LogReader:
	
	_exception = None
	_error     = ''
	
	def __init__(self, queue):
		self.err_re = re.compile('^\d+-\d+-\d+\s+\d+:\d+:\d+,\d+\s+-\s+ERROR\s+-\s+.+$', re.M)
		self.traceback_re = re.compile('Traceback.+')
		self.queue = queue 

	def expect(self, regexp, timeframe=30):
		self._error = ''
		self.ret = None
		break_tail = Event()

		t = Thread(target =self.reader_thread, args=(regexp, break_tail))
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
			raise Exception('Timeout after %s.' % timeframe)				

	def reader_thread(self, regexp, break_tail):
		search_re = re.compile(regexp) if type(regexp) == str else regexp
		while not break_tail.is_set():
			
			try:
				log_line = self.queue.get(False)
			except Empty:
				continue
			
			
			error = re.search(self.err_re, log_line)
			if error:
				# Detect if traceback presents
				try:
					traceback = self.queue.get(False)					
				
					if re.search(self.traceback_re, traceback):
						while True:
							newline = self.queue.get(False)
							if not newline.startswith(' '):
									break
							traceback += '\n' + newline
						self._error = error.group(0) + traceback
					else:
						self._error = error.group(0)
				except Empty:
					self._error = error.group(0)
				break_tail.set()
				break
			
			
			matched = re.search(search_re, log_line)
			if matched:
				self.ret = matched
				break_tail.set()
				break




class MutableLogFile:
	_logger = None
	_queues = None
	_channel = None
	_log_file = None
	_whole_log = None
	_lock = None	
	_reader = None
	_reader_started = False
	
	def __init__(self, channel, log_file='/var/log/scalarizr.log'):
		if channel.closed:
			raise Exception('Channel is closed')
		self._channel = channel
		self._log_file = log_file
		self._queues = []
		self._lock = Lock()
		self._whole_log = []
		self._reader = Thread(target=self._read, name='Log file %s reader' % self._log_file)
		self._reader.setDaemon(True)
		self._logger = logging.getLogger(__name__ + '.MutableLogFile')
	
	def _start_reader(self):
		self._reader.start()
		self._reader_started = True	
	
	def _read(self):
		# Clean channel
		self._logger.debug('Clean channel')
		while self._channel.recv_ready():
			self._channel.recv(1)
			
		# Wait when log file will be available
		self._logger.debug('Wait when log file will be available')
		while True:
			if execute(self._channel, 'ls -la %s 2>/dev/null' % self._log_file):
				break
			
		# Open log file
		self._logger.debug('Open log file')
		cmd = 'tail -f -n +0 %s\n' % self._log_file
		self._channel.send(cmd)
		time.sleep(0.3)
		self._channel.recv(len(cmd))
		
		# Read log file
		self._logger.debug('Entering read log file loop')
		line = ''
		while True:
			# Receive line
			#self._logger.debug('Receiving line')
			while self._channel.recv_ready():
				char = self._channel.recv(1)
				line += char
				if char == '\n':
					self._logger.debug('Received: %s', line)
					self._lock.acquire()			
					self._whole_log.append(line[:-1])
					self._lock.release()
					for queue in self._queues:
						queue.put(line[:-1])
					line = ''

					break

	
	def head(self, skip_lines=0):
		if not self._reader_started:
			self._start_reader()

		queue = Queue()
		if len(self._whole_log) < skip_lines:
			raise Exception("Cannot skip %s lines after log's head: there is no such amount of lines in log" % skip_lines)
		
		for line in self._whole_log[skip_lines:]:
			queue.put(line)
					
		self._queues.append(queue)

		return LogReader(queue)
	
	def tail(self, lines=0):
		if not self._reader_started:
			self._start_reader()
		
		if len(self._whole_log) < lines:
			raise Exception("Cannot get %s lines before log's tail: there is no such amount of lines in log" % lines)
		
		queue = Queue()	
		for line in self._whole_log[-lines:]:
			queue.put(line)
			
		self._queues.append(queue)
		return LogReader(queue)
	
	def detach(self, queue):
		if queue in self._queues:
			self._queues.remove(queue)


