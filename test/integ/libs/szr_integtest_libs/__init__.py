import time
import os
import signal
import re
import select
import paramiko
import socket
import pexpect
import logging
import tempfile
from Queue import Queue, Empty

"""
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
"""
regexps = ['root@.*#',
		   'local2:.*#',
		   '\-bash\-.*#']

root_re = '|'.join(regexps)

class SshManager:
	
	transport = None
	connected = False
	channels  = []

	def __init__(self, host, key, timeout = 90, key_pass = None):
		self.host = host
		self.ip   = socket.gethostbyname(host)
		key_file = os.path.expanduser(key)
		if not os.path.exists(key_file):
			raise Exception("Key file '%s' doesn't exist", key_file)
		self.key = paramiko.RSAKey.from_private_key_file(key_file, password = key_pass if key_pass else None)

		self.timeout = timeout
		self.user = 'root'
		self.ssh = paramiko.SSHClient()
		self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		
	def _check_connection(self):
		if not self.connected or (self.connected and not self.ssh._transport.is_alive()):
			self.connect()
		
	def connect(self):
		start_time = time.time()
		while time.time() - start_time < self.timeout:
			try:
				self.ssh.connect(self.host, pkey = self.key, username=self.user)
				break
			except:
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
		
#def tail_log_channel(channel):
#	if channel.closed:
#		raise Exception('Channel is closed')
#	
#	while channel.recv_ready():
#		channel.recv(1)
#		
#	cmd = 'tail -f -n 0 /var/log/scalarizr.log\n'
#	channel.send(cmd)
#	channel.recv(len(cmd))

def exec_command(channel, cmd, timeout = 60):
	
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