'''
Created on Sep 23, 2010

@author: marat
'''
import paramiko
import os
import time

class Scalarizr:
	def __init__(self):
		pass
	
	def use(self, ssh):
		pass
	
	def restart(self):
		pass
	
	def start(self):
		pass
	
	def stop(self):
		pass
	
	def execute(self, options=None):
		pass

class TailLogSpawner:
	
	def __init__(self, host, key, timeout= 60):
		self.host = host
		self.key = key
		self.timeout = timeout
		
	def spawn(self):
			
		if not os.path.isfile(self.key):
			raise Exception("Key %s doesn't exist" % self.key)
		
		self.ssh = paramiko.SSHClient()
		self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		start_time = time.time()
		while time.time() - start_time < self.timeout:
			try:
				self.ssh.connect(self.host, key_filename = self.key, username='root')
				break
			except:
				continue
		else:
			raise Exception("Cannot connect to server %s" % self.host)
		self.transport = self.ssh.get_transport()
		self.channel   = self.transport.open_session()
		self.channel.get_pty()
		self.channel.invoke_shell()
		time.sleep(0.5)
		while self.channel.recv_ready():
			self.channel.recv(1)
		cmd = 'tail -f -n 0 /var/log/scalarizr.log\n'
		self.channel.send(cmd)
		self.channel.recv(len(cmd))

		return self.channel
	


	#return pexpect.spawn('/usr/bin/ssh -o StrictHostKeyChecking=no -i %s %s tail -f /var/log/scalarizr.log' % (key_path, host))
