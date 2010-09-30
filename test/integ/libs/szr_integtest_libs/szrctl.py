'''
Created on Sep 23, 2010

@author: marat
'''
from szr_integtest_libs import SshManager

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
		self.sshmanager = SshManager(host, key)
		self.sshmanager.connect()
		self.channel = self.sshmanager.get_root_ssh_channel()
		
	def spawn(self):
		while self.channel.recv_ready():
			self.channel.recv(1)
		cmd = 'tail -f -n 0 /var/log/scalarizr.log\n'
		self.channel.send(cmd)
		self.channel.recv(len(cmd))

		return self.channel
	


	#return pexpect.spawn('/usr/bin/ssh -o StrictHostKeyChecking=no -i %s %s tail -f /var/log/scalarizr.log' % (key_path, host))
