'''
Created on Sep 23, 2010

@author: marat
'''
import pexpect

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

def spawn_tail_log(host, key_path):
	return pexpect.spawn('/usr/bin/ssh -o StrictHostKeyChecking=no -i %s %s tail -f /var/log/scalarizr.log' % (key_path, host))
