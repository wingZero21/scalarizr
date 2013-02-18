'''
Created on Sep 23, 2010

@author: marat
'''
from szr_integtest_libs.ssh_tool import execute

INIT_SCRIPT = '/etc/init.d/scalarizr'


class Scalarizr:
	in_use = False
	def __init__(self):
		pass
	
	def use(self, channel):
		if channel.closed:
			raise Exception('Channel closed')
		
		out = execute(channel, 'ls -la /etc/init.d/scalarizr 2>/dev/null')
		if not out:
			raise Exception("Scalarizr isn't installed")
				
		self.channel = channel
		self.in_use = True
	
	def restart(self):
		self._start_stop_reload('restart')
	
	def start(self):
		self._start_stop_reload('start')
	
	def stop(self):
		self._start_stop_reload('stop')
	
	def _start_stop_reload(self, cmd):
		execute(self.channel, INIT_SCRIPT + ' ' + cmd)
		if self._get_ret_code() != '0':
			raise Exception("Cannot %s scalarizr." % cmd)
		
	def execute(self, options=None):
		pass

	def _get_ret_code(self):		
		return execute(self.channel, 'echo $?')