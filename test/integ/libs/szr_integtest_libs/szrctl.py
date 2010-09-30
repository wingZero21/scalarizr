'''
Created on Sep 23, 2010

@author: marat
'''
from szr_integtest_libs import exec_command

INIT_SCRIPT = '/etc/init.d/scalarizr'


class Scalarizr:
	def __init__(self):
		pass
	
	def use(self, channel):
		if channel.closed:
			raise Exception('Channel closed')
		
		self.channel = channel
	
	def restart(self):
		self._start_stop_reload('restart')
	
	def start(self):
		self._start_stop_reload('start')
	
	def stop(self):
		self._start_stop_reload('stop')
	
	def _start_stop_reload(self, cmd):
		out = exec_command(self.channel, INIT_SCRIPT + ' ' + cmd)
		if self._get_ret_code() != '0':
			raise Exception("Cannot %s scalarizr. Out: %s" % (cmd, out))
		
	def execute(self, options=None):
		pass

	def _get_ret_code(self):		
		return exec_command(self.channel, 'echo $?')