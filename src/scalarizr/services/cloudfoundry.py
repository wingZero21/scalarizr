'''
Created on Aug 29, 2011

@author: marat
'''

import os
import subprocess
import string

from scalarizr import util


class CloudFoundry(object):
	
	def __init__(self, vcap_home):
		self.vcap_home = vcap_home
		self._mbus = None
		self._cloud_controller = None
		
	
	def start(self, svs=None):
		self._start_stop_restart('start', svs)
	
	
	def stop(self, svs=None):
		self._start_stop_restart('stop', svs)
	
	
	def restart(self, svs=None):
		self._start_stop_restart('restart', svs)
		
	
	def _start_stop_restart(self, action, svs=None):
		args = [action]
		if svs:
			args.append(svs)
		self.exec_vcap(*args)
	
		
	def status(self):
		ret = {}
		for line in self.exec_vcap('status')[0].splitlines():
			svs, status = map(string.strip, line.split(':'))
			ret[svs] = status == 'RUNNING'
		return ret
	
	def exec_vcap(self, *args):
		cmd = [os.path.join(self.vcap_home, 'bin/vcap')]
		cmd.append(args)
		cmd.append('--no-color')		
		return util.system2(cmd)
	
	
	def _set_mbus(self, url):
		find = subprocess.Popen(('find', self.vcap_home, '-name', '*.yml'), stdout=subprocess.PIPE)
		grep = subprocess.Popen(('xargs', 'grep', '--files-with-matches', 'mbus'), stdin=find.stdout, stdout=subprocess.PIPE)
		sed = subprocess.Popen(('xargs', 'sed', '--in-place', 's/mbus.*/mbus: %s/1' % url.replace('/', '\/')), stdin=grep.stdout)
		out, err = sed.communicate()
		if sed.returncode:
			raise util.PopenError('Failed to update mbus for all VCAP components', out, err, sed.returncode, None)
		self._mbus_url = url
	
	
	def _get_mbus(self, mbus):
		return self._mbus_url
	
	mbus = property(_get_mbus, _set_mbus)
	
	
	def _set_cloud_controller(self, host):
		self._cloud_controller = host
		self.mbus = 'mbus://%s:4222/' % host
	
		
	def _get_cloud_controller(self):
		return self._cloud_controller
	
	
	cloud_controller = property(_get_cloud_controller, _set_cloud_controller)