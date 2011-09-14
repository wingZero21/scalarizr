'''
Created on Aug 29, 2011

@author: marat
'''

import logging
import os
import subprocess
import re

from scalarizr import util

LOG = logging.getLogger(__name__)


class VCAPExec(object):
	def __init__(self, cf):
		self.cf = cf
		
	def __call__(self, *args):
		cmd = [self.cf.vcap_home + '/bin/vcap']
		cmd += args
		cmd.append('--no-color')
		LOG.debug('Executing %s', cmd)
		cmd = ' '.join(cmd)
		return util.system2(('/bin/bash', '-c', 'source /root/.bashrc; ' + cmd), 
						close_fds=True, warn_stderr=True)		
		

class Component(object):
	
	def __init__(self, cf, name, config_file=None):
		self.cf = cf
		self.name = name
		self.config_file = config_file or os.path.join(self.cf.vcap_home, 
													name, 'config', name + '.yml')
		self._pid_file = None 
	
	
	def start(self):
		self.cf.vcap_exec(self.name, 'start')
	
	
	def stop(self):
		self.cf.vcap_exec(self.name, 'stop')
	
	
	def restart(self):
		self.cf.vcap_exec(self.name, 'restart')		


	@property
	def running(self):
		if os.path.exists(self.pid_file):
			stat_file = '/proc/%s/stat' % self.pid
			if os.path.exists(stat_file):
				stat = open(stat_file).read()
				return stat.split(' ')[2] != 'Z'
		return False
	
	
	@property
	def pid_file(self):
		if not self._pid_file:
			for line in open(self.config_file):
				matcher = re.match(r'pid:\s+(.*)', line)
				if matcher:
					self._pid_file = matcher.group(1)
		return self._pid_file


	@property
	def pid(self):
		open(self.pidfile).read().strip()


class CloudFoundry(object):
	
	def __init__(self, vcap_home):
		self.vcap_home = vcap_home
		self.vcap_exec = VCAPExec(self)
		self.components = {}
		for name in ('router', 'cloud_controller', 'health_manager', 'dea'):
			self.components[name] = Component(self, name)
		
		self._mbus_url = None
		self._cloud_controller = None
		
			
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
	
	
	def _set_vcap_home(self, path):
		self._vcap_home = path
		self.vcap_exec = VCAPExec(self.vcap_home + '/bin/vcap')
	
		
	def _get_vcap_home(self):
		return self._vcap_home
	
	
	