'''
Created on Aug 29, 2011

@author: marat
'''

import logging
import os
import subprocess
import re
import time

from scalarizr import util


import yaml


LOG = logging.getLogger(__name__)


class CloudFoundryError(Exception):
	pass


class VCAPExec(object):
	def __init__(self, cf):
		self.cf = cf
		
	def __call__(self, *args):
		cmd = [self.cf.home + '/bin/vcap']
		cmd += args
		cmd.append('--no-color')
		LOG.debug('Executing %s', cmd)
		return system(cmd, close_fds=True, warn_stderr=True)
		#cmd = ' '.join(cmd)
		#return util.system2(('/bin/bash', '-c', 'source /root/.bashrc; ' + cmd), 
		#				close_fds=True, warn_stderr=True)		
		

class Component(object):
	
	def __init__(self, cf, name, config_file=None):
		self.cf = cf
		self.name = name
		self.config_file = config_file or os.path.join(self.cf.home, 
													name, 'config', name + '.yml')
		self.config = yaml.load(open(self.config_file))
	
	
	def start(self):
		self.cf.vcap_exec('start', self.name)
	
	
	def stop(self):
		self.cf.vcap_exec('stop', self.name)
	
	
	def restart(self):
		self.cf.vcap_exec('restart', self.name)		

		
	@property
	def running(self):
		if os.path.exists(self.pid_file):
			stat_file = '/proc/%s/stat' % self.pid
			if os.path.exists(stat_file):
				stat = open(stat_file).read()
				LOG.debug('Contents of %s:\n%s', stat_file, stat)
				return stat.split(' ')[2] != 'Z'
			else:
				LOG.debug('Component %s not running File %s ')
		return False

	
	@property
	def pid_file(self):
		return self.config['pid']


	@property
	def pid(self):
		return open(self.pid_file).read().strip()
		
		
	@property
	def log_file(self):
		return '/tmp/vcap-run/%s.log' % self.name


	def _get_local_route(self):
		if not self._local_route:
			self._local_route = self.get_config(r'local_route')
			LOG.debug('Found %s local_route: %s', self.name, self._local_route)
		return self._local_route
	
	
	def _set_local_route(self, ip):
		LOG.debug('Setting %s local route: %s', self.name, ip)
		util.system2(('sed', '--in-place', 's/local_route.*/local_route: %s/1' % ip, self.config_file))
		self._local_route = ip

	
	local_route = property(_get_local_route, _set_local_route)

	@property
	def home(self):
		return os.path.join(self.cf.home, self.name)

class CloudFoundry(object):
	
	def __init__(self, home):
		self.home = home
		self.vcap_exec = VCAPExec(self)
		self.components = {}
		for name in ('cloud_controller', 'router', 'health_manager', 'dea'):
			self.components[name] = Component(self, name)
		
		self._mbus_url = None
		self._cloud_controller = None
		self._db_file = None
		
			
	def _set_mbus(self, url):
		LOG.debug('Changing mbus server: %s', url)
		find = subprocess.Popen(('find', self.home, '-name', '*.yml'), stdout=subprocess.PIPE)
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
		LOG.debug('Setting cloud controller host: %s', host)
		self._cloud_controller = host
		self.mbus = 'mbus://%s:4222/' % host
	
		
	def _get_cloud_controller(self):
		return self._cloud_controller
	

	cloud_controller = property(_get_cloud_controller, _set_cloud_controller)
	
	
	def _set_home(self, path):
		self._home = path
		self.vcap_exec = VCAPExec(self.home + '/bin/vcap')
	
		
	def _get_home(self):
		return self._home

	
	def start(self, *cmps):
		started = []
		for name in cmps:
			cmp = self.components[name]
			LOG.info('Starting %s', name)
			cmp.start()
			started.append(cmp)				
		
		# Check 3 times that all requred services were started
		i = 0
		while i < 3:
			failed = []
			for cmp in started:
				if not cmp.running:
					failed.append(cmp)
					if os.path.exists(cmp.log_file):
						LOG.error('%s failed to start', cmp.name)
						LOG.warn('Contents of %s:\n%s', cmp.log_file, open(cmp.log_file).read())
					else:
						LOG.error('%s failed to start and dies without any logs', cmp.name)
			if not failed:
				break
			started = failed
			i += 1
			if i < 3:
				time.sleep(5)
			
		if failed:
			raise CloudFoundryError('%d component(s) failed to start (%s)' % ( 
									len(failed), ', '.join(cmp.name for cmp in failed)))

	
	def stop(self, *cmps):
		for name in cmps:
			cmp = self.components[cmp]
			LOG.info('Stopping %s', name)
			cmp.stop()
			
			
	def init_db(self):
		cmp = self.components['cloud_controller']
		dbenv = cmp.config['databases'][cmp.config['rails_environment']]
		if dbenv['adapter'] == 'sqlite3':
			if not os.path.exists(dbenv['database']):
				LOG.debug("Database doesn't exists. Creating")
				system('rake db:migrate', cwd=cmp.home)


	def valid_datadir(self, datadir):
		return os.path.exists(os.path.join(datadir, 'cloud_controller'))
			
			
	def init_datadir(self, datadir):
		for name in ('dea', 'cloud_controller/db', 'cloud_controller/tmp'):
			dir = os.path.join(datadir, name)
			if not os.path.exists(dir):
				os.makedirs(dir)



def system(*args, **kwds):
	cmd = args[0]
	if not isinstance(cmd, basestring):
		cmd = ' '.join(cmd)
	return util.system2(('/bin/bash', '-c', 'source /root/.bashrc; ' + cmd), **kwds)	