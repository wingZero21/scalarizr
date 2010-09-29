'''
Created on Sep 23, 2010

@author: marat
'''
from scalarizr.libs.metaconf import Configuration, PathNotExistsError
import os
import re
import time

BASE_PATH = os.path.join(os.path.dirname(__file__), '..' + os.path.sep + '..')
RESOURCE_PATH = os.path.join(BASE_PATH, 'resources')
CONFIG_PATH   = os.path.join(RESOURCE_PATH, 'integ_test.ini')

DISTR_DETECTION_STRING = "python -c \"import platform; d = platform.dist(); print int(d[0].lower() in ['centos', 'rhel', 'redhat'] and d[1].split('.')[0]); \
print int((d[0].lower() == 'fedora' or (d[0].lower() == 'redhat' and d[2].lower() == 'werewolf')) and d[1].split('.')[0])\""


if not os.path.exists(CONFIG_PATH):
	raise Exception("Configuration file doesn't exist: %s" % CONFIG_PATH)

class RepoType:
	LOCAL = 'local'
	PRODUCTION = 'production'
	NIGHTLY_BUILD = 'nightly_build'

class ScalarizrDeploy:
	distr = None
	
	def __init__(self, ssh):
		self.ssh = ssh
		self.config = Configuration('ini')
		self.config.read(CONFIG_PATH)		
		self.detect_distr()
		
	def add_repos(self, repo_type):

		if self.ssh.closed:
			raise Exception("SSH Channel closed")
		
		if self.distr in ('rhel', 'fedora'):
			# Red Hat based
			try:
				repo_url = self.config.get('./repos-rpm/'+repo_type+'_repo_url')
			except PathNotExistsError:
				raise Exception("Configuration file doesn't contain %s repository url" % repo_type)
			# Add repo
			baseurl = os.path.join(repo_url, 'rpm') + os.sep + '%s/$releasever/$basearch' % self.distr
			self.ssh.send("echo -e '[scalarizr]\nname=scalarizr\nbaseurl=%s\nenabled=1\ngpgcheck=0' > /etc/yum.repos.d/scalarizr.repo\n" % baseurl)		

		else:
			# Debian based
			out = exec_command(self.ssh, 'wget http://apt.scalr.net/scalr-repository_0.2_all.deb')
			if not out or not 'saved' in out:
				raise Exception("Cannot download scalarizr's repo package")
			
			out = exec_command(self.ssh, 'dpkg -i scalr-repository_0.2_all.deb')
			if not out or not 'Adding Intridea keyring to local trust store... OK' in out:
				raise Exception("Cannot install scalarizr's repo package")
			
			try:
				repo_url = self.config.get('./repos-deb/'+repo_type+'_repo_url')
			except PathNotExistsError:
				raise Exception("Configuration file doesn't contain %s repository url" % repo_type)
			
			exec_command(self.ssh, "echo %s > /etc/apt/sources.list.d/scalr.list" % repo_url)
			out = exec_command(self.ssh, "apt-get update")
			if not out or not "Reading package lists... Done" in out:
				raise Exception("Something wrong with updating package list")
			
					
	
	def install_package(self):
		if self.ssh.closed:
			raise Exception("SSH Channel closed")

		if self.distr in ('rhel', 'fedora'):
			# Install scalarizr
			out = exec_command(self.ssh, 'yum -y install scalarizr')
			if not 'Complete!' in out:
				raise Exception('Cannot install scalarizr')
			
		else:
			out = exec_command(self.ssh, 'apt-get -y install scalarizr')
			
	
	def update_package(self):
		pass
	
	def apply_changes_from_svn(self, repo_url):
		pass

	def detect_distr(self):
		if not self.distr:
			ret = exec_command(self.ssh, DISTR_DETECTION_STRING)
			rh_based_result = re.findall('^(\d)\s*$', ret, re.M)
			if len(rh_based_result) != 2:
				raise Exception("Cannot detect distr")
			rhel, fedora = [int(i) for i in rh_based_result]
			if rhel:
				self.distr = 'rhel'
			elif fedora:
				self.distr = 'fedora'
			else:
				self.distr = 'debian'

def exec_command(channel, cmd, timeout = 60):
	while channel.recv_ready():
		channel.recv(1)
	channel.send(cmd)
	command = channel.recv(len(cmd))
	newlines = re.findall('\r', command)
	if newlines:
		channel.recv(len(newlines))
	channel.send('\n')
	out = ''
	start_time = time.time()
	
	while time.time() - start_time < timeout:
		if channel.recv_ready():
			out += channel.recv(1024)
			if re.search('root@.*?#', out):
				break
	else:
		raise Exception('Timeout while doing "%s"' % cmd)
	lines = out.splitlines()
	if len(lines) > 2:
		return '\n'.join(lines[1:-1]).strip()
	else:
		return None
			
def create_nightly_build(svn_repo, dist):
	'''
	Requires that /usr/bin/szrbuild is installed
	'''
	pass