'''
Created on Sep 23, 2010

@author: marat
'''
from scalarizr.libs.metaconf import Configuration, NoPathError
from szr_integtest import config
from szr_integtest_libs import exec_command
import os
import re

DISTR_DETECTION_STRING = "python -c \"import platform; d = platform.dist(); print int(d[0].lower() in ['centos', 'rhel', 'redhat'] and d[1].split('.')[0]); \
print int((d[0].lower() == 'fedora' or (d[0].lower() == 'redhat' and d[2].lower() == 'werewolf')) and d[1].split('.')[0])\""
EPEL_PACKAGE = "http://download.fedora.redhat.com/pub/epel/5/i386/epel-release-5-4.noarch.rpm"


class RepoType:
	LOCAL = 'local'
	PRODUCTION = 'production'
	NIGHTLY_BUILD = 'nightly_build'
	RELEASE = 'release'

class ScalarizrDeploy:
	distr = None
	
	def __init__(self, channel):
		self.channel = channel
		self.config = config
		self.detect_distr()
		
	def add_repos(self, repo_type):

		if self.channel.closed:
			raise Exception("SSH Channel closed")
		
		if self.distr in ('rhel', 'fedora'):
			
			if 'rhel' == self.distr:

				python_ver = self.get_python_version()
				if python_ver < 6:
					exec_command(self.channel, 'rpm -Uvh ' + EPEL_PACKAGE)
					out = exec_command(self.channel, 'yum -y install python26')
					if not re.search('Complete!|Nothing to do', out):
						raise Exception("Can't install python 2.6")
				
			# Red Hat based
			try:
				repo_url = self.config.get('./repos-rpm/'+repo_type+'_repo_url')
			except NoPathError:
				raise Exception("Configuration file doesn't contain %s repository url" % repo_type)
			# Add repo
			baseurl = os.path.join(repo_url, '%s/$releasever/$basearch' % self.distr)
			self.channel.send("echo -e '[scalarizr]\nname=scalarizr\nbaseurl=%s\nenabled=1\ngpgcheck=0' > /etc/yum.repos.d/scalarizr.repo\n" % baseurl)		

		else:
			# Debian based
			out = exec_command(self.channel, 'wget http://apt.scalr.net/scalr-repository_0.2_all.deb')
			if not 'saved' in out:
				raise Exception("Cannot download scalarizr's repo package")
			
			out = exec_command(self.channel, 'dpkg -i scalr-repository_0.2_all.deb')
			if not 'Adding Intridea keyring to local trust store... OK' in out:
				raise Exception("Cannot install scalarizr's repo package")
			
			try:
				repo_url = self.config.get('./repos-deb/'+repo_type+'_repo_url')
			except NoPathError:
				raise Exception("Configuration file doesn't contain %s repository url" % repo_type)
			
			exec_command(self.channel, "echo %s > /etc/apt/sources.list.d/scalr.list" % repo_url)
			out = exec_command(self.channel, "apt-get update")
			if not "Reading package lists... Done" in out:
				raise Exception("Something wrong with updating package list")					
	
	def install_package(self):
		
		if self.channel.closed:
			raise Exception("SSH Channel closed")

		if self.distr in ('rhel', 'fedora'):
			# Install scalarizr
			out = exec_command(self.channel, 'yum -y install scalarizr', 120)
			if not re.search('Complete!|Nothing to do', out):
				raise Exception('Cannot install scalarizr')
		else:
			# Debian based
			out = exec_command(self.channel, 'apt-get -y install scalarizr')

			error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
			if error:
				raise Exception("Can't install scalarizr package: '%s'" % error.group('err_text'))			
	
	def update_package(self):

		self.check_scalarizr_installed()
		if self.distr in ('rhel', 'fedora'):
			exec_command(self.channel, 'yum -y update scalarizr')
		else:
			exec_command(self.channel, 'apt-get -y install scalarizr')
	
	def apply_changes_from_svn(self):
		self.check_scalarizr_installed()		
		try:
			svn_repo = config.get('./general/svn_repo')
			svn_user = config.get('./general/svn_user')
			svn_password = config.get('./general/svn_password')
		except:
			raise Exception("Can't retrieve necessary options from config")
		
		if not svn_repo:
			raise Exception("Svn repository url is empty!")
		
		svn_installed = exec_command(self.channel, 'ls -la /usr/bin/svn 2>/dev/null')
		if not svn_installed:
			
			if self.distr in ('rhel', 'fedora'):
				out = exec_command(self.channel, 'yum -y install subversion')
				if not re.search('Complete!|Nothing to do', out):
					raise Exception("Cannot install subversion")
			else:
				out = exec_command(self.channel, 'apt-get update; apt-get -y install subversion')
				error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
				if error:
					raise Exception("Can't install subversion package: '%s'" % error.group('err_text'))
		
		py_version = self.get_python_version()
		
		if self.distr in ('rhel', 'fedora'):
			scalarizr_path = "/usr/lib/python2.%s/site-packages/scalarizr"  % py_version
		else:
			scalarizr_path = "/var/lib/python-support/python2.%s/scalarizr" % py_version
		
		exec_command(self.channel, 'rm -rf ' + scalarizr_path)
		
		cmd = 'svn export %s %s' % (svn_repo, scalarizr_path)
		cmd += (' --username %s ' % svn_user) if svn_user else ''
		cmd += (' --password %s ' % svn_password) if svn_password else ''
		out = exec_command(self.channel, cmd)
		if not 'Exported revision' in out:
			raise Exception('Svn export failed.')

	def detect_distr(self):
		if not self.distr:
			ret = exec_command(self.channel, DISTR_DETECTION_STRING)
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
				
	def get_python_version(self):
		for i in range(7, 4, -1):
			out = exec_command(self.channel, 'ls -la /usr/bin/python2.%s 2>/dev/null' % i)
			if out:
				return i
			
		out = exec_command(self.channel, "python -c 'import platform; print platform.python_version()[2]'")

		try:
			python_ver = int(out)
		except:
			raise Exception("Can't detect python version")
		return python_ver
	
	def check_scalarizr_installed(self):
		scalarizr_installed = exec_command(self.channel, 'ls -la /etc/init.d/scalarizr 2>/dev/null')
		if not scalarizr_installed:
			raise Exception('Install scalarizr package first!')
				


			
def create_nightly_build(svn_repo, dist):
	'''
	Requires that /usr/bin/szrbuild is installed
	'''
	pass