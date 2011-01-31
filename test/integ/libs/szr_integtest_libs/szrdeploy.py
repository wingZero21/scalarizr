'''
Created on Sep 23, 2010

@author: marat
'''
from szr_integtest					import config
from szr_integtest_libs.ssh_tool	import execute

from scalarizr.libs.metaconf import NoPathError

import os
import re
import tarfile 
import time

DISTR_DETECTION_STRING = "python -c \"import platform; d = platform.dist(); print int(d[0].lower() in ['centos', 'rhel', 'redhat'] and d[1].split('.')[0]); \
print int((d[0].lower() == 'fedora' or (d[0].lower() == 'redhat' and d[2].lower() == 'werewolf')) and d[1].split('.')[0])\""
EPEL_PACKAGE = "http://download.fedora.redhat.com/pub/epel/5/i386/epel-release-5-4.noarch.rpm"
SZR_PATH = os.path.realpath(os.path.join(os.path.dirname(__file__), '../../../../src/scalarizr'))
SHARE_PATH = os.path.realpath(os.path.join(os.path.dirname(__file__), '../../../../share'))

class RepoType:
	NIGHTLY_BUILD = 'nightly'	
	RC = 'rc'
	RELEASE = 'release'

class ScalarizrDeploy:
	dist = None
	
	def __init__(self, sshmanager):
		self.ssh = sshmanager
		if not self.ssh.connected:
			self.ssh.connect()
		self.config = config
		self.channel = sshmanager.get_root_ssh_channel()
		self.detect_dist()
		
	def add_repos(self, repo_type):

		if self.channel.closed:
			raise Exception("SSH Channel closed")
		
		if self.dist in ('rhel', 'fedora'):
			
			if 'rhel' == self.dist:

				python_ver = self.get_python_version()
				if python_ver < 6:
					execute(self.channel, 'rpm -Uvh ' + EPEL_PACKAGE)
					out = execute(self.channel, 'yum -y install python26', 120)
					if not re.search('Complete!|Nothing to do', out):
						raise Exception("Can't install python 2.6")
				
			# Red Hat based
			try:
				repo_url = self.config.get('./repos-rpm/'+repo_type+'_repo_url')
			except NoPathError:
				raise Exception("Configuration file doesn't contain %s repository url" % repo_type)
			# Add repo
			baseurl = os.path.join(repo_url, '%s/$releasever/$basearch' % self.dist)
			execute(self.channel, "echo -e '[scalarizr]\nname=scalarizr\nbaseurl=%s\nenabled=1\ngpgcheck=0' > /etc/yum.repos.d/scalarizr.repo" % baseurl)		

		else:
			# Debian based
			out = execute(self.channel, 'wget http://apt.scalr.net/scalr-repository_0.2_all.deb')
			if not 'saved' in out:
				raise Exception("Cannot download scalarizr's repo package")
			
			out = execute(self.channel, 'dpkg -i scalr-repository_0.2_all.deb')
			if not 'Adding Intridea keyring to local trust store... OK' in out:
				raise Exception("Cannot install scalarizr's repo package")
			
			try:
				repo_url = self.config.get('./repos-deb/'+repo_type+'_repo_url')
			except NoPathError:
				raise Exception("Configuration file doesn't contain %s repository url" % repo_type)
			
			execute(self.channel, "echo %s > /etc/apt/sources.list.d/scalr.list" % repo_url)
			out = execute(self.channel, "apt-get update", 120)
			if not "Reading package lists... Done" in out:
				raise Exception("Something wrong with updating package list")					
	
	def install_package(self):
		
		if self.channel.closed:
			raise Exception("SSH Channel closed")

		if self.dist in ('rhel', 'fedora'):
			# Install scalarizr
			out = execute(self.channel, 'yum -y install scalarizr', 120)
			if not re.search('Complete!|Nothing to do', out):
				raise Exception('Cannot install scalarizr %s' % out)
		else:
			# Debian based
			out = execute(self.channel, 'apt-get -y install scalarizr')

			error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
			if error:
				raise Exception("Can't install scalarizr package: '%s'" % error.group('err_text'))			
	
	def update_package(self):

		self.check_scalarizr_installed()
		if self.dist in ('rhel', 'fedora'):
			execute(self.channel, 'yum -y update scalarizr')
		else:
			execute(self.channel, 'apt-get -y install scalarizr')
	
	def apply_changes_from_svn(self):
		self.check_scalarizr_installed()		
		try:
			svn_repo = config.get('./repos-svn/repo_url')
			svn_user = config.get('./repos-svn/user')
			svn_password = config.get('./repos-svn/password')
		except:
			raise Exception("Can't retrieve necessary options from config")
		
		if not svn_repo:
			raise Exception("Svn repository url is empty!")
		
		svn_installed = execute(self.channel, 'ls -la /usr/bin/svn 2>/dev/null')
		if not svn_installed:
			
			if self.dist in ('rhel', 'fedora'):
				out = execute(self.channel, 'yum -y install subversion')
				if not re.search('Complete!|Nothing to do', out):
					raise Exception("Cannot install subversion")
			else:
				out = execute(self.channel, 'apt-get update; apt-get -y install subversion')
				error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
				if error:
					raise Exception("Can't install subversion package: '%s'" % error.group('err_text'))
		
		scalarizr_path = self.get_scalarizr_path()
		
		execute(self.channel, 'rm -rf ' + scalarizr_path)
		cmd = 'echo yes | '
		cmd += 'svn export %s %s' % (os.path.join(svn_repo, 'src/scalarizr'), scalarizr_path)
		cmd += (' --username "%s" ' % svn_user) if svn_user else ''
		cmd += (' --password "%s" ' % svn_password) if svn_password else ''
		out = execute(self.channel, cmd)
		if not 'Exported revision' in out:
			raise Exception('Svn export failed')
		
		# Export share
		share_path = '/usr/share/scalr'
		execute(self.channel, 'rm -rf ' + share_path)
		cmd = 'echo yes | '
		cmd += 'svn export %s %s' % (os.path.join(svn_repo, 'share'), share_path)
		cmd += (' --username "%s" ' % svn_user) if svn_user else ''
		cmd += (' --password "%s" ' % svn_password) if svn_password else ''
		out = execute(self.channel, cmd)
		if not 'Exported revision' in out:
			raise Exception('Svn export failed')
		

	def apply_changes_from_tarball(self):

		file = 'szr-tarball.%s' % time.strftime('%Y_%m_%d.%H_%M') + '.tar.gz'
		file_path = '/tmp/' + file
		tarball = tarfile.open(file_path , 'w:gz')
		tarball.add(SZR_PATH, os.path.basename(SZR_PATH))
		tarball.add(SHARE_PATH, os.path.basename(SHARE_PATH))
		tarball.close()
		
		sftp = self.ssh.get_sftp_client()
		
		try:
			sftp.put(file_path, file_path)
		except (Exception, BaseException), e:
			raise BaseException('Error while uploading file %s: %s' % (file, e))

		scalarizr_path = self.get_scalarizr_path()
		execute(self.channel, 'rm -rf ' + scalarizr_path)
		execute(self.channel, 'tar -xzf ' + file_path + ' -C '+ os.path.dirname(scalarizr_path))
		execute(self.channel, 'mv ' + os.path.dirname(scalarizr_path) + '/share /usr/share/scalr')
				
		
	def detect_dist(self):
		if not self.dist:
			ret = execute(self.channel, DISTR_DETECTION_STRING)
			rh_based_result = re.findall('^(\d)\s*$', ret, re.M)
			if len(rh_based_result) != 2:
				raise Exception("Cannot detect dist")
			rhel, fedora = [int(i) for i in rh_based_result]
			if rhel:
				self.dist = 'rhel'
			elif fedora:
				self.dist = 'fedora'
			else:
				self.dist = 'debian'
				
	def get_python_version(self):
		for i in range(7, 4, -1):
			out = execute(self.channel, 'ls -la /usr/bin/python2.%s 2>/dev/null' % i)
			if out:
				return i
			
		out = execute(self.channel, "python -c 'import platform; print platform.python_version()[2]'")

		try:
			python_ver = int(out)
		except:
			raise Exception("Can't detect python version")
		return python_ver
	
	def check_scalarizr_installed(self):
		scalarizr_installed = execute(self.channel, 'ls -la /etc/init.d/scalarizr 2>/dev/null')
		if not scalarizr_installed:
			raise Exception('Install scalarizr package first!')
				
	def get_scalarizr_path(self):
		
		if self.dist in ('rhel', 'fedora'):
			py_version = self.get_python_version()
			scalarizr_path = "/usr/lib/python2.%s/site-packages/scalarizr"  % py_version
		else:
			scalarizr_path = "/usr/share/python-support/scalarizr-base/scalarizr"

		return scalarizr_path
			
def create_nightly_build(svn_repo, dist):
	'''
	Requires that /usr/bin/szrbuild is installed
	'''
	pass