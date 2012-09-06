'''
Created on Aug 28, 2012

@author: marat
'''

import logging
import re
import string
import time

from scalarizr import linux


LOG = logging.getLogger(__name__)


class PackageMgr(object):

	def install(self, name, version=None, updatedb=False, **kwds):
		''' Installs a `version` of package `name` '''
		raise NotImplementedError()

	def installed(self, name):
		''' Return installed package version '''
		raise NotImplementedError()	

	def updatedb(self):
		''' Updates package manager internal database '''
		raise NotImplementedError()

	def check_update(self, name):
		''' Returns info for package `name` '''
		raise NotImplementedError()

	def candidates(self, name):
		''' Returns all available installation candidates for `name` '''
		raise NotImplementedError()


class AptPackageMgr(PackageMgr):
	def apt_get_command(self, command, **kwds):
		kwds.update(env={
			'DEBIAN_FRONTEND': 'noninteractive', 
			'DEBIAN_PRIORITY': 'critical',
			'PATH': '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games'
		})
		return linux.system(('/usr/bin/apt-get',
						'-q', '-y', '--force-yes',
						'-o Dpkg::Options::=--force-confold') + \
						tuple(filter(None, command.split())), **kwds)
		

	def apt_cache_command(self, command, **kwds):
		return linux.system(('/usr/bin/apt-cache',) + tuple(filter(None, command.split())), **kwds)


	def apt_policy(self, name):
		installed_re = re.compile(r'^\s{2}Installed: (.+)$')
		candidate_re = re.compile(r'^\s{2}Candidate: (.+)$')
		installed = candidate = None
		
		for line in self.apt_cache_command('policy %s' % name)[0].splitlines():
			m = installed_re.match(line)
			if m:
				installed = m.group(1)
				if installed == '(none)':
					installed = None
				continue
			
			m = candidate_re.match(line)
			if m:
				candidate = m.group(1)
				continue
			
		return installed, candidate
	

	def updatedb(self):
		self.apt_get_command('update')		


	def candidates(self, name):
		return [self.apt_policy(name)[1]]

	def check_update(self, name):
		installed, candidate = self.apt_policy(name) 
		if not linux.system(('usr/bin/dpkg', '--compare-versions', candidate, 'gt',
											installed), raise_exc = False)[2]:
				return candidate
				
	def install(self, name, version=None, updatedb=False, **kwds):
		if version:
			name += '=%s' % version
		if updatedb:
			self.updatedb()
		for _ in range(0, 30):
			try:
				self.apt_get_command('install %s' % name, raise_exc=True)
				break
			except linux.LinuxError, e:
				if not 'E: Could not get lock' in e.err:
					raise
				time.sleep(2)



	def installed(self, name):
		out, code = linux.system(('dpkg-query', '--showformat', 
								'${Status} ${Package} ${Version}', 
								'--show', name), 
								raise_exc=False)[::2]
		if not code:
			cols = out.split()
			return cols[2] == 'installed' and cols[4]
		return None 


class RpmVersion(object):
	
	def __init__(self, version):
		self.version = version
		self._re_not_alphanum = re.compile(r'^[^a-zA-Z0-9]+')
		self._re_digits = re.compile(r'^(\d+)')
		self._re_alpha = re.compile(r'^([a-zA-Z]+)')
	
	def __iter__(self):
		ver = self.version
		while ver:
			ver = self._re_not_alphanum.sub('', ver)
			if not ver:
				break

			if ver and ver[0].isdigit():
				token = self._re_digits.match(ver).group(1)
			else:
				token = self._re_alpha.match(ver).group(1)
			
			yield token
			ver = ver[len(token):]
			
		raise StopIteration()
	
	def __cmp__(self, other):
		iter2 = iter(other)
		
		for tok1 in self:
			try:
				tok2 = iter2.next()
			except StopIteration:
				return 1
		
			if tok1.isdigit() and tok2.isdigit():
				c = cmp(int(tok1), int(tok2))
				if c != 0:
					return c
			elif tok1.isdigit() or tok2.isdigit():
				return 1 if tok1.isdigit() else -1
			else:
				c = cmp(tok1, tok2)
				if c != 0:
					return c
			
		try:
			iter2.next()
			return -1
		except StopIteration:
			return 0


class YumPackageMgr(PackageMgr):


	def yum_command(self, command, **kwds):
		return linux.system((('/usr/bin/yum', '-d0', '-y') + tuple(filter(None, command.split()))), **kwds)
	
	
	def rpm_ver_cmp(self, v1, v2):
		return cmp(RpmVersion(v1), RpmVersion(v2))

	
	def yum_list(self, name):
		out = self.yum_command('list --showduplicates %s' % name)[0].strip()
		
		version_re = re.compile(r'[^\s]+\s+([^\s]+)')
		lines = map(string.strip, out.splitlines())
		
		try:
			line = lines[lines.index('Installed Packages')+1]
			installed = version_re.match(line).group(1)
		except ValueError:
			installed = None

		if 'Available Packages' in lines:		
			versions = [version_re.match(line).group(1) for line in lines[lines.index('Available Packages')+1:]]
		else:
			versions = []
		
		return installed, versions
	
	
	def candidates(self, name):
		installed, versions = self.yum_list(name)
		
		if installed:
			versions = [v for v in versions if self.rpm_ver_cmp(v, installed) > 0]
		
		return versions


	def updatedb(self):
		self.yum_command('clean expire-cache')		


	def check_update(self, name):
		out, _, code = self.yum_command('check-update %s' % name)
		if code == 100:
			return filter(None, out.strip().split(' '))[1]


	def install(self, name, version=None, updatedb=False, **kwds):
		if version:
			name += '-%s' % version
		if updatedb:
			self.updatedb()
		self.yum_command('install %s' %  name, raise_exc=True)


	def installed(self, name):
		return self.yum_list(name)[0]


def package_mgr():
	if linux.os['family'] in ('RedHat', 'Oracle'):
		return YumPackageMgr()
	return AptPackageMgr()
