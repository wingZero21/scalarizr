'''
Created on Nov 2, 2012

@author: uty
'''

from scalarizr import linux
from scalarizr.linux import pkgmgr

import mock

@mock.patch.dict('scalarizr.linux.os', {'family': 'RedHat'})
def test_package_mgr():
	mgr = pkgmgr.package_mgr()
	assert isinstance(mgr, pkgmgr.YumPackageMgr)


@mock.patch.dict('scalarizr.linux.os', {'family': 'RedHat', 'name': 'CentOS'})
@mock.patch.object(pkgmgr.RPMPackageMgr, 'installed')
@mock.patch.object(pkgmgr.RPMPackageMgr, 'install')
@mock.patch('scalarizr.linux.system')
def test_epel_repository(system, install, installed):
	installed.return_value = False
	pkgmgr.epel_repository()
	install.assert_called_once_with(pkgmgr.EPEL_RPM_URL)


@mock.patch.dict('scalarizr.linux.os', {'family': 'Debian',
										'codename1': 'c1',
										'codename12': 'c12', 
										'codename2': 'c2', 
										'codename22': 'c22', 
										'arch': 'x86_64'})
@mock.patch('__builtin__.open')
@mock.patch('scalarizr.linux.system')
def test_apt_source(system, open):
	name = 'test_list'
	sources = ['deb http://test.repo/apt/${arch} ${codename1} ${codename12} main',
			   'deb-src http://test.repo/apt ${codename2} ${codename22} main']
	gpg_keyserver = 'key_server'
	gpg_keyid = 'key_id'
	file_contents = 'deb http://test.repo/apt/x86_64 c1 c12 main\ndeb-src http://test.repo/apt c2 c22 main'
	pkgmgr.apt_source(name, sources, gpg_keyserver, gpg_keyid)

	linux.system.assert_called_once_with(('apt-key', 'adv', 
				  						  '--keyserver', 'key_server',
										  '--recv', 'key_id'),
										 raise_exc=False)
	open.assert_called_once_with('/etc/apt/sources.list.d/'+name, 'w+')
	write_mock = open.return_value.write
	write_mock.assert_called_once_with(file_contents)
	

@mock.patch('scalarizr.linux.pkgmgr.package_mgr')
def test_installed(mgr):
	mgr().installed.return_value = False

	pkgmgr.installed('thing', '1.0', True)
	
	mgr().install.assert_called_once_with('thing', '1.0', True)


@mock.patch('scalarizr.linux.pkgmgr.package_mgr')
def test_latest(mgr):
	mgr().check_update.return_value = None
	pkgmgr.latest('thing', True)

	mgr().install.assert_called_once_with('thing', updatedb=True)

	mgr().check_update.return_value = 'better_thing'
	pkgmgr.latest('thing', False)

	mgr().install.assert_called_with('better_thing', updatedb=False)


@mock.patch('scalarizr.linux.pkgmgr.package_mgr')
def test_removed(mgr):
	mgr().installed.return_value = True

	pkgmgr.removed('thing', True)

	mgr().remove.assert_called_once_with('thing', True)


#RPMPackageMgr class tests
class TestRPMPackageMgr(object):
		
	@mock.patch('scalarizr.linux.system')
	def test_rpm_command(self, system):
		mgr = pkgmgr.RPMPackageMgr()
		mgr.rpm_command('-Uvh test.rpm', raise_exc=True)

		system.assert_called_once_with(('/usr/bin/rpm', '-Uvh', 'test.rpm'), raise_exc=True)


	@mock.patch.object(pkgmgr.RPMPackageMgr, 'rpm_command')
	def test_install(self, rpm_command):
		mgr = pkgmgr.RPMPackageMgr()
		mgr.install('test.rpm', 1.1, True, test_kwd=True)

		rpm_command.assert_called_once_with('-Uvh test.rpm', raise_exc=True, test_kwd=True)
		

	@mock.patch.object(pkgmgr.RPMPackageMgr, 'rpm_command')
	def test_remove(self, rpm_command):
		mgr = pkgmgr.RPMPackageMgr()
		mgr.remove('test.rpm', True)

		rpm_command.assert_called_once_with('-e test.rpm', raise_exc=True)
		

	@mock.patch.object(pkgmgr.RPMPackageMgr, 'rpm_command')
	def test_installed(self, rpm_command):
		mgr = pkgmgr.RPMPackageMgr()
		mgr.installed('http://www.test.repo/smothing/test.rpm')

		rpm_command.assert_called_once_with('-q test.rpm', raise_exc=False)
		

	@mock.patch('scalarizr.linux.system')
	def test_updatedb(self, system):
		mgr = pkgmgr.RPMPackageMgr()
		mgr.updatedb()

		assert not system.called
		

	@mock.patch('scalarizr.linux.system')
	def test_check_update(self, system):
		mgr = pkgmgr.RPMPackageMgr()
		mgr.check_update('test')

		assert not system.called
		

	@mock.patch('scalarizr.linux.system')
	def test_candidates(self, system):
		mgr = pkgmgr.RPMPackageMgr()
		result = mgr.candidates('test')

		assert not system.called
		assert result == []
	