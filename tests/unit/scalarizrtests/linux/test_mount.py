'''
Created on Aug 27, 2012

@author: marat
'''

from scalarizr import linux
from scalarizr.linux import mount

import mock
from nose.tools import raises
import os


def test_mount():
	with mock.patch('scalarizr.linux.system') as m:
		mount.mount('/dev/sdb', '/mnt')
		assert m.called
	

@raises(mount.NoFileSystem)
def test_mount_no_filesystem():
	m = mock.Mock(side_effect=linux.LinuxError('', '', 'mount: you must specify the filesystem type\n', 32, ()))
	with mock.patch('scalarizr.linux.system', m):
		mount.mount('/dev/sdb', '/mnt')

		
def test_umount():
	with mock.patch('scalarizr.linux.system') as m:
		mount.umount('/mnt/mpoint')
		assert m.called


def test_umount_not_mounted():
	m = mock.Mock(side_effect=linux.LinuxError('', '', 'umount: /mnt: not mounted\n', 1, ()))
	with mock.patch('scalarizr.linux.system', m):
		mount.umount('/mnt')
		assert m.called

		
@raises(linux.LinuxError)
def test_umount_raise_error():
	m = mock.Mock(side_effect=linux.LinuxError('', '', 'umount: /: device is busy.\n', 1, ()))
	with mock.patch('scalarizr.linux.system', m):
		mount.umount('/')

	

class TestMounts(object):
	def setup(self):
		filename = os.path.abspath(__file__ + '/../../../fixtures/linux/proc.mounts')
		self.mounts = mount._Mounts(filename)
	
	
	def test_getitem_by_unique_device(self):
		assert 'sunrpc' in self.mounts
		sunrpc = self.mounts['sunrpc']
		assert sunrpc.mpoint == '/var/lib/nfs/rpc_pipefs'
		assert sunrpc.fstype == 'rpc_pipefs'
		assert sunrpc.options == 'rw,relatime'
		assert sunrpc.dump == '0'
		assert sunrpc.fsck_order == '0'

	
	def test_getitem_by_unique_mpoint(self):
		assert '/sys/fs/cgroup/freezer' in self.mounts
		cgroup = self.mounts['/sys/fs/cgroup/freezer']
		assert cgroup.device == 'cgroup'

	
	@raises(KeyError)
	def test_getitem_no_device_or_mpoint(self):
		assert '/dev/sda2' not in self.mounts
		self.mounts['/dev/sda2']

	
	def test_getitem_device_not_unique(self):
		assert 'cgroup' in self.mounts
		cgroup = self.mounts['cgroup']
		assert cgroup.mpoint == '/sys/fs/cgroup/systemd', 'First matched line'

