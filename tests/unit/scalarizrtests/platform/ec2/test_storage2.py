'''
Created on Sep 4, 2012

@author: marat
'''

import mock

from scalarizr.platform.ec2 import storage2 as ec2storage


def test_name2device():
	pass


@mock.patch('os.path.exists', return_value=True)
def test_name2device_xen(*args):
	device = ec2storage.name2device('/dev/sda1')
	assert device == '/dev/xvda1'
	

@mock.patch('os.path.exists', return_value=True)
@mock.patch('scalarizr.platform.ec2.storage2.storage2mod')
def test_name2device_rhel_bug(s, exists):
	s.RHEL_DEVICE_ORDERING_BUG = True
	device = ec2storage.name2device('/dev/sda1')
	assert device == '/dev/xvde1'


def test_name2device_device_passed():
	pass

def test_device2name():
	pass

def test_device2name_xen():
	pass

def test_device2name_rhel_bug():
	pass

def test_device2name_name_passed():
	pass


class TestFreeDeviceLetterMgr(object):
	def setup(self):
		self.mgr = ec2storage.FreeDeviceLetterMgr()
		
	@mock.patch('glob.glob')
	@mock.patch('scalarizr.platform.ec2.storage2.STATE')
	def test_acquire(self, state, glob):
		glob_returns = [['/dev/sdf1'], []]
		def globfn(*args, **kwds):
			return glob_returns.pop(0)
		glob.side_effect = globfn
		
		letter = self.mgr.__enter__().get()
		
		glob.assert_called_with('/dev/sd%s*' % letter)
		assert glob.call_count == 2
	
			
	def test_acquire_concurrent(self):
		pass
	
	
	@mock.patch('glob.glob', return_value=[])
	@mock.patch('scalarizr.platform.ec2.storage2.STATE')
	def test_acquire_t1micro(self, state, glob):
		state.__getitem__.return_value = ['/dev/sdg', '/dev/sdf']
		
		letter = self.mgr.__enter__().get()
		
		assert letter not in ('g', 'f')
		state.__getitem__.assert_called_once_with('ec2.t1micro_detached')


Ebs = ec2storage.EbsVolume

class TestEbsVolume(object):
	def setup(self):
		self.conn = mock.Mock()
		
		patchers = []
		po = mock.patch.object; 
		patchers.append(po(Ebs, '_connect_ec2', return_value=self.conn))
		patchers.append(po(Ebs, '_instance_id', return_value='i-12345678'))
		patchers.append(po(Ebs, '_instance_type', return_value='m1.small'))
		patchers.append(po(Ebs, '_avail_zone', return_value='us-east-1a'))
		patchers.append(po(Ebs, '_free_device_letter_mgr', **{'get.return_value' : 'b'}))
		patchers.append(mock.patch('scalarizr.platform.ec2.storage2.name2device', 
								side_effect=lambda name: name.replace('/sd', '/xvd')))															
		self.patchers = patchers
		for p in self.patchers:
			p.start()


	def teardown(self):
		for p in self.patchers:
			p.stop()
	
	
	def _test_ensure_new(self):
		ebs = mock.Mock(
			id='vol-12345678', 
			size=1, 
			zone='us-east-1a', 
			attach_data=mock.Mock(device='/dev/sdb'),
			**{'volume_state.return_value': 'available'}
		)
		
		with mock.patch.object(Ebs, '_create_volume', return_value=ebs):
			with mock.patch.object(Ebs, '_attach_volume'):
				self.vol = Ebs(type='ebs')
				self.vol.ensure()
				
				assert self.vol.id == ebs.id
				assert self.vol.size == ebs.size
				assert self.vol.avail_zone == 'us-east-1a'
				assert self.vol.name == '/dev/sdb'
				assert self.vol.device == '/dev/xvdb'
				assert self.vol.config()


	def _test_ensure_existed(self):
		ebs = mock.Mock(
			id='vol-12345678', 
			size=1, 
			zone='us-east-1a',
			**{'volume_state.return_value': 'available'}
		)
		
		with mock.patch.object(self.conn, 'get_all_volumes', return_value=[ebs]):
			with mock.patch.object(Ebs, '_attach_volume'):
				self.vol = Ebs(type='ebs', id='vol-12345678')
				self.vol.ensure()
				
				assert self.vol.id == ebs.id
				assert self.vol.size == ebs.size
				assert self.vol.avail_zone == 'us-east-1a'
				assert self.vol.name == '/dev/sdb'
				assert self.vol.device == '/dev/xvdb'
				self.conn.get_all_volumes.assert_called_once_with(['vol-12345678'])
				assert self.vol.config()
				

	def test_ensure_existed_in_different_zone(self):
		ebs = mock.Mock(
			id='vol-12345678', 
			size=1, 
			zone='us-east-1b',
			**{'volume_state.return_value': 'available'}
		)
		ebs2 = mock.Mock(
			id='vol-87654321',
			size=1,
			zone='us-east-1a',
			**{'volume_state.return_value': 'available'}
		)
		with mock.patch.object(self.conn, 'get_all_volumes', return_value=[ebs]):
			with mock.patch.object(Ebs, '_create_snapshot', return_value=mock.Mock(id='snap-12345678')):
				with mock.patch.object(Ebs, '_create_volume', return_value=ebs2):
					with mock.patch.object(Ebs, '_attach_volume'):

						self.vol = Ebs(type='ebs', id='vol-12345678')
						self.vol.ensure()
						
						assert self.vol.id == ebs2.id


	def test_ensure_existed_attached_to_other_instance(self):
		pass

	def test_ensure_existed_attaching_to_other_instance(self):
		pass
	
	def test_ensure_restore(self):
		pass

	def test_snapshot(self):
		pass
	
	def test_snapshot_nowait(self):
		pass

	def test_snapshot_no_connection(self):
		pass

	def test_detach(self):
		pass
	
	def test_detach_t1micro(self):
		pass
	
	def test_detach_no_connection(self):
		pass
	
	def test_destroy(self):
		pass
		
	def test_destroy_no_connection(self):
		pass


class TestEbsSnapshot(object):
	def test_status(self):
		pass
	
	def test_status_no_connection(self):
		pass
	
	def test_destroy(self):
		pass


class TestEc2EphemeralVolume(object):
	def test_ensure(self):
		pass
	
	def test_ensure_metadata_server_error(self):
		pass


	