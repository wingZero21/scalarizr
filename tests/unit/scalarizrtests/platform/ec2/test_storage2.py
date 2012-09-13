'''
Created on Sep 4, 2012

@author: marat
'''

import mock
from nose.tools import raises

from scalarizr.platform.ec2 import storage2 as ec2storage
from scalarizr.linux import coreutils
#from scalarizr.config import STATE, State


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
	@mock.patch('scalarizr.platform.ec2.storage2.STATE', new_callable=mock.MagicMock)
	def test_acquire_t1micro(self, state, glob):
		state.get.return_value = ['/dev/sdg', '/dev/sdf']
		letter = self.mgr.__enter__().get()
		assert letter not in ('g', 'f')
		state.get.assert_called_once_with('ec2.t1micro_detached', [])


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
	
	
	def test_ensure_new(self):
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


	def test_ensure_existed(self):
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
		ebs = mock.Mock(
			id='vol-12345678', 
			size=1, 
			zone='us-east-1a',
			
			**{'volume_state.return_value': 'available',
				'attachment_state.return_value': 'attached'}
		)
		
		with mock.patch.object(self.conn, 'get_all_volumes', return_value=[ebs]):
			with mock.patch.object(Ebs, '_attach_volume'):
				with mock.patch.object(Ebs, '_detach_volume'):
					self.vol = Ebs(type='ebs', id='vol-12345678')
					self.vol.ensure()
					assert self.vol.id == ebs.id
					self.vol._detach_volume.assert_called_once_with(ebs)
					self.vol._attach_volume.assert_called_once_with(ebs, '/dev/sdb')
				

	def test_ensure_existed_attaching_to_other_instance(self):
		ebs = mock.Mock(
			id='vol-12345678', 
			size=1, 
			zone='us-east-1a',
			
			**{'volume_state.return_value': 'available',
				'attachment_state.return_value': 'attaching'}
		)
		
		with mock.patch.object(self.conn, 'get_all_volumes', return_value=[ebs]):
			with mock.patch.object(Ebs, '_attach_volume'):
				with mock.patch.object(Ebs, '_detach_volume'):
					with mock.patch.object(Ebs, '_wait_attachment_state_change'):
						self.vol = Ebs(type='ebs', id='vol-12345678')
						self.vol.ensure()
						assert self.vol.id == ebs.id
						assert self.vol._detach_volume.call_count == 0
						self.vol._attach_volume.assert_called_once_with(ebs, '/dev/sdb')

	
	def test_ensure_restore(self):
		with mock.patch.object(self.conn, 'get_all_volumes', return_value=[]):
			with mock.patch.object(Ebs, '_attach_volume'):
				with mock.patch.object(Ebs, '_create_volume'):
					with mock.patch.object(Ebs, '_dictify'):
						snap_config = {'id':'snap-12345678', 'tags':{'tag1':'1'}}
						size=1
						zone='us-east-1a'
						self.vol = Ebs(id=None, size=size, zone=zone, snap=snap_config)
						self.vol.ensure()
						self.vol._create_volume.assert_called_once_with(
							zone=zone, 
							size=size, 
							snapshot=snap_config['id'],
							tags=snap_config['tags'],
							volume_type=None, 
							iops=None)


	def test_snapshot(self):
		with mock.patch.object(Ebs, '_create_snapshot'):
			vol_id='vol-12345678'
			description='test'
			tags={'tag1':'1'}
			kwds = {'nowait' : True}
			self.vol = Ebs(type='ebs', id=vol_id)
			snap = self.vol.snapshot(description=description, tags=tags, **kwds)
			self.vol._create_snapshot.assert_called_once_with(vol_id, description, tags, kwds['nowait'])
		
			
	def test_snapshot_tags(self):
		snapshot = mock.Mock(id='snap-12345678')
		with mock.patch.object(self.conn, 'create_snapshot', return_value=snapshot):
			with mock.patch.object(self.conn, 'create_tags'):
				with mock.patch.object(coreutils, 'sync'):
					vol_id='vol-12345678'
					description='test'
					tags={'tag1':'1'}
					kwds = {'nowait' : True}
					self.vol = Ebs(type='ebs', id=vol_id)
					snap = self.vol.snapshot(description=description, tags=tags, **kwds)
					self.conn.create_tags.assert_called_once_with([snapshot.id], tags)
			
					
	def test_snapshot_nowait(self):
		snapshot = mock.Mock(id='snap-12345678')
		with mock.patch.object(self.conn, 'create_snapshot', return_value=snapshot):
			with mock.patch.object(self.conn, 'create_tags'): 
				with mock.patch.object(Ebs, '_wait_snapshot'):
					vol_id='vol-12345678'
					description='test'
					tags={'tag1':'1'}
					nowait_kwds = {'nowait' : True}
					wait_kwds = {'nowait' : False}
					self.vol = Ebs(type='ebs', id=vol_id)
					snap = self.vol.snapshot(description=description, tags=tags, **nowait_kwds)
					self.vol._wait_snapshot.call_count == 0
					snap = self.vol.snapshot(description=description, tags=tags, **wait_kwds)
					self.vol._wait_snapshot.assert_called_once_with(snapshot)

	@raises(AssertionError)
	def test_snapshot_no_connection(self):
		with mock.patch.object(Ebs, '_create_snapshot'):
			with mock.patch.object(Ebs, '_connect_ec2', return_value=None): 
				vol_id='vol-12345678'
				description='test'
				tags={'tag1':'1'}
				kwds = {'nowait' : True}
				self.vol = Ebs(type='ebs', id=vol_id)
				snap = self.vol.snapshot(description=description, tags=tags, **kwds)


	def test_detach(self):
		ebs = mock.Mock(
		id='vol-12345678', 
		size=1, 
		zone='us-east-1a',
		
		**{'detach.return_value': None,
			'update.return_value': 'available'}
				)
		
		with mock.patch.object(Ebs, '_ebs_volume', return_value=ebs):
			self.vol = Ebs(type='ebs', id='vol-12345678')
			self.vol.detach()
			ebs.detach.call_count == 1
			ebs.update.call_count == 1
			
			
	def test_detach_t1micro(self):
		ebs = mock.Mock(
		id='vol-12345678', 
		size=1, 
		zone='us-east-1a',
		
		**{'detach.return_value': None,
			'update.return_value': 'available'}
				)
		with mock.patch('scalarizr.platform.ec2.storage2.STATE', new_callable=dict):
			with mock.patch.object(Ebs, '_ebs_volume', return_value=ebs):
				with mock.patch.object(Ebs, '_detach_volume'):
					with mock.patch.object(Ebs, '_instance_type', return_value='t1.micro'):
						self.vol = Ebs(name='/dev/sdf', type='ebs', id='vol-12345678')
						self.vol._detach(True)
						from scalarizr.platform.ec2.storage2 import STATE
						assert STATE['ec2.t1micro_detached'] == [self.vol.name,]
	

	@raises(AssertionError)
	def test_detach_no_connection(self):
		with mock.patch.object(Ebs, '_detach_volume'):
			with mock.patch.object(Ebs, '_connect_ec2', return_value=None): 
				vol_id='vol-12345678'
				description='test'
				tags={'tag1':'1'}
				kwds = {'nowait' : True}
				self.vol = Ebs(type='ebs', id=vol_id)
				snap = self.vol._detach(False)	
	
	
	def test_destroy(self):
		with mock.patch.object(self.conn, 'delete_volume'): 
			vol_id='vol-12345678'
			self.vol = Ebs(type='ebs', id=vol_id)
			snap = self.vol.destroy(True)
			self.conn.delete_volume.assert_called_once_with(self.vol.id)
		
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


	