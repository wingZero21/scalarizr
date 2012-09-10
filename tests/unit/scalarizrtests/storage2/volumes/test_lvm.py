'''
Created on Aug 22, 2012

@author: marat
'''

import mock

from scalarizr.storage2.volumes import lvm
from scalarizr.storage2.volumes import base


class TestLvmVolume(object):
	
	@mock.patch('scalarizr.storage2.volumes.lvm.lvm2')
	def test_ensure_new(self, lvm2):
		lvm2.NotFound = Exception
		lvs_returns = [
			lvm2.NotFound, 
			{'data/vol1': mock.Mock(lv_path='/dev/mapper/data-vol1', lv_attr='-wi-ao')}
		]
		def lvs(*args, **kwds):
			ret = lvs_returns.pop(0)
			if isinstance(ret, Exception):
				raise ret
			return ret
		lvm2.lvs.side_effect = lvs
		lvm2.pvs.return_value = {}
		lvm2.vgs.side_effect = lvm2.NotFound
		
		vol = lvm.LvmVolume(
				name='vol1', 
				vg='data', 
				pvs=['/dev/sdb', '/dev/sdc'], 
				size='98%FREE')
		vol.ensure()
		
		assert len(vol.pvs) == 2
		assert isinstance(vol.pvs[0], base.Volume)
		assert isinstance(vol.pvs[1], base.Volume)
		assert vol.device == '/dev/mapper/data-vol1'
		calls = [mock.call('/dev/sdb'),  mock.call('/dev/sdc')]
		lvm2.pvcreate.assert_has_calls(calls)
		lvm2.vgcreate.assert_called_once_with('data', '/dev/sdb', '/dev/sdc')
		lvm2.lvcreate.assert_called_once_with('data', name='vol1', extents='98%FREE')


	@mock.patch('scalarizr.storage2.volumes.lvm.lvm2')
	def test_ensure_new_with_existed_layout(self, lvm2):
		lvm2.NotFound = Exception
		lvs_returns = [
			lvm2.NotFound, 
			{'data/vol1': mock.Mock(lv_path='/dev/mapper/data-vol1', lv_attr='-wi---')}
		]
		def lvs(*args, **kwds):
			ret = lvs_returns.pop(0)
			if isinstance(ret, Exception):
				raise ret
			return ret
		lvm2.lvs.side_effect = lvs		
		lvm2.pvs.return_value = {'/dev/sdb': mock.Mock(), '/dev/sdc': mock.Mock()}

		vol = lvm.LvmVolume(
				name='vol1', 
				vg='data', 
				pvs=['/dev/sdb', '/dev/sdc'], 
				size='98%FREE')
		vol.ensure()

		assert len(vol.pvs) == 2
		assert isinstance(vol.pvs[0], base.Volume)
		assert isinstance(vol.pvs[1], base.Volume)
		assert vol.device == '/dev/mapper/data-vol1'
		lvm2.pvs.assert_called_once_with()
		assert lvm2.pvcreate.call_count == 0
		lvm2.vgs.assert_called_once_with('data')
		lvm2.lvchange.assert_called_once_with(vol.device, available=True)
		
	
	def test_ensure_existed(self):
		pass
	
	def test_ensure_existed_with_changed_layout(self):
		pass
	
	def test_detach(self):
		pass
	
	def test_detach_already_detached(self):
		pass
	
	def test_destroy(self):
		pass
	
	def test_destroy_already_detached(self):
		pass
	
	def test_destroy_already_destroyed(self):
		pass
	
	def test_snapshot(self):
		pass
	
