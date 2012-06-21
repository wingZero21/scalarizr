'''
Created on Jun 19, 2012

@author: marat
'''

from scalarizr.api import storage as storage_api

import mock
from nose.tools import assert_equal, assert_true, assert_raises


sample_volume_config = dict(type='loop', size=0.01)
sample_volume_config_w_id = dict(type='loop', size=0.01, file='/mnt/myfile', id='loop-vol-asdfasdf')
sample_snapshot_config = dict(snapshot=dict(type='loop', file='/tmp/file', size=0.01))


class TestStorageAPI(object):
	
	@classmethod
	def setup_class(cls):
		thread_patcher = mock.patch('scalarizr.api.storage.threading.Thread')
		Thread = thread_patcher.start()

		def thread_side_effect(*args, **kwargs):
			target = kwargs.get('target')
			ret = mock.MagicMock()
			ret.start.side_effect = lambda: target()
			return ret

		Thread.side_effect = thread_side_effect

	
	def setup(self):
		self.storage_patcher = mock.patch('scalarizr.api.storage.storage_lib.Storage')
		self.storage = self.storage_patcher.start()
		self.sample_vol = mock.MagicMock()
		self.storage.create.return_value = self.sample_vol

		self.op_patcher = mock.patch('scalarizr.api.storage.handlers.operation')
		self.operation = self.op_patcher.start()

		self.api = storage_api.StorageAPI()


	def teardown(self):
		self.storage_patcher.stop()
		self.op_patcher.stop()


	""" Test methods """


	def test_create(self):
		self.storage.create.return_value = self.sample_vol
		ret = self.api.create(volume_config=sample_volume_config)
		self.storage.create.assert_called_once_with(**sample_volume_config)
		assert_equal(ret, self.sample_vol.config())
		# No operation were defined
		assert_equal(self.operation.mock_calls, [])


	def test_create_async(self):
		assert_raises(AssertionError, self.api.create)
		assert_raises(AssertionError, self.api.create,
					  volume_config=sample_volume_config,
					  snapshot_config=sample_snapshot_config)

		op_id = self.api.create(volume_config=sample_volume_config, async=True)
		self.storage.create.assert_called_once_with(**sample_volume_config)

		txt = 'Create volume'
		self.operation.assert_called_once_with(name=txt)
		op = self.operation.return_value
		assert_equal(op_id, op.id)
		op.define.assert_called_once_with()
		op.phase.assert_called_once_with(txt)
		op.step.assert_called_once_with(txt)
		op.ok.assert_called_once_with(data=self.sample_vol.config())

		self.api.create(snapshot_config=sample_volume_config)
		self.storage.create.assert_called_with(snapshot=sample_volume_config)


	def test_status(self):
		self.api.status(volume_config=sample_volume_config)
		self.storage.create.assert_called_once_with(sample_volume_config)
		self.sample_vol.status.assert_called_once_with()


	def test_snapshot(self):
		description = 'my_very_randomly_named_snapshot'
		ret = self.api.snapshot(volume_config=sample_volume_config, description=description)

		self.storage.create.assert_called_once_with(sample_volume_config)
		self.sample_vol.snapshot.assert_called_once_with(description)
		self.sample_vol.snapshot.return_value.config.assert_called_once_with()
		assert_equal(ret, self.sample_vol.snapshot.return_value.config.return_value)


	def test_snapshot_async(self):
		txt = 'Create snapshot'
		description = 'oh_my_beloved_backup'

		op = self.operation.return_value
		op_id = self.api.snapshot(volume_config=sample_volume_config, description=description, async=True)
		self.operation.assert_called_once_with(name=txt)
		op.define.assert_called_once_with()
		op.phase.assert_called_once_with(txt)
		op.step.assert_called_once_with(txt)
		op.ok.assert_called_once_with(data=self.sample_vol.snapshot.return_value.config.return_value)

		assert_equal(op_id, op.id)

		self.storage.create.assert_called_once_with(sample_volume_config)
		self.sample_vol.snapshot.assert_called_once_with(description)
		self.sample_vol.snapshot.return_value.config.assert_called_once_with()


	def test_detach(self):
		assert_raises(AssertionError, self.api.detach, volume_config=sample_volume_config)

		vol_cfg = self.api.detach(volume_config=sample_volume_config_w_id)
		self.storage.create.assert_called_once_with(sample_volume_config_w_id)
		self.sample_vol.detach.assert_called_once_with()
		assert_equal(vol_cfg, self.sample_vol.config.return_value)


	def test_detach_async(self):
		txt = 'Detach volume'
		assert_raises(AssertionError, self.api.detach, volume_config=sample_volume_config, async=True)

		op_id = self.api.detach(volume_config=sample_volume_config_w_id, async=True)

		self.storage.create.assert_called_once_with(sample_volume_config_w_id)
		self.sample_vol.detach.assert_called_once_with()

		self.operation.assert_called_once_with(name=txt)
		op = self.operation.return_value
		op.define.assert_called_once_with()
		op.phase.assert_called_once_with(txt)
		op.step.assert_called_once_with(txt)
		op.ok.assert_called_once_with(data=self.sample_vol.config.return_value)

		self.sample_vol.detach.assert_called_once_with()


	def test_destroy(self):
		assert_raises(AssertionError, self.api.detach, volume_config=sample_volume_config)
		res = self.api.destroy(volume_config=sample_volume_config_w_id)
		assert_equal(res, None)
		self.storage.create.assert_called_once_with(sample_volume_config_w_id)
		self.sample_vol.destroy.assert_called_once_with(remove_disks=False)

		res = self.api.destroy(volume_config=sample_volume_config_w_id, destroy_disks=True)
		assert_equal(res, None)
		self.storage.create.assert_called_with(sample_volume_config_w_id)
		self.sample_vol.destroy.assert_called_with(remove_disks=True)


	def test_destroy_async(self):
		txt = 'Destroy volume'
		assert_raises(AssertionError, self.api.destroy, volume_config=sample_volume_config, async=True)

		op_id = self.api.destroy(volume_config=sample_volume_config_w_id, async=True)
		self.storage.create.assert_called_once_with(sample_volume_config_w_id)
		self.sample_vol.destroy.assert_called_once_with(remove_disks=False)

		self.operation.assert_called_once_with(name=txt)
		op = self.operation.return_value
		assert_equal(op_id, op.id)
		op.define.assert_called_once_with()
		op.phase.assert_called_once_with(txt)
		op.step.assert_called_once_with(txt)
		op.ok.assert_called_once_with()

		self.operation.reset_mock()
		self.sample_vol.reset_mock()
		self.storage.reset_mock()
		assert_raises(AssertionError, self.api.destroy, volume_config=sample_volume_config,
					  											destroy_disks=True, async=True)

		op_id = self.api.destroy(volume_config=sample_volume_config_w_id, destroy_disks=True, async=True)
		self.storage.create.assert_called_once_with(sample_volume_config_w_id)
		self.sample_vol.destroy.assert_called_once_with(remove_disks=True)

		self.operation.assert_called_once_with(name=txt)
		op = self.operation.return_value
		assert_equal(op_id, op.id)
		op.define.assert_called_once_with()
		op.phase.assert_called_once_with(txt)
		op.step.assert_called_once_with(txt)
		op.ok.assert_called_once_with()


	def test_replace_raid_disk(self):
		pass


	def test_replace_raid_disk_async(self):
		pass











		

