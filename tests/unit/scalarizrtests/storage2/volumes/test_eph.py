import unittest

import mock

from scalarizr.storage2.volumes import eph

volume = {
	'snap_backend': {
		'path': 's3://scalr-3414-ap-northeast-1/data-bundles/5071/percona'
	},
	'vg': 'percona',
	'fs_created': True,
	'fstype': 'xfs',
	'lvm_group_cfg': None,
	'mpoint': '/mnt/dbstorage',
	'device': '/dev/mapper/percona-data',
	'disk': {
		'fs_created': None,
		'fstype': None,
		'mpoint': None,
		'device': '/dev/sda2',
		'type': 'base',
		'id': 'base-vol-1fdb2bf0'
	},
	'type': 'eph',
	'id': 'eph-vol-2a3bd1c8',
	'size': '80%'
}

snapshot = {
	'snap_backend>' : {
		'path': 's3://scalr-3414-ap-northeast-1/data-bundles/5071/percona'
	},
	'vg': 'percona',
	'description': 'MySQL data bundle (farm: 5071 role: percona64-centos6)',
	'snap_strategy': 'data',
	'fs_created': True,
	'fstype': 'xfs',
	'lvm_group_cfg': None,
	'mpoint': '/mnt/dbstorage',
	'device': '/dev/mapper/percona-data',
	'path': 's3://scalr-3414-ap-northeast-1/data-bundles/5071/percona/eph-snap-fe9ac9ce.manifest.ini',
	'disk': {
		'fs_created': None,
		'fstype': None,
		'mpoint': None,
		'device': '/dev/sda2',
		'type': 'base',
		'id': 'base-vol-1fdb2bf0'
	},
	'type': 'eph',
	'id': 'eph-snap-fe9ac9ce',
}

@mock.patch('scalarizr.storage2.volumes.eph.storage2')
class EphVolumeTest(unittest.TestCase):


	def test_10_compatible(self, storage2):
		vol = eph.EphVolume(**volume)
		self.assertFalse(hasattr(vol, 'snap_backend'))
		self.assertEqual(vol.cloudfs_dir, volume['snap_backend']['path'] + '/')


	def test_ensure(self, storage2):
		vol = eph.EphVolume(**volume)
		vol.ensure()

		storage2.volume.assert_called_once_with(
			type='lvm', pvs=[volume['disk']], size='%sVG' % volume['size'],
			vg=volume['vg'], name='data'
		)

		lvm_vol = storage2.volume.return_value
		lvm_vol.ensure.assert_called_once_with()
		self.assertEqual(lvm_vol.device, vol.device)

	@mock.patch('scalarizr.storage2.volumes.eph.os.rmdir')
	@mock.patch('scalarizr.storage2.volumes.eph.tempfile')
	@mock.patch('scalarizr.storage2.volumes.eph.cloudfs')
	@mock.patch('scalarizr.storage2.volumes.eph.EphVolume.mkfs')
	def test_ensure_from_snap(self, mkfs, cfs, tfile, rmdir, storage2):
		# Normal snapshot
		disk = mock.MagicMock()
		vol = eph.EphVolume(type='eph', snap=snapshot, size='80%',
							vg='mongo', disk=disk)

		tmp_mpoint = 'test_temp'
		tfile.mkdtemp.return_value = tmp_mpoint
		with mock.patch.multiple(vol, mount=mock.DEFAULT, umount=mock.DEFAULT):
			vol.ensure()
			vol.mount.assert_called_once_with()
			vol.umount.assert_called_once_with()


		storage2.snapshot.assert_called_once_with(snapshot)
		snap = storage2.snapshot.return_value
		storage2.volume.assert_called_once_with(
			pvs=[disk], size='80%VG', vg='mongo', name='data', type='lvm'
		)
		lvm_vol = storage2.volume.return_value
		lvm_vol.ensure.assert_called_once_with()
		self.assertEqual(lvm_vol.device, vol.device)
		mkfs.assert_called_once_with()

		tfile.mkdtemp.assert_called_once_with()

		cfs.LargeTransfer.assert_called_once_with(snap.path, tmp_mpoint + '/')
		tr = cfs.LargeTransfer.return_value

		tr.run.assert_called_once
		rmdir.assert_called_once_with(tmp_mpoint)

		self.assertEqual(vol.mpoint, None)

		# TODO:

		# mpoint

		# Transfer raises exception - cleanup check


	def test_detach(self, storage2):
		vol = eph.EphVolume(**volume)
		vol.ensure()

		with mock.patch.object(vol, 'umount') as um:
			vol.detach(force=True)
			um.assert_called_once_with()
			vol._lvm_volume.detach.assert_called_once_with(force=True)


	def test_destroy(self, storage2):
		vol = eph.EphVolume(**volume)
		vol.ensure()

		with mock.patch.object(vol, 'umount') as um:
			vol.destroy(force=True)
			um.assert_called_once_with()
			vol._lvm_volume.destroy.assert_called_once_with(force=True)

		assert vol.device is None


	def test_snapshot(self, storage2):
		vol = eph.EphVolume(**volume)
		vol.ensure()
		#vol.snapshot()
		# TODO check size



	def test_destroy_snapshot(self, storage2):
		pass


class EphSnapshotTest(unittest.TestCase):

	def test_destroy(self):
		pass
