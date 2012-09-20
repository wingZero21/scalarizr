
import os
import shutil
import glob

import mock
from nose.tools import raises

from scalarizr import storage2
from scalarizr.linux import coreutils
from scalarizr.services import backup
__node__mock = mock.patch.dict('scalarizr.node.__node__', {
				'behavior': ['percona'],
				'percona': {
					'root_user': 'scalr',
					'root_password': 'abc'
				}})
__node__mock.start()
from scalarizr.services import mysql2


@mock.patch('scalarizr.storage2.volume')
@mock.patch('scalarizr.linux.coreutils.sync')
class TestMySQLSnapBackupAndRestore(object):

	def setup(self):
		self.bak = backup.backup(type='snap_mysql')
		mock.patch.object(self.bak, '_client').start()
		self.bak._client.return_value.master_status.return_value = ('binlog.000003', '107')

		self.rst = backup.restore(type='snap_mysql')


	def test_events_subscribed(self, *args, **kwds):
		assert self.bak._listeners.get('freeze')
		assert self.bak._listeners.get('error')
		assert self.bak._listeners.get('complete')
		assert self.rst._listeners.get('complete')


	def test_correct_type(self, *args, **kwds):
		assert type(self.bak) == mysql2.MySQLSnapBackup
		assert type(self.rst) == mysql2.MySQLSnapRestore


	def test_freeze(self, *args, **kwds):
		state = {}
		self.bak.freeze(mock.Mock(), state)

		self.bak._client.return_value.lock_tables.assert_called_with()
		self.bak._client.return_value.master_status.assert_called_with()
		assert state == {'log_file': 'binlog.000003', 'log_pos': '107'}


	def test_unfreeze(self, *args, **kwds):
		self.bak.unfreeze()
		
		self.bak._client.return_value.unlock_tables.assert_called_with()


	def test_rst_complete(self, *args, **kwds):
		self.rst.complete(mock.Mock())

		vol_factory = args[1]
		vol_factory.return_value.mount.assert_called_with()


@mock.patch.object(mysql2, 'my_print_defaults',
				return_value={'datadir': '/mnt/dbstorage/mysql-data', 
							'log_bin': '/mnt/dbstorage/mysql-misc/binlog'})
@mock.patch.object(mysql2, 'innobackupex')
class TestXtrabackupBackup(object):

	@raises(AssertionError)
	def test_run_invalid_backup_type(self, *args):
		bak = backup.backup(
				type='xtrabackup',
				backup_type=None)
		bak.run()

	@mock.patch.object(os.path, 'exists', return_value=False)
	@mock.patch.object(os, 'makedirs')
	def test_run_full(self, md, ex, innobackupex, *args):
		bak = backup.backup(
				type='xtrabackup')
		self._patch_bak(bak)
		rst = bak.run()
		
		assert rst.type == 'xtrabackup'
		assert rst.backup_type == 'full'
		assert rst.log_file == 'binlog.000003'
		assert rst.log_pos == '107'
		assert rst.from_lsn == '0'
		assert rst.to_lsn == '53201'
		innobackupex.assert_called_with(bak.backup_dir, 
						user=mock.ANY, password=mock.ANY)
		bak._checkpoints.assert_called_with()
		bak._binlog_info.assert_called_with()


	@mock.patch.object(os.path, 'exists', return_value=False)
	@mock.patch.object(os, 'makedirs')
	def test_run_incremental(self, md, ex, innobackupex, *args):
		bak = backup.backup(
					type='xtrabackup',
					backup_type='incremental',					
					from_lsn='23146')
		self._patch_bak(bak)
		rst = bak.run()

		assert rst.type == 'xtrabackup'
		assert rst.backup_type == 'incremental'
		assert rst.log_file == 'binlog.000003'
		assert rst.log_pos == '107'
		assert rst.from_lsn == '0'
		assert rst.to_lsn == '53201'
		innobackupex.assert_called_with(bak.backup_dir, 
						incremental=True, incremental_lsn='23146',
						user=mock.ANY, password=mock.ANY)
		bak._checkpoints.assert_called_with()
		bak._binlog_info.assert_called_with()


	def _patch_bak(self, bak):
		mock.patch.object(bak, '_mysql_init')
		mock.patch.object(bak, '_checkpoints', 
					return_value={'to_lsn': '53201', 'from_lsn': '0'}).start()
		mock.patch.object(bak, '_binlog_info', 
					return_value=('binlog.000003', '107')).start()


	@mock.patch.object(storage2, 'volume')
	@mock.patch.object(os.path, 'exists', return_value=False)
	@mock.patch.object(os, 'makedirs')
	def test_run_with_volume(self, md, ex, st2vol, innobackupex, *args):
		ebs = mock.Mock(
			id='vol-12345678',
			size=1,
			zone='us-east-1a',
			**{'volume_state.return_value': 'available',
			   'attachment_state.return_value': 'attaching'}
		)
		bak = backup.backup(
			type='xtrabackup',
			backup_type='incremental',
			from_lsn='23146',
			volume=ebs)
		self._patch_bak(bak)
		st2vol.return_value = ebs
		rst = bak.run()
		st2vol.assert_called_with(ebs)
		md.asert_called_with(bak.backup_dir)
		assert ebs.mpoint == bak.backup_dir
		ebs.detach.assert_called_with()
		ebs.snapshot.assert_called_with('MySQL xtrabackup', None)
		assert rst.volume == ebs
		assert rst.snapshot


	def test_checkpoints(self, *args):
		fixtures_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures', 'services', 'mysql', 'dbbackup',
		                                              '2012-09-18_09-06-49'))
		bak = backup.backup(
			type='xtrabackup')
		mock.patch.object(bak, '_latest_backup_dir', return_value=fixtures_path).start()
		assert bak._checkpoints() == {'backup_type': 'full-backuped', 'to_lsn': '1597945', 'last_lsn': '1597945', 'from_lsn': '0'}
		bak._latest_backup_dir.assert_called_once()


	def test_binlog_info(self, *args):
		fixtures_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures', 'services', 'mysql', 'dbbackup',
		                                              '2012-09-18_09-06-49'))
		bak = backup.backup(
			type='xtrabackup')
		mock.patch.object(bak, '_latest_backup_dir', return_value=fixtures_path).start()
		assert bak._binlog_info() == ['binlog.000009', '192']
		bak._latest_backup_dir.assert_called_once()


	def test_latest_backup_dir(self, *args):
		fixtures_path = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', '..', 'fixtures', 'services', 'mysql', 'dbbackup'))
		bak = backup.backup(
			type='xtrabackup')
		bak.backup_dir = fixtures_path
		assert bak._latest_backup_dir() == os.path.join(fixtures_path, '2012-09-18_09-06-49')


@mock.patch.object(mysql2, 'my_print_defaults',
				return_value={'datadir': '/mnt/dbstorage/mysql-data', 
							'log_bin': '/mnt/dbstorage/mysql-misc/binlog'})
class TestXtrabackupRestore(object):

	@mock.patch.object(os, 'listdir',
	                   return_value=['2012-09-16_11-54', '2012-09-15_18-06'])
	@mock.patch.object(coreutils, 'chown_r')
	@mock.patch.object(mysql2, 'innobackupex')
	@mock.patch.object(storage2, 'snapshot')
	@mock.patch.object(storage2, 'volume')
	def test_tmp_volume_creation_only_snapshot(self, ec2volume, ec2snapshot, *args):
		snapshot = mock.Mock(id='vol-123456ab', type='base')
		ebs = mock.Mock(
			id='vol-12345678',
			size=1,
			zone='us-east-1a',
			**{'volume_state.return_value': 'available',
			   'attachment_state.return_value': 'attaching'}
		)
		ec2volume.return_value = ebs
		ec2snapshot.return_value = snapshot
		rst = backup.restore(type='xtrabackup', snapshot=snapshot)
		mock.patch.object(rst, '_mysql_init').start()
		mock.patch.object(rst, '_start_copyback').start()
		mock.patch.object(rst, '_commit_copyback').start()
		mock.patch.object(rst, '_rollback_copyback').start()
		res = rst.run()
		ec2volume.assert_called_with(snap=snapshot, type=snapshot.type)
		ebs.destroy.assert_called_once_with()


	@mock.patch.object(os, 'listdir',
	                   return_value=['2012-09-16_11-54', '2012-09-15_18-06'])
	@mock.patch.object(coreutils, 'chown_r')
	@mock.patch.object(mysql2, 'innobackupex')
	@mock.patch.object(storage2, 'snapshot')
	@mock.patch.object(storage2, 'volume')
	def test_tmp_volume_creation_both_volume_and_snapshot(self, ec2volume, ec2snapshot, *args):
		snapshot = mock.Mock(id='vol-123456ab', type='base')
		ebs = mock.Mock(
			id='vol-12345678',
			size=1,
			zone='us-east-1a',
			**{'volume_state.return_value': 'available',
			   'attachment_state.return_value': 'attaching'}
		)
		ec2volume.return_value = ebs
		ec2snapshot.return_value = snapshot
		rst = backup.restore(type='xtrabackup', snapshot=snapshot, volume=ebs)
		mock.patch.object(rst, '_mysql_init').start()
		mock.patch.object(rst, '_start_copyback').start()
		mock.patch.object(rst, '_commit_copyback').start()
		mock.patch.object(rst, '_rollback_copyback').start()
		res = rst.run()
		ebs.config.assert_called_once_with()
		ec2volume.assert_called_with(ebs.config())
		ebs.destroy.assert_called_once_with()


	@mock.patch.object(os, 'listdir', 
				return_value=['2012-09-16_11-54', '2012-09-15_18-06'])
	@mock.patch.object(coreutils, 'chown_r')
	@mock.patch.object(mysql2, 'innobackupex')
	def test_run(self, innobackupex, chown_r, *args):
		rst = backup.restore(type='xtrabackup')
		mock.patch.object(rst, '_mysql_init').start()
		mock.patch.object(rst, '_start_copyback').start()
		mock.patch.object(rst, '_commit_copyback').start()
		mock.patch.object(rst, '_rollback_copyback').start()
		rst.run()

		rst._mysql_init.stop.assert_called_with()
		calls = innobackupex.call_args_list
		# Prepare base
		assert calls[0] == ((os.path.join(rst.backup_dir, '2012-09-15_18-06'), ), 
				dict(apply_log=True, redo_only=True, user=mock.ANY, password=mock.ANY))
		# Prepare inc
		assert calls[1] == ((os.path.join(rst.backup_dir, '2012-09-15_18-06'), ), 
				dict(incremental_dir=os.path.join(rst.backup_dir, '2012-09-16_11-54'), 
					apply_log=True, redo_only=True, 
					user=mock.ANY, password=mock.ANY))
		# Prepare full
		assert calls[2] == ((os.path.join(rst.backup_dir, '2012-09-15_18-06'), ), 
				dict(apply_log=True, user=mock.ANY, password=mock.ANY))
		chown_r.assert_called_with(rst._data_dir, 'mysql', 'mysql')
		rst._mysql_init.start.assert_called_with()


	@mock.patch.object(os, 'listdir',
	                   return_value=['2012-09-16_11-54', '2012-09-15_18-06'])
	@mock.patch.object(coreutils, 'chown_r')
	@mock.patch.object(mysql2, 'innobackupex')
	@mock.patch.object(glob, 'glob')
	@mock.patch.object(shutil, 'rmtree')
	@mock.patch.object(os, 'remove')
	@mock.patch.object(os, 'makedirs')
	@mock.patch.object(os, 'rename')
	def test_copyback_start_commit(self, rename, makedirs, remove, rmtree, pglob, *args):
		def glob_returns(*args):
			if '.bak' in args[0]:
				return ['/mnt/dbstorage/mysql-misc/logbin.index.bak', '/mnt/dbstorage/mysql-misc/logbin.000001.bak']
			return ['/mnt/dbstorage/mysql-misc/logbin.index', '/mnt/dbstorage/mysql-misc/logbin.000001']
		rst = backup.restore(type='xtrabackup')
		mock.patch.object(rst, '_mysql_init').start()
		rollback = mock.patch.object(rst, '_rollback_copyback').start()
		pglob.side_effect = glob_returns
		rst.run()
		assert not rollback.call_count
		rename_calls = [mock.call(rst._data_dir, rst._data_dir+'.bak'),
		                mock.call('/mnt/dbstorage/mysql-misc/logbin.index', '/mnt/dbstorage/mysql-misc/logbin.index.bak'),
		                mock.call('/mnt/dbstorage/mysql-misc/logbin.000001', '/mnt/dbstorage/mysql-misc/logbin.000001.bak')]
		rename.assert_has_calls(rename_calls)
		makedirs.assert_called_once_with(rst._data_dir)
		rmtree.assert_called_with(rst._data_dir + '.bak')
		remove_calls = [mock.call('/mnt/dbstorage/mysql-misc/logbin.index.bak'),
						mock.call('/mnt/dbstorage/mysql-misc/logbin.000001.bak')]
		remove.assert_has_calls(remove_calls)


	@mock.patch.object(os, 'listdir',
	                   return_value=['2012-09-16_11-54', '2012-09-15_18-06'])
	@mock.patch.object(coreutils, 'chown_r')
	@mock.patch.object(mysql2, 'innobackupex')
	@mock.patch.object(glob, 'glob')
	@mock.patch.object(shutil, 'rmtree')
	@mock.patch.object(os, 'remove')
	@mock.patch.object(os, 'makedirs')
	@mock.patch.object(os, 'rename')
	def test_copyback_start_rollback(self, rename, makedirs, remove, rmtree, pglob, *args):
		def glob_returns(*args, **kwargs):
			if '.bak' in args[0]:
				return ['/mnt/dbstorage/mysql-misc/logbin.index.bak', '/mnt/dbstorage/mysql-misc/logbin.000001.bak']
			return ['/mnt/dbstorage/mysql-misc/logbin.index', '/mnt/dbstorage/mysql-misc/logbin.000001']
		rst = backup.restore(type='xtrabackup')
		mock.patch.object(rst._mysql_init, 'start', side_effect=Exception('Test')).start()
		mock.patch.object(rst, '_commit_copyback').start()
		pglob.side_effect = glob_returns
		try:
			rst.run()
		except:
			pass
		rename_calls = [mock.call(rst._data_dir, rst._data_dir+'.bak'),
		                mock.call('/mnt/dbstorage/mysql-misc/logbin.index', '/mnt/dbstorage/mysql-misc/logbin.index.bak'),
		                mock.call('/mnt/dbstorage/mysql-misc/logbin.000001', '/mnt/dbstorage/mysql-misc/logbin.000001.bak'),
		                mock.call(rst._data_dir+'.bak', rst._data_dir),
		                mock.call('/mnt/dbstorage/mysql-misc/logbin.index.bak', '/mnt/dbstorage/mysql-misc/logbin.index'),
		                mock.call('/mnt/dbstorage/mysql-misc/logbin.000001.bak', '/mnt/dbstorage/mysql-misc/logbin.000001')
		                ]
		rename.assert_has_calls(rename_calls)
		makedirs.assert_called_once_with(rst._data_dir)

