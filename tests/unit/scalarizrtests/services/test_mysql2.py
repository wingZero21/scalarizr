
import sys

import mock

from scalarizr.services import backup

@mock.patch('scalarizr.storage2.volume')
@mock.patch('scalarizr.linux.coreutils.sync')
class TestMySQLSnapBackupAndRestore(object):
	@classmethod
	def setup_class(cls):
		cls.patcher = mock.patch.dict('scalarizr.node.__node__', 
						{'percona': {}, 'behavior': ['percona']})
		cls.patcher.start()
		from scalarizr.services import mysql2
		cls.mysql2 = mysql2


	@classmethod
	def teardown_class(cls):
		cls.patcher.stop()


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
		assert type(self.bak) == self.mysql2.MySQLSnapBackup
		assert type(self.rst) == self.mysql2.MySQLSnapRestore


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


