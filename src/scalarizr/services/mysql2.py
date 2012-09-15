
import os

from scalarizr import linux, storage2
from scalarizr.linux import coreutils, pkgmgr
from scalarizr.node import __node__
from scalarizr.services import mysql as mysql_svc
from scalarizr.services import backup
from scalarizr.libs import cdo


__behavior__ = 'percona' \
			if 'percona' in __node__['behavior'] \
			else 'mysql2'


__mysql__ = __node__[__behavior__]
__mysql__.update({
	'behavior': __behavior__,
	'port': 3306,
	'storage_dir': '/mnt/dbstorage',
	'data_dir': '/mnt/dbstorage/mysql-data',
	'binlog_dir': '/mnt/dbstorage/mysql-misc',
	'tmp_dir': '/mnt/dbstorage/tmp',
	'root_user': 'scalr',
	'repl_user': 'scalr_repl',
	'stat_user': 'scalr_stat',
	'pma_user': 'pma',
	'debian.cnf': '/etc/mysql/debian.cnf',
	'my.cnf': '/etc/my.cnf' if linux.os['family'] in ('RedHat', 'Oracle') else '/etc/mysql/my.cnf',
	#'mysqld_exec': util.try_exec('/usr/sbin/mysqld', '/usr/libexec/mysqld')
	'mysqldump_chunk_size': 200 * 1024 * 1024,
	'stop_slave_timeout': 180,
	'change_master_timeout': 60
})


class MySQLSnapBackup(backup.SnapBackup):
	def __init__(self, **kwds):
		super(MySQLSnapBackup, self).__init__(**kwds)
		self.on(
			freeze=self.freeze,
			complete=self.unfreeze,
			error=self.unfreeze
		)

	def _client(self):
		return mysql_svc.MySQLClient(
					__mysql__['root_user'],
					__mysql__['root_password'])


	def freeze(self, volume, state):
		client = self._client()
		client.lock_tables()
		coreutils.sync()
		(log_file, log_pos) = client.master_status()
		state.update({
			'log_file': log_file,
			'log_pos': log_pos
		})


	def unfreeze(self, *args):
		client = self._client()
		client.unlock_tables()

		
class MySQLSnapRestore(backup.SnapRestore):
	def __init__(self, **kwds):
		super(MySQLSnapRestore, self).__init__(**kwds)
		self.on(complete=self.complete)

	def complete(self, volume):
		vol = storage2.volume(volume)
		vol.mpoint = __mysql__['storage_dir']
		vol.mount()


backup.backup_types['snap_mysql'] = MySQLSnapBackup
backup.restore_types['snap_mysql'] = MySQLSnapRestore


class XtrabackupBackup(backup.Backup):
	default_config = backup.Backup.default_config.copy()
	default_config.update({
		'backup_type': 'full',	
		# Allowed: full | incremental
		'from_lsn': None,
		# Allowed: int. Log sequence number to start from
		'backup_dir': '/mnt/dbbackup'
		# Directory to store backup files
		'backup_volume': None
		# Volume to ensure and mount to 'backup_dir'.
		# After backup completion it will be snapshoted and
		# snapshot will be available in Restore configuration
	})

	
	def _run(self):
		if self.backup_volume:
			self.backup_volume = storage2.volume(self.backup_volume)
			self.backup_volume.mpoint = self.backup_dir
			self.backup_volume.ensure(mount=True)
		else:
			os.makedirs(self.backup_dir)

		kwds = {}
		if self.backup_type == 'incremental':
			from_lsn = self.from_lsn
			if not from_lsn:
				dir_ = self._latest_backup_dir()
				# TODO: find LSN in backup_dir
				from_lsn = None
			kwds.update({
				'incremental': True,
				'incremental_lsn': from_lsn
			})

		try:
			innobackupex(self.backup_dir, 
					user=__mysql__['root_user'], 
					password=__mysql__['root_password'],
					**kwds)
			# TODO: find binary file/pos
			log_file = log_pos = None
			# TODO: find from_lsn/to_lsn
			from_lsn = to_lsn = None
		finally:
			if self.backup_volume:
				try:
					self.backup_volume.detach()
				except:
					msg = 'Failed to detach backup volume: %s'
					LOG.warn(msg, sys.exc_info()[1])

		return backup.restore(
				type='xtrabackup', 
				log_file=log_file, 
				log_pos=log_pos,
				from_lsn=from_lsn,
				to_lsn=to_lsn,
				backup_type=self.backup_type,
				backup_dir=self.backup_dir,
				backup_volume=self.backup_volume)


	def _latest_backup_dir(self):
		pass


class XtrabackupRestore(backup.Restore):
	pass


backup.backup_types['xtrabackup'] = XtrabackupBackup
backup.restore_types['xtrabackup'] = XtrabackupRestore		


class User(cdo.ConfigDriven):
	default_config = {
		'user': None,
		'password': None,
		'priveleges': '*'
	}


	def ensure(self):
		pass


	def exists(self):
		pass


	def delete(self):
		pass


def innobackupex(*params, **long_kwds):
	if not os.path.exists('/usr/bin/innobackupex'):
		mgr = pkgmgr.package_mgr()
		mgr.install('percona-xtrabackup')
	return linux.system(linux.build_cmd_args(
			executable='/usr/bin/innobackupex', 
			long=long_kwds, 
			params=params))
		
		
def my_print_defaults(*option_groups):
	out = linux.system(linux.build_cmd_args(
			executable='/usr/bin/my_print_defaults', 
			params=option_groups))[0]
	ret = {}
	for line in out.splitlines():
		cols = line.split('=')
		ret[cols[0][2:]] = cols[1] if len(cols) > 1 else True
	return ret


def mysqldump(*databases, **long_kwds):
	output = long_kwds.pop('output', None)
	cmd = linux.build_cmd_args(
			executable='/usr/bin/mysqldump',
			long=long_kwds,
			params=databases)
	kwds = {}
	if output:
		kwds['stdout'] = open(output, 'w+')
	return linux.system(cmd, **kwds)


