
import os
import sys
import string
import logging

from scalarizr import linux, storage2
from scalarizr.linux import coreutils, pkgmgr
from scalarizr.node import __node__
from scalarizr.services import mysql as mysql_svc
from scalarizr.services import backup
from scalarizr.libs import cdo
import shutil
import glob


LOG = logging.getLogger(__name__)


class Error(Exception):
	pass


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


class XtrabackupMixin(object):

	def _latest_backup_dir(self):
		try:
			name = sorted(os.listdir(self.backup_dir))[0]
		except IndexError:
			msg = 'Failed to find any previous backup in %s'
			raise Error(msg, self.backup_dir)
		else:
			return os.path.join(self.backup_dir, name) 	

	
	def _checkpoints(self, filename=None):
		if not filename:
			filename = self._latest_backup_dir() + '/xtrabackup_checkpoints'
		ret = {}
		for line in open(filename):
			key, value = line.split('=')
			ret[key.strip()] = value.strip()
		return ret


	def _binlog_info(self, filename=None):
		if not filename:
			filename = self._latest_backup_dir() + '/xtrabackup_binlog_info'
		return map(string.strip, open(filename).read().split(' '))


class XtrabackupBackup(XtrabackupMixin, backup.Backup):
	default_config = backup.Backup.default_config.copy()
	default_config.update({
		'backup_type': 'full',	
		# Allowed: full | incremental
		'from_lsn': None,
		# Allowed: int. Log sequence number to start from
		'backup_dir': '/mnt/dbbackup',
		# Directory to store backup files
		'volume': None
		# Volume to ensure and mount to 'backup_dir'.
		# After backup completion it will be snapshoted and
		# snapshot will be available in Restore configuration
	})

	
	def _run(self):
		if self.volume:
			self.volume = storage2.volume(self.volume)
			if self.tags:
				self.volume.tags = self.tags
			self.volume.mpoint = self.backup_dir
			self.volume.ensure(mount=True)
		else:
			os.makedirs(self.backup_dir)

		kwds = {}
		if self.backup_type == 'incremental':
			from_lsn = self.from_lsn
			if not from_lsn:
				checkpoints = self._checkpoints()
				from_lsn = checkpoints['to_lsn']
			kwds.update({
				'incremental': True,
				'incremental_lsn': from_lsn
			})

		try:
			innobackupex(self.backup_dir, 
					user=__mysql__['root_user'], 
					password=__mysql__['root_password'],
					**kwds)
			log_file, log_pos = self._binlog_info()
			chkpoints = self._checkpoints()
			to_lsn = chkpoints['to_lsn']
			from_lsn = chkpoints['from_lsn']
			snapshot = None
		finally:
			if self.volume:
				try:
					self.volume.detach()
				except:
					msg = 'Failed to detach backup volume: %s'
					LOG.warn(msg, sys.exc_info()[1])
				snapshot = self.volume.snapshot(
							self.description or 'MySQL xtrabackup', 
							self.tags)

		return backup.restore(
				type='xtrabackup', 
				log_file=log_file, 
				log_pos=log_pos,
				from_lsn=from_lsn,
				to_lsn=to_lsn,
				backup_type=self.backup_type,
				backup_dir=self.backup_dir,
				volume=self.volume,
				snapshot=snapshot)


class XtrabackupRestore(backup.Restore):
	default_config = backup.Backup.default_config.copy()
	default_config.update({
		'log_file': None,
		'log_pos': None,
		'from_lsn': None,
		'to_lsn': None,
		'backup_type': None,
		'backup_dir': '/mnt/dbbackup',
		'volume': None,
		'snapshot': None
	})

	def __init__(self, **kwds):
		self._mysql_init = mysql_svc.MysqlInitScript()
		self._data_dir = None
		self._binlog_dir = None
		self._log_bin = None

	def _run(self):
		rst_volume = None
		exc_info = None
		my_defaults = my_print_defaults('mysqld')
		self._data_dir = my_defaults['datadir']
		self._log_bin = self._my_defaults['log_bin']
		self._binlog_dir = os.path.dirname(self._log_bin)
		
		self._mysql_init.stop()				
		try:
			if self.snapshot and self.volume:
				# Clone volume object
				LOG.info('Creating restore volume from snapshot')
				rst_volume = storage2.volume(self.volume.config())
				rst_volume.snap = self.snapshot
				rst_volume.mpoint = self.backup_dir
				rst_volume.ensure(mount=True)
	
			if not os.listdir(self.backup_dir):
				msg = 'Failed to find any backups in %s'
				raise Error(msg, self.backup_dir)
			
			backups = sorted(os.listdir(self.backup_dir))
			LOG.info('Preparing the base backup')
			base = backups.pop(0)
			target_dir = os.path.join(self.backup_dir, base)
			innobackupex(target_dir, 
						apply_log=True, 
						redo_only=True,
						user=__mysql__['root_user'],
						password=__mysql__['root_password'])
			for inc in backups:
				LOG.info('Preparing incremental backup %s', inc)
				innobackupex(target_dir,
							apply_log=True, 
							redo_only=True, 
							incremental_dir=os.path.join(self.backup_dir, inc),
							user=__mysql__['root_user'],
							password=__mysql__['root_password'])
			LOG.info('Preparing the full backup')
			innobackupex(target_dir, 
						apply_log=True, 
						user=__mysql__['root_user'],
						password=__mysql__['root_password'])
			
			LOG.info('Copying backup to datadir')
			self._start_copyback()
			try:
				innobackupex(target_dir, copy_back=True)
				coreutils.chown_r(self._my_defaults['datadir'], 'mysql', 'mysql')
				self.mysql_init.start()
				self._commit_copyback()
			except:
				self._rollback_copyback()
				raise
		except:
			exc_info = sys.exc_info()
		finally:
			if rst_volume:
				LOG.info('Destroying restore volume')
				try:
					rst_volume.destroy()
				except:
					msg = 'Failed to destroy volume %s: %s'
					LOG.warn(msg, rst_volume.id, sys.exc_info()[1])
		if exc_info:
			raise exc_info[0], exc_info[1], exc_info[2]


	def _start_copyback(self):
		src = self._data_dir
		dst = src + '.bak'
		LOG.debug('Backup %s -> %s', src, dst)
		os.rename(src, dst)
		for name in glob.glob(self._log_bin + '*'):
			src = os.path.join(self._binlog_dir, name)
			dst = src + '.bak'
			LOG.debug('Backup %s -> %s', src, dst)
			os.rename(src, dst)
		os.makedirs(self._data_dir)				
		
	
	def _commit_copyback(self):
		shutil.rmtree(self._data_dir + '.bak')
		for name in glob.glob(self._log_bin + '*.bak'):
			os.remove(os.path.join(self._binlog_dir, name))
	
	
	def _rollback_copyback(self):
		if os.path.exists(self._data_dir):
			shutil.rmtree(self._data_dir)
		os.rename(self._data_dir + '.bak', self._data_dir)
		for name in glob.glob(self._log_bin + '*.bak'):
			dstname = os.path.splitext(name)[0]
			shutil.move(os.path.join(self._binlog_dir, name), 
						os.path.join(self._binlog_dir, dstname))


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


