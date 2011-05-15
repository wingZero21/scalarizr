'''
Created on April 18th, 2011

@author: Dmytro Korsakov
'''

import os
import pwd
import logging
import shutil
import time

from scalarizr import config

from scalarizr.libs.metaconf import Configuration
from scalarizr.handlers import ServiceCtlHanler, HandlerError
from scalarizr.util.filetool import read_file, write_file
from scalarizr.util import initdv2, system2, wait_until, PopenError
from scalarizr.util import disttool, cryptotool, firstmatched
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.bus import bus
from scalarizr.storage import Storage, Snapshot, StorageError, Volume


SU_EXEC = '/bin/su'
USERMOD = '/usr/sbin/usermod'
USERADD = '/usr/sbin/useradd'
OPENSSL = '/usr/bin/openssl'

PSQL_PATH = '/usr/bin/psql'
CREATEUSER = '/usr/bin/createuser'
CREATEDB = '/usr/bin/createdb'

ROOT_USER 				= "scalr"

OPT_ROOT_PASSWORD 		= "root_password"
OPT_REPLICATION_MASTER  = "replication_master"

OPT_VOLUME_CNF			= 'volume_config'
OPT_SNAPSHOT_CNF		= 'snapshot_config'

STORAGE_PATH 			= "/mnt/pgstorage"
STORAGE_DATA_DIR 		= "data"

STORAGE_VOLUME_CNF 		= 'postgresql.json'
STORAGE_SNAPSHOT_CNF 	= 'postgresql-snap.json'

BACKUP_CHUNK_SIZE 		= 200*1024*1024

BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.POSTGRESQL
CNF_SECTION = BEHAVIOUR
CNF_NAME = BEHAVIOUR



def lazy(init):
	def wrapper(cls, *args, **kwargs):
		obj = init(cls, *args, **kwargs)
		return LazyInitScript(obj)
	return wrapper


class LazyInitScript(object):
	
	_script = None
	reload_queue = None
	restart_queue = None
	
	def __init__(self, script):
		self._script = script
		self.reload_queue = []
		self.restart_queue = []

	def start(self):
		try:
			if not self._script.running:
				self._script.start()
			elif self.restart_queue:
				reasons = ' '.join([req+',' for req in self.restart_queue])[:-1]
				self._script.restart(reasons)	
			elif self.reload_queue:
				reasons = ' '.join([req+',' for req in self.reload_queue])[:-1]
				self._script.reload(reasons)		
		finally:
			self.restart_queue = []
			self.reload_queue = []	
		
	def stop(self, reason=None):
		if self._script.running:
			try:
				self._script.stop(reason)
			finally:
				self.restart_queue = []
				self.reload_queue = []	

	def restart(self, reason=None):
		if self._script.running:
			self.restart_queue.append(reason)
		
	def reload(self, reason=None):
		if self._script.running:
			self.reload_queue.append(reason)
			
				
class PgSQLInitScript(initdv2.ParametrizedInitScript):
	socket_file = None
	
	@lazy
	def __new__(cls, *args, **kws):
		obj = super(PgSQLInitScript, cls).__new__(cls, *args, **kws)
		cls.__init__(obj)
		return obj
			
	def __init__(self):
		initd_script = None
		if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
			initd_script = ('/usr/sbin/service', 'postgresql')
		else:
			initd_script = firstmatched(os.path.exists, ('/etc/init.d/postgresql-9.0', '/etc/init.d/postgresql'))
		initdv2.ParametrizedInitScript.__init__(self, name=SERVICE_NAME, 
				initd_script=initd_script)
		
	def status(self):
		try:
			system2(PSQL_PATH)
		except PopenError, e:
			if 'No such file or directory' in str(e):
				return initdv2.Status.NOT_RUNNING
		return initdv2.Status.RUNNING

	
	def stop(self, reason=None):
		initdv2.ParametrizedInitScript.stop(self)
	
	def restart(self, reason=None):
		initdv2.ParametrizedInitScript.restart(self)
	
	def reload(self, reason=None):
		initdv2.ParametrizedInitScript.restart(self)
	
	
initdv2.explore(SERVICE_NAME, PgSQLInitScript)


class PostgreSql(object):
	
	_objects = None
	_instance = None
	
	service = None
	
	@property
	def is_replication_master(self):
		return True if int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)) else False
	
	def _set(self, key, obj):
		self._objects[key] = obj
		
	def _get(self, key, callback, *args, **kwargs):
		if not self._objects.has_key(key):
			self._set(callback(*args, **kwargs))
		return self._objects[key]
		
	def _get_config_dir(self):
		return self._get('config_dir', ConfigDir)
		
	def _set_config_dir(self, obj):
		self._set('config_dir', obj)
		
	def _get_postgresql_conf(self):
		return self._get('postgresql_conf', PostgresqlConf.find, self.config_dir)
	
	def _set_postgresql_conf(self, obj):
		self._set('postgresql_conf', obj)
	
	def _get_cluster_dir(self):
		return self._get('cluster_dir', ClusterDir.find, self.postgresql_conf)
		
	def _set_cluster_dir(self, obj):
		self._set('cluster_dir', obj)
			
	def _get_pg_hba_conf(self):
		return self._get('pg_hba_conf', PgHbaConf.find, self.config_dir)
		
	def _set_pg_hba_conf(self, obj):
		self._set('pg_hba_conf', obj)
		
	def _get_recovery_conf(self):
		return self._get('recovery_conf', RecoveryConf.find, self.config_dir)
	
	def _set_recovery_conf(self, obj):
		self._set('recovery_conf', obj)
	
	def _get_pid_file(self):
		return self._get('pid_file', PidFile.find, self.postgresql_conf)
	
	def _set_pid_file(self, obj):
		self._set('pid_file', obj)
		
	def _get_trigger_file(self):
		return self._get('trigger_file', Trigger.find, self.recovery_conf)
	
	def _set_trigger_file(self, obj):
		self._set('trigger_file', obj)
	
	def _get_root_user(self):
		key = 'root_user'
		if not self._objects.has_key(key):
			self._objects[key] = PgUser(ROOT_USER)
		return self._objects[key]
	
	def _set_root_user(self, user):
		self._set('root_user', user)
	
	root_user = property(_get_root_user, _set_root_user)
	config_dir = property(_get_config_dir, _set_config_dir)
	cluster_dir = property(_get_cluster_dir, _set_cluster_dir)
	postgresql_conf = property(_get_postgresql_conf, _set_postgresql_conf)
	pg_hba_conf = property(_get_pg_hba_conf, _set_pg_hba_conf)
	recovery_conf = property(_get_recovery_conf, _set_recovery_conf)
	pid_file = property(_get_pid_file, _set_pid_file)
	trigger_file = property(_get_trigger_file, _set_trigger_file)
		
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(PostgreSql, cls).__new__(
								cls, *args, **kwargs)
		return cls._instance
	
	def __init__(self):
		self._objects = {}
		self.service = initdv2.lookup(SERVICE_NAME)
		self._logger = logging.getLogger(__name__)
		self._cnf = bus.cnf
	
	def init_master(self, mpoint):
		self._init_service(mpoint)
		#snap = self.create_snapshot(volume_id) ; return snap.id 
		#TODO: create volume outside pgsql class
		
	def init_slave(self, mpoint, primary_ip, primary_port):
		#vol = ebstool.create_volume(ec2_conn, snap_id)
		self._init_service(mpoint)
		self.trigger_file = Trigger(os.path.join(self.config_dir.path, 'trigger'))
		self.recovery_conf.trigger_file = self.trigger_file.path
		self.recovery_conf.standby_mode = 'on'
		self.postgresql_conf.hot_standby = 'on'
		self.change_primary(self, primary_ip, primary_port, self.root_user.name)
		
	def register_slave(self, slave_ip):
		self.postgresql_conf.listen_addresses = '*'
		self.pg_hba_conf.add_standby_host(slave_ip)
		self.service.restart(force=True)
		
	def change_primary(self, primary_ip, primary_port, username):
		#TODO: make it work [see pg.repl documentation]
		self.recovery_conf.primary_conninfo = (primary_ip, primary_port, username)
	
	def unregister_slave(self, slave_ip):
		self.pg_hba_conf.delete_standby_host(slave_ip)
		self.service.restart(force=True)

	def stop_replication(self):
		self.trigger_file.create()
		
	def start_replication(self):
		self.trigger_file.destroy()
	
	def create_user(self, name, password=None):
		self.service.start()
		self.set_trusted_mode()
		user = PgUser(name)	
		password = password or user.generate_password(20)
		user.create(password, super=True)
		self.set_password_mode()
		return user	

	def set_trusted_mode(self):
		self.pg_hba_conf.set_trusted_access_mode()
		self.service.restart()
	
	def set_password_mode(self):
		self.pg_hba_conf.set_password_access_mode()
		self.service.restart()

	def _init_service(self, mpoint):
		#vol = self._init_storage(volume_id, devname, mpoint)
		#TODO: initialize volume outside of pgsql class
		self.root_user = self.create_user(ROOT_USER)
		self.service.stop()
		self.cluster_dir.move_to(mpoint)
		
		if disttool.is_centos():
			self.config_dir.move_to(self.config_dir.default_ubuntu_path)
			
		self.postgresql_conf.wal_level = 'hot_standby'
		self.postgresql_conf.max_wal_senders = '5'
		self.postgresql_conf.wal_keep_segments = '32'
				
	
postgresql = PostgreSql()

	
class PgUser(object):
	name = None
	password = None
	psql = None
	
	def __init__(self, name, password=None, group='postgres'):
		self.name = name
		self.password = password
		self.group = group
		self._logger = logging.getLogger(__name__)
		self.psql = PSQL()
		self._cnf = bus.cnf
	
	def create(self, password=None, super=True):	
		self._create_system_user(password or self.password)
		self._create_pg_database()
		self._create_role(super)
	
	def exists(self):
		return self._is_system_user_exist and self._is_role_exist and self._is_pg_database_exist
	
	def generate_password(self, length=20):
		return cryptotool.pwgen(length)
		
	def _create_role(self, super=True):
		if self._is_role_exist:
			self._logger.debug('Cannot create role: role %s already exists' % self.name)
		else:
			self._logger.debug('Creating role %s' % self.name)
			try:
				out = system2([SU_EXEC, '-', self.group, '-c', '%s -s %s' % (CREATEUSER, self.name)])[0]
				self._logger.debug(out or 'Role %s has been successfully created.' % self.name)
			except PopenError, e:
				self._logger.error('Unable to create role %s: %s' % (self.name, e))
				raise
		
	def _create_pg_database(self):
		if self._is_pg_database_exist:
			self._logger.debug('Cannot create db: database %s already exists' % self.name)
		else:
			self._logger.debug('Creating db %s' % self.name)
			try:
				out = system2([SU_EXEC, '-', self.group, '-c', '%s %s' % (CREATEDB,self.name)])[0]
				self._logger.debug(out or 'DB %s has been successfully created.' % self.name)
			except PopenError, e:
				self._logger.error('Unable to create db %s: %s' % (self.name, e))
				raise
	
	def _create_system_user(self, password):
		if self._is_system_user_exist:
			self._logger.debug('Cannot create system user: user %s already exists' % self.name)
			#TODO: check password
		else:
			try:
				out = system2([USERADD, '-g', self.group, '-p', password, self.name])[0]
				if out: self._logger.debug(out)
				self._logger.debug('Creating system user %s' % self.name)	
			except PopenError, e:
				self._logger.error('Unable to create system user %s: %s' % (self.name, e))
				raise
			#change password in privated/pgsql.ini
		self.store_password(password)

	@property
	def _is_role_exist(self):
		return self.name in self.psql.list_pg_roles()
	
	@property
	def _is_pg_database_exist(self):
		return self.name in self.psql.list_pg_databases()
	
	@property
	def _is_system_user_exist(self):
		file = open('/etc/passwd', 'r')
		return -1 != file.read().find(self.name)
	
	def delete(self, delete_db=True, delete_role=True, delete_system_user=True):
		#TODO: implement delete method
		pass
	
	def _delete_system_user(self):
		pass	

	def change_password(self, new_pass=None):
		new_pass = new_pass or self.generate_password()
		self._logger.debug('Changing password of system user %s to %s' % (self.name, new_pass)) 
		out, err, retcode = system2([OPENSSL, 'passwd', '-1', new_pass])
		shadow_password = out.strip()
		if retcode != 0:
			self._logger.error('Error creating hash for ' + self.name)
		if err:
			self._logger.error(err)
		
		r = system2([USERMOD, '-p', '-1', shadow_password, self.name])[2]
		if r != 0:
			self._logger.error('Error changing password for ' + self.name)	
		
		#change password in privated/pgsql.ini
		self.store_password(new_pass)
		
		return new_pass
	
	def check_password(self, password=None):
		#TODO: check (password or self.password), raise ValueError
		pass
	
	def get_password(self):
		return self._cnf.rawini.get(CNF_SECTION, self.opt_user_password)

	def store_password(self, password):
		self._cnf.rawini.set(CNF_SECTION, self.opt_user_password, password)
	
	opt_user_password = lambda(self): '%s_password' % self.username
			

class PSQL(object):
	path = PSQL_PATH
	user = None
	
	def __init__(self, user='postgres'):	
		self.user = user
		self._logger = logging.getLogger(__name__)
		
	def test_connection(self):
		try:
			system2(self.path)
		except PopenError, e:
			if 'err' in str(e):
				return False
		return True
		
	def execute(self, query):
		try:
			out = system2([SU_EXEC, '-', self.user, '-c', '%s -c "%s"' % (self.path, query)])[0]
			return out	
		except PopenError, e:
			self._logger.error('Unable to execute query %s from user %s: %s' % (query, self.user, e))
			raise		

	def list_pg_roles(self):
		out = self.execute('SELECT rolname FROM pg_roles;')
		roles = out.split()[2:-2]
		return roles
	
	def list_pg_databases(self):
		out = self.execute('SELECT datname FROM pg_database;')
		roles = out.split()[2:-2]
		return roles	
	
	def delete_pg_role(self, name):
		out = self.execute('DROP ROLE IF EXISTS %s;' % name)
		self._logger.debug(out)

	def delete_pg_database(self, name):
		out = self.psql.execute('DROP DATABASE IF EXISTS %s;' % name)
		self._logger.debug(out)
			
	
class ClusterDir(object):
	def __init__(self, path=None, user = "postgres"):
		self.path = path
		self.user = user
		self._logger = logging.getLogger(__name__)
		
	@classmethod
	def find(cls, postgresql_conf):
		return cls(postgresql_conf.data_directory)

	def move_to(self, dst):
		new_cluster_dir = os.path.join(dst, 'data')
		if os.path.exists(new_cluster_dir) and os.listdir(new_cluster_dir):
			self._logger.error('cluster seems to be already moved to %s' % dst)
		elif os.path.exists(self.path):
			self._logger.debug("copying cluster files from %s into %s" % (self.path, new_cluster_dir))
			shutil.copytree(self.path, new_cluster_dir)
		
		self._logger.debug("changing directory owner to %s" % self.user)	
		rchown(self.user, dst)
		
		self._logger.debug("Changing postgres user`s home directory")
		if disttool.is_centos():
			#looks like ubuntu doesn`t need this
			system2([USERMOD, '-d', new_cluster_dir, self.user]) 
	
		return new_cluster_dir


class ConfigDir(object):
	
	path = None
	user = None
	default_ubuntu_path = '/etc/postgresql/9.0/main'
	default_centos_path = '/var/lib/pgsql/9.0/main'
	sysconf_path = '/etc/sysconfig/pgsql/postgresql-9.0'
	
	def __init__(self, path=None, user = "postgres"):
		self._logger = logging.getLogger(__name__)
		self.path = path or self.find_path()
		self.user = user
	
	def find_path(self):
		path = self.get_sysconfig_pgdata()
		if path:
			return path
		return self.default_ubuntu_path if disttool.is_ubuntu() else self.default_centos_path
	
	def move_to(self, dst):
		if not os.path.exists(dst):
			self._logger.debug("creating %s" % dst)
			os.makedirs(dst)
		
		for config in ['postgresql.conf', 'pg_ident.conf', 'pg_hba.conf']:
			old_config = os.path.join(self.path, config)
			new_config = os.path.join(dst, config)
			if os.path.exists(old_config):
				self._logger.debug('Moving %s' % config)
				shutil.move(old_config, new_config)
			elif os.path.exists(new_config):
				self._logger.debug('%s is already in place. Skipping.' % config)
			else:
				raise BaseException('Postgresql config file not found: %s' % old_config)
			rchown(self.user, new_config)
		
		self._logger.debug("configuring pid and cluster dir")
		conf = PostgresqlConf(dst, autosave=False)
		conf.data_directory = self.path
		conf.pid_file = os.path.join(dst, 'postmaster.pid')
		conf.save()
		self._make_symlinks(dst)
		self._patch_sysconfig(dst)
		self.path = dst
		
	def _make_symlinks(self, dst_dir):
		self._logger.debug("creating symlinks required by initscript")
		for obj in ['base', 'PG_VERSION', 'postmaster.pid']:
			src = os.path.join(self.path, obj)
			dst = os.path.join(dst_dir, obj) 
			if os.path.islink(dst):
				self._logger.debug("%s exists and it is probably old. Unlinking." % dst)
				os.unlink(dst)
			elif os.path.exists(dst):
				self._logger.warning('Something wrong: %s is not a symlink. Removing.' % dst)
				shutil.rmtree(dst)
				
			self._logger.debug("Creating symlink %s -> %s" % (src, dst))
			os.symlink(src, dst)
			if os.path.exists(src):
				rchown(self.user, dst)
			else:
				self._logger.debug('Warning: %s is a dead link' % dst)
			
	def _patch_sysconfig(self, config_dir):
		if config_dir == self.get_sysconfig_pgdata():
			self._logger.debug('sysconfig file already rewrites PGDATA. Skipping.')
		else:
			self.set_sysconfig_pgdata(config_dir)
	
	def set_sysconfig_pgdata(self, pgdata):
		self._logger.debug("rewriting PGDATA path in sysconfig")
		file = open(self.sysconf_path, 'w')
		file.write('PGDATA=%s' % pgdata)
		file.close()
		
	def get_sysconfig_pgdata(self):
		pgdata = None
		if os.path.exists(self.sysconf_path):
			s = open(self.sysconf_path, 'r').readline().strip()
			if s and len(s)>7:
				pgdata = s[7:]
			else: 
				self._logger.debug('sysconfig has no PGDATA')
		return pgdata


class PidFile(object):
	path = None
	
	def __init__(self, path):
		self.path = path
		
	@classmethod
	def find(cls, postgresql_conf):
		return cls(postgresql_conf.pid_file)	
	
	@property	
	def proc_id(self):
		return open(self.path, 'r').readline().strip() if os.path.exists(self.path) else None


class Trigger(object):
	
	path = None
	
	def __init__(self, path):
		self.path = path
		self._logger = logging.getLogger(__name__)
		
	@classmethod
	def find(cls, recovery_conf):
		return cls(recovery_conf.trigger_file)
	
	def create(self):
		if not self.exists():
			write_file(self.path, '', 'w', logger=self._logger)
		
	def destroy(self):
		if self.exists():
			os.remove(self.path)
		
	def exists(self):
		return os.path.exists(self.path)
	

class BasePGConfig(object):
	'''
	Parent class for object representations of postgresql.conf and recovery.conf which fortunately both have similar syntax
	'''
	
	autosave = None
	path = None
	data = None
	config_name = None
	
	def __init(self, path, autosave=True):
		self.autosave = autosave
		self.path = path
		
	@classmethod
	def find(cls, config_dir):
		return cls(os.path.join(config_dir.path, cls.config_name))
		
	def set(self, option, value):
		if not self.data:
			self.data = Configuration('pgsql')
			self.data.read(self.path)
		self.data.add(option,value, force=True)
		if self.autosave:
			self.save()
			self.data = None
			
	def set_path_type_option(self, option, path):
		if not os.path.exists(path):
			raise ValueError('%s %s does not exist' % (option, path))
		self.set(option, path)		
		
	def set_numeric_option(self, option, number):
		try:
			assert not number or float(number)
			self.set(option, number)
		except ValueError:
			raise ValueError('%s must be a number (got %s instead)' % (option, number))
					
	def get(self, option):
		if not self.data:
			self.data =  Configuration('pgsql')
			self.data.read(self.path)	
		value = self.data.read(option)	
		if self.autosave:
			self.data = None
		return value
	
	def save(self):
		if self.data:
			self.data.write(self.path)
			

class PostgresqlConf(BasePGConfig):

	config_name = 'postgresql.conf'
	
	def _get_pid_file_path(self):
		return self.get('external_pid_file')
	
	def _set_pid_file_path(self, path):
		self.set('external_pid_file', path)
		if not os.path.exists(path):
			self._logger.warning('pid file does not exist')
	
	def _get_data_directory(self):
		return self.get('data_directory')
	
	def _set_data_directory(self, path):
		self.set_path_type_option('data_directory', path)
	
	def _get_wal_level(self):
		return self.get('wal_level')
	
	def _set_wal_level(self, level):
		self.set('wal_level', level)
	
	def _get_max_wal_senders(self):
		self.get('max_wal_senders')
	
	def _set_max_wal_senders(self, number):
		self.set_numeric_option('max_wal_senders', number)
	
	def _get_wal_keep_segments(self):
		self.get('wal_keep_segments')
	
	def _set_wal_keep_segments(self, number):
		self.set_numeric_option('wal_keep_segments', number)
		
	def _get_listen_addresses(self):
		self.get('listen_addresses')
	
	def _set_listen_addresses(self, addresses='*'):
		self.set('listen_addresses', addresses)
	
	def _get_hot_standby(self):
		self.get('hot_standby')
	
	def _set_hot_standby(self, mode):
		#must bee boolean and default is 'off'
		self.set('hot_standby', mode)
		
	pid_file = property(_get_pid_file_path, _set_pid_file_path)
	data_directory = property(_get_data_directory, _set_data_directory)
	wal_level = property(_get_wal_level, _set_wal_level)
	max_wal_senders = property(_get_max_wal_senders, _set_max_wal_senders)
	wal_keep_segments = property(_get_wal_keep_segments, _set_wal_keep_segments)
	listen_addresses = property(_get_listen_addresses, _set_listen_addresses)
	hot_standby = property(_get_hot_standby, _set_hot_standby)

	
class RecoveryConf(BasePGConfig):
	
	def _get_standby_mode(self):
		self.get('standby_mode')
	
	def _set_standby_mode(self, mode):
		self.set('standby_mode', mode)
	
	def _get_primary_conninfo(self):
		info = self.get('primary_conninfo')
		return tuple([raw.split('=')[1].strip() if len(raw.split('=')) == 2 else '' for raw in info.split()])
		
	def _set_primary_conninfo(self, info_tuple):
		#need to check first
		host, port, user = info_tuple
		self.set('primary_conninfo', "host=%s port=%s user=%s" % (host,port,user))
		
	def _get_trigger_file(self):
		self.get('trigger_file')
	
	def _set_trigger_file(self, path):
		self.set('trigger_file', path)	
	
	standby_mode = property(_get_standby_mode, _set_standby_mode)
	primary_conninfo = property(_get_primary_conninfo, _set_primary_conninfo)
	trigger_file = property(_get_trigger_file, _set_trigger_file)

	
class PgHbaRecord(object):
	'''
	A record can have one of the seven formats
	
	local      database  user  auth-method  [auth-options]
	host       database  user  address  auth-method  [auth-options]
	hostssl    database  user  address  auth-method  [auth-options]
	hostnossl  database  user  address  auth-method  [auth-options]
	host       database  user  IP-address  IP-mask  auth-method  [auth-options]
	hostssl    database  user  IP-address  IP-mask  auth-method  [auth-options]
	hostnossl  database  user  IP-address  IP-mask  auth-method  [auth-options]
	'''
	host_types = ['local', 'host', 'hostssl','hostnossl']
	auth_methods = ['trust','reject','md5','password', 'gss',
				'sspi', 'krb5', 'ident', 'peer', 'ldap',
				'radius', 'cert', 'pam']
	
	def __init__(self, host='local', database='all', user='all', auth_method='trust', address=None, ip=None, mask=None, auth_options=None):		
		self.host = host
		self.database = database
		self.user = user
		self.auth_method = auth_method
		self.auth_options = auth_options
		self.address = address 
		self.ip = ip
		self.mask = mask		
	
	@classmethod
	def from_string(cls, entry):
		attrs = entry.split()
		if len(attrs) < 4:
			raise ParseError('Cannot parse pg_hba.conf entry: %s. Entry must contain more than 4 values' % entry)

		host = attrs[0]
		database = attrs[1]
		user = attrs[2]

		if host not in cls.host_types:
			raise ParseError('Cannot parse pg_hba.conf entry: %s. Unknown host type' % entry)
		
		last_attrs = attrs[3:]
		for method in cls.auth_methods:
			if method in last_attrs:
				
				auth_method = method
				
				index = last_attrs.index(method)
				host_info = last_attrs[:index]
				address = host_info[0] if len(host_info) == 1 else None
				(ip, mask) = (host_info[0], host_info[1]) if len(host_info) == 2 else (None,None)

				if host=='local' and (address or ip):
					raise ParseError('Cannot parse pg_hba.conf entry: %s. Address cannot be set when host is "local"' % entry)
				elif address and (ip or mask): 
					raise ParseError('Cannot parse pg_hba.conf entry: %s. Cannot set adress along with ip and mask' % entry)				
				auth_options = ' '.join(last_attrs[index+1:]) if len(last_attrs[index+1:]) else None
				
				break
		else:
			raise ParseError('Cannot parse pg_hba.conf entry: %s. No auth method found' % entry)
		return PgHbaRecord(host, database, user, auth_method, address, ip, mask, auth_options)
	
	def is_similar_to(self, other):
		return 	self.host == other.host and \
		self.database == other.database and \
		self.user == other.user and \
		self.address == other.address and \
		self.ip == other.ip and \
		self.mask == other.mask	
	
	def __eq__(self, other):
		return self.is_similar_to(other) and \
		self.auth_method == other.auth_method and \
		self.auth_options == other.auth_options	
			
	def __repr__(self):
		line = '%s\t%s\t%s' % (self.host, self.database, self.user)
		
		if self.address: line += '\t%s' % self.address
		else:
			if self.ip: line += '\t%s' % self.ip
			if self.mask: line += '\t%s' % self.mask
		
		line +=  '\t%s' % self.auth_method
		if self.auth_options: line += '\t%s' % self.auth_options
			
		return line	
	
		
class PgHbaConf(Configuration):
	
	config_name = 'pg_hba.conf'
	path = None
	trusted_mode = PgHbaRecord('local', 'all', 'postgres', auth_method = 'trust')
	password_mode = PgHbaRecord('local', 'all', 'postgres', auth_method = 'password')
	
	def __init__(self, config_dir_path):
		self.config_dir_path = config_dir_path
		self.path = os.path.join(self.config_name, config_dir_path)
		self._logger = logging.getLogger(__name__)

	@classmethod
	def find(cls, config_dir):
		return cls(os.path.join(config_dir.path, cls.config_name))
	
	def add_record(self, record):
		text = read_file(self.path) or ''
		for line in text.splitlines():
			if not line.strip().startswith('#') and PgHbaRecord.from_string(line) == record:
				#already in file
				return
		write_file(self.path, str(record), 'a')	
			
	def delete_record(self, record):
		lines = []
		text = read_file(self.path)
		for line in text.splitlines():
			if line.strip().startswith('#') or PgHbaRecord.from_string(line) == record:
				lines.append(file)
		write_file(self.path, lines)
	
	def add_standby_host(self, ip):
		record = self._make_standby_record(ip)
		self.add_record(record)

	def delete_standby_host(self, ip):
		record = self._make_standby_record(ip)
		self.delete_record(record)
	
	def set_trusted_access_mode(self):
		self.delete_record(self.password_mode)
		self.add_record(self.trusted_mode)
	
	def set_password_access_mode(self):
		self.delete_record(self.trusted_mode)
		self.add_record(self.password_mode)
	
	def _make_standby_record(self,ip):
		return PgHbaRecord('host','replication','postgres',address='%s/32'%ip, auth_method='trust')
	
class ParseError(BaseException):
	pass
	
		
class PostgreSqlMessages:
	CREATE_DATA_BUNDLE = "Pgsql_CreateDataBundle"
	
	CREATE_DATA_BUNDLE_RESULT = "Pgsql_CreateDataBundleResult"
	'''
	@ivar status: ok|error
	@ivar last_error
	@ivar snapshot_config
	@ivar used_size
	'''
	
	CREATE_BACKUP = "Pgsql_CreateBackup"
	
	CREATE_BACKUP_RESULT = "Pgsql_CreateBackupResult"
	"""
	@ivar status: ok|error
	@ivar last_error
	@ivar backup_urls: S3 URL
	"""
	
	"""
	@ivar status: ok|error
	@ivar last_error
	@ivar pma_user
	@ivar pma_password
	@ivar farm_role_id
	"""
	
	PROMOTE_TO_MASTER	= "Pgsql_PromoteToMaster"
	"""
	@ivar root_password: 'scalr' user password 
	@ivar repl_password: 'scalr_repl' user password
	@ivar stat_password: 'scalr_stat' user password
	@ivar volume_config?: Master storage configuration
	"""
	
	PROMOTE_TO_MASTER_RESULT = "Pgsql_PromoteToMasterResult"
	"""
	@ivar status: ok|error
	@ivar last_error: Last error message in case of status = 'error'
	@ivar volume_config: Master storage configuration
	@ivar snapshot_config?
	@ivar log_file?
	@ivar log_pos?
	"""
	
	NEW_MASTER_UP = "Postgresql_NewMasterUp"
	"""
	@ivar behaviour
	@ivar local_ip
	@ivar remote_ip
	@ivar role_name		
	@ivar repl_password
	@ivar snapshot_config?
	@ivar log_file?
	@ivar log_pos?
	"""
	
	"""
	Also Postgresql behaviour adds params to common messages:
	
	= HOST_INIT_RESPONSE =
	@ivar postgresql=dict(
		replication_master: 	1|0
		root_password:			'scalr' user password  					(on slave)
		repl_password:			'scalr_repl' user password				(on slave)
		stat_password: 			'scalr_stat' user password				(on slave)
		log_file:				Binary log file							(on slave)
		log_pos:				Binary log file position				(on slave)
		volume_config			Master storage configuration			(on master)
		snapshot_config			Master storage snapshot 				(both)
	)
	
	= HOST_UP =
	@ivar postgresql=dict(
		root_password: 			'scalr' user password  					(on master)
		repl_password: 			'scalr_repl' user password				(on master)
		stat_password: 			'scalr_stat' user password				(on master)
		log_file: 				Binary log file							(on master) 
		log_pos: 				Binary log file position				(on master)
		volume_config:			Current storage configuration			(both)
		snapshot_config:		Master storage snapshot					(on master)		 
	) 
	"""


class PostgreSqlHander(ServiceCtlHanler):
	_logger = None
		
	_queryenv = None
	""" @type _queryenv: scalarizr.queryenv.QueryEnvService	"""
	
	_platform = None
	""" @type _platform: scalarizr.platform.Ec2Platform """
	
	_cnf = None
	''' @type _cnf: scalarizr.config.ScalarizrCnf '''
	
	storage_vol = None

			
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return postgresql.behaviour in behaviour and (
					message.name == PostgreSqlMessages.NEW_MASTER_UP
				or 	message.name == PostgreSqlMessages.PROMOTE_TO_MASTER
				or 	message.name == PostgreSqlMessages.CREATE_DATA_BUNDLE
				or 	message.name == PostgreSqlMessages.CREATE_BACKUP
				or  message.name == PostgreSqlMessages.UPDATE_SERVICE_CONFIGURATION)	

	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)

		self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
		self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))
			
		bus.on("init", self.on_init)
		bus.define_events(
			'before_postgresql_data_bundle',
			
			'postgresql_data_bundle',
			
			# @param host: New master hostname 
			'before_postgresql_change_master',
			
			# @param host: New master hostname 
			'postgresql_change_master'
		)	
		
		self.postgresql = PostgreSql()


	def on_init(self):		
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("before_reboot_start", self.on_before_reboot_start)
		bus.on("before_reboot_finish", self.on_before_reboot_finish)
		
		if self._cnf.state == ScalarizrState.RUNNING:

			storage_conf = Storage.restore_config(self._volume_config_path)
			self.storage_vol = Storage.create(storage_conf)
			if not self.storage_vol.mounted():
				self.storage_vol.mount()
			
			if int(self._get_ini_options(OPT_REPLICATION_MASTER)[0]):
				self._logger.debug("Checking presence of Scalr's PostgreSQL root user.")
				root_password = self.postgresql.root_user.get_password()
				if not self.postgresql.root_user.exists():
					self._logger.debug("Scalr's PostgreSQL root user does not exist. Recreating")
					self.postgresql.root_user = self.postgresql.create_user(ROOT_USER, root_password)
				else:
					try:
						self.postgresql.root_user.check_password(root_password)
						self._logger.debug("Scalr's root PgSQL user is present. Password is correct.")				
					except ValueError:
						self._logger.warning("Scalr's root PgSQL user was changed. Recreating.")
						self.postgresql.root_user.change_password(root_password)


	def on_host_init_response(self, message):
		"""
		Check postgresql data in host init response
		@type message: scalarizr.messaging.Message
		@param message: HostInitResponse
		"""
		if not message.body.has_key("postgresql"):
			raise HandlerError("HostInitResponse message for PostgreSQL behaviour must have 'postgresql' property")

		dir = os.path.dirname(self._volume_config_path)
		if not os.path.exists(dir):
			os.makedirs(dir)
		
		postgresql_data = message.mysql.copy()
		for key, file in ((OPT_VOLUME_CNF, self._volume_config_path), 
						(OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
			if os.path.exists(file):
				os.remove(file)
			if key in postgresql_data:
				Storage.backup_config(postgresql_data[key], file)
				del postgresql_data[key]
						
		self._logger.debug("Update postgresql config with %s", postgresql_data)
		self._update_config(postgresql_data)


	def on_before_host_up(self, message):
		"""
		Configure PostgreSQL behaviour
		@type message: scalarizr.messaging.Message		
		@param message: HostUp message
		"""

		repl = 'master' if self.postgresql.is_replication_master else 'slave'
		bus.fire('before_mysql_configure', replication=repl)
		
		if self.postgresql.is_replication_master:
			self._init_master(message)									  
		else:
			self._init_slave(message)		
			
		bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)
				
	
	def _init_master(self, message):
		"""
		Initialize MySQL master
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		
		self._logger.info("Initializing PostgreSQL master")
		
		# Plug storage
		volume_cnf = Storage.restore_config(self._volume_config_path)
		try:
			snap_cnf = Storage.restore_config(self._snapshot_config_path)
			volume_cnf['snapshot'] = snap_cnf
		except IOError:
			pass
		self.storage_vol = self._plug_storage(mpoint=self._storage_path, vol=volume_cnf)
		Storage.backup_config(self.storage_vol.config(), self._volume_config_path)		
		
		# Stop PostgreSQL server
		self.postgresql.service.stop('Initializing Master')
		
		msg_data = None
		storage_valid = self._storage_valid() # It's important to call it before _move_mysql_dir
		self.postgresql.cluster_dir.move(move_files=storage_valid)
		
		# If It's 1st init of mysql master storage
		if not storage_valid:
				
			# Add system users	
			self.postgresql.root_user = self.postgresql.create_user(ROOT_USER)
			root_password = self.postgresql.root_user.get_password()
			
			# Get binary logfile, logpos and create storage snapshot
			snap, log_file, log_pos = self._create_snapshot(ROOT_USER, root_password)
			Storage.backup_config(snap.config(), self._snapshot_config_path)
			
			# Update HostUp message 
			msg_data = dict(root_password=root_password)
			msg_data.update(self._compat_storage_data(self.storage_vol, snap))
			
		# If volume has mysql storage directory structure (N-th init)
		else:
			# Get required configuration options
			root_password = self._get_ini_options(OPT_ROOT_PASSWORD)
			
			# Create snapshot
			snap = self._create_snapshot(ROOT_USER, root_password)
			Storage.backup_config(snap.config(), self._snapshot_config_path)
			
			# Update HostUp message 
			msg_data = dict(
				log_file=log_file, 
				log_pos=log_pos
			)
			msg_data.update(self._compat_storage_data(self.storage_vol, snap))
			
		if msg_data:
			message.mysql = msg_data.copy()
			try:
				del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
			except KeyError:
				pass 
			self._update_config(msg_data)
	
	
	def _init_slave(self, message):
		"""
		Initialize MySQL slave
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		self._logger.info("Initializing MySQL slave")
		
		storage_valid = self._storage_valid()
		
		if not storage_valid:
			self._logger.debug("Initialize slave storage")
			self.storage_vol = self._plug_storage(self._storage_path, 
					dict(snapshot=Storage.restore_config(self._snapshot_config_path)))			
			Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
			
		self.postgresql.cluster_dir.move(move_files=storage_valid)
		
		# Change replication master 
		master_host = None
		self._logger.info("Requesting master server")
		while not master_host:
			try:
				master_host = list(host 
					for host in self._queryenv.list_roles(self._role_name)[0].hosts 
					if host.replication_master)[0]
			except IndexError:
				self._logger.debug("QueryEnv respond with no mysql master. " + 
						"Waiting %d seconds before the next attempt", 5)
				time.sleep(5)
				
		self._logger.debug("Master server obtained (local_ip: %s, public_ip: %s)",
				master_host.internal_ip, master_host.external_ip)
		
		host = master_host.internal_ip or master_host.external_ip
		
		self.postgresql.change_primary(host, username=ROOT_USER)
		
		'''
		self._change_master( 
			host=host, 
			user=ROOT_USER, 
			password=self.postgresql.root_user.get_password(),
			timeout=self._change_master_timeout
		)
		'''
		
		# Update HostUp message
		message.postgresql = self._compat_storage_data(self.storage_vol)


	def on_Postgresql_CreateDataBundle(self, message):
		
		try:
			bus.fire('before_postgresql_data_bundle')
			# Retrieve password for scalr postgresql root user
			root_password = self.postgresql.root_user.get_password()
			# Creating snapshot		
			snap = self._create_snapshot(ROOT_USER, root_password)
			used_size = int(system2(('df', '-P', '--block-size=M', self._storage_path))[0].split('\n')[1].split()[2][:-1])
			bus.fire('before_postgresql_data_bundle', snapshot_id=snap.id)			
			
			# Notify scalr
			msg_data = dict(
				used_size='%.3f' % (float(used_size) / 1000,),
				status='ok'
			)
			msg_data.update(self._compat_storage_data(snap=snap))
			self.send_message(PostgreSqlMessages.CREATE_DATA_BUNDLE_RESULT, msg_data)

		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(PostgreSqlMessages.CREATE_DATA_BUNDLE_RESULT, dict(
				status		='error',
				last_error	= str(e)
			))


	def _create_storage_snapshot(self):
		self._logger.info("Creating storage snapshot")
		try:
			return self.storage_vol.snapshot()
		except StorageError, e:
			self._logger.error("Cannot create PostgreSQL data snapshot. %s", e)
			raise


	def _create_snapshot(self, root_user, root_password, dry_run=False):
		if self.postgresql.service.running:
			# TODO: Lock tables
			pass
		
		system2('sync', shell=True)
		# Creating storage snapshot
		snap = None if dry_run else self._create_storage_snapshot()
		if not self.postgresql.service.running:
			# TODO: Unlock tables
			pass
		
		wait_until(lambda: snap.state in (Snapshot.CREATED, Snapshot.COMPLETED, Snapshot.FAILED))
		if snap.state == Snapshot.FAILED:
			raise HandlerError('MySQL storage snapshot creation failed. See log for more details')
		
		return snap


	def _compat_storage_data(self, vol=None, snap=None):
		ret = dict()
		if vol:
			ret['volume_config'] = vol.config()
		if snap:
			ret['snapshot_config'] = snap.config()
		return ret


	def _plug_storage(self, mpoint, vol):
		if not isinstance(vol, Volume):
			vol = Storage.create(vol)

		try:
			if not os.path.exists(mpoint):
				os.makedirs(mpoint)
			vol.mount(mpoint)
		except StorageError, e:
			''' XXX: Crapy. We need to introduce error codes from fstool ''' 
			if 'you must specify the filesystem type' in str(e):
				vol.mkfs()
				vol.mount(mpoint)
			else:
				raise
		return vol
	
						
	def _update_config(self, data): 
		self._cnf.update_ini(BEHAVIOUR, {CNF_SECTION: data})
		
	
	def on_Postgresql_PromoteToMaster(self, message):
		"""
		Promote slave to master
		@type message: scalarizr.messaging.Message
		@param message: Mysql_PromoteToMaster
		"""
		old_conf 		= None
		new_storage_vol	= None
		
		if not int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)):
			master_storage_conf = message.body.get('volume_config')
			tx_complete = False	
			
						
			try:
				# Stop mysql
				if master_storage_conf:
					self.postgresql.stop_replication()
					self.postgresql.service.stop()
					
					# Unplug slave storage and plug master one
					old_conf = self.storage_vol.detach(force=True) # ??????
					new_storage_vol = self._plug_storage(self._storage_path, master_storage_conf)				
					# Continue if master storage is a valid MySQL storage 
					storage_valid = self._storage_valid()
					if storage_valid:
						self.postgresql.cluster_dir.move(move_files=storage_valid)
						# Update behaviour configuration
						updates = {
							OPT_ROOT_PASSWORD : message.root_password,
							OPT_REPLICATION_MASTER 	: "1"
						}
						self._update_config(updates)
						Storage.backup_config(new_storage_vol.config(), self._volume_config_path) 
						
						# Send message to Scalr
						msg_data = dict(status='ok')
						msg_data.update(self._compat_storage_data(vol=new_storage_vol))
						self.send_message(PostgreSqlMessages.PROMOTE_TO_MASTER_RESULT, msg_data)
					else:
						raise HandlerError("%s is not a valid MySQL storage" % self._storage_path)
					self.postgresql.service.start()
				else:
					self.postgresql.stop_replication()
					#TODO: ask what to do in this situation
					'''
					self._start_service()
					mysql = spawn_mysql_cli(ROOT_USER, message.root_password)
					timeout = 180
					try:
						mysql.sendline("STOP SLAVE;")
						mysql.expect("mysql>", timeout=timeout)
						mysql.sendline("RESET MASTER;")
						mysql.expect("mysql>", 20)
						filetool.remove(os.path.join(self._data_dir, 'relay-log.info'))
						filetool.remove(os.path.join(self._data_dir, 'master.info'))
					except pexpect.TIMEOUT:
						raise HandlerError("Timeout (%d seconds) reached " + 
									"while waiting for slave stop and master reset." % (timeout,))
					finally:
						mysql.close()
					'''
					updates = {
						OPT_ROOT_PASSWORD : message.root_password,
						OPT_REPLICATION_MASTER 	: "1"
					}
					self._update_config(updates)
										
					snap = self._create_snapshot(ROOT_USER, message.root_password)
					Storage.backup_config(snap.config(), self._snapshot_config_path)
					
					# Send message to Scalr
					msg_data = dict(
						status="ok",
					)
					msg_data.update(self._compat_storage_data(self.storage_vol.config(), snap))
					self.send_message(PostgreSqlMessages.PROMOTE_TO_MASTER_RESULT, msg_data)							
					
				tx_complete = True
				
			except (Exception, BaseException), e:
				self._logger.exception(e)
				if new_storage_vol:
					new_storage_vol.detach()
				# Get back slave storage
				if old_conf:
					self._plug_storage(self._storage_path, old_conf)
				
				self.send_message(PostgreSqlMessages.PROMOTE_TO_MASTER_RESULT, dict(
					status="error",
					last_error=str(e)
				))

				# Start MySQL
				self._start_service()
			
			if tx_complete and master_storage_conf:
				# Delete slave EBS
				self.storage_vol.destroy(remove_disks=True)
				self.storage_vol = new_storage_vol
				Storage.backup_config(self.storage_vol.config(), self._storage_path)
		else:
			self._logger.warning('Cannot promote to master. Already master')
	
	
	def on_Postgresql_NewMasterUp(self, message):
		"""
		Switch replication to a new master server
		@type message: scalarizr.messaging.Message
		@param message:  Mysql_NewMasterUp
		"""
		
		if not self.postgresql.is_replication_master:
			host = message.local_ip or message.remote_ip
			self._logger.info("Switching replication to a new MySQL master %s", host)
			bus.fire('before_postgresql_change_master', host=host)			
			
			if 'snapshot_config' in message.body:
				self._logger.info('Reinitializing Slave from the new snapshot %s', 
						message.snapshot_config['id'])
				self.postgresql.service.stop()
				
				self._logger.debug('Destroing old storage')
				self.storage_vol.destroy()
				self._logger.debug('Storage destoyed')
				
				self._logger.debug('Plugging new storage')
				vol = Storage.create(snapshot=message.snapshot_config.copy())
				self._plug_storage(self._storage_path, vol)
				self._logger.debug('Storage plugged')
				
				Storage.backup_config(vol.config(), self._volume_config_path)
				Storage.backup_config(message.snapshot_config, self._snapshot_config_path)
				self.storage_vol = vol
				
				self.postgresql.service.start()		
			#TODO: decide what to do here		
			'''
			my_cli = spawn_mysql_cli(ROOT_USER, message.root_password)
			
			if not 'snapshot_config' in message.body:
				self._logger.debug("Stopping slave i/o thread")
				my_cli.sendline("STOP SLAVE IO_THREAD;")
				my_cli.expect("mysql>")
				self._logger.debug("Slave i/o thread stopped")
				
				self._logger.debug("Retrieving current log_file and log_pos")
				my_cli.sendline("SHOW SLAVE STATUS\\G");
				my_cli.expect("mysql>")
				log_file = log_pos = None
				for line in my_cli.before.split("\n"):
					pair = map(str.strip, line.split(": ", 1))
					if pair[0] == "Master_Log_File":
						log_file = pair[1]
					elif pair[0] == "Read_Master_Log_Pos":
						log_pos = pair[1]
				self._logger.debug("Retrieved log_file=%s, log_pos=%s", log_file, log_pos)
			'''
			
			self._change_master(
				host=host, 
				user=ROOT_USER, 
				password=message.root_password,
				timeout=self._change_master_timeout,
			)
				
			self._logger.debug("Replication switched")
			bus.fire('postgresql_change_master', host=host)
		else:
			self._logger.debug('Skip NewMasterUp. My replication role is master')	
				
	
	def on_Postgresql_CreateBackup(self, message):
		pass

	
	def on_Postgresql_CreatePmaUser(self, message):
		pass

		
def get_handlers():
	return (PostgreSqlHander(), )

	

def rchown(user, path):
	#log "chown -r %s %s" % (user, path)
	user = pwd.getpwnam(user)	
	os.chown(path, user.pw_uid, user.pw_gid)
	try:
		for root, dirs, files in os.walk(path):
			for dir in dirs:
				os.chown(os.path.join(root , dir), user.pw_uid, user.pw_gid)
			for file in files:
				if os.path.exists(os.path.join(root, file)): #skipping dead links
					os.chown(os.path.join(root, file), user.pw_uid, user.pw_gid)
	except OSError, e:
		#log 'Cannot chown directory %s : %s' % (path, e)	
		pass
