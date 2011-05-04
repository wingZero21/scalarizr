'''
Created on April 18th, 2011

@author: Dmytro Korsakov
'''

import os
import pwd
import logging
import shutil

from scalarizr.libs.metaconf import Configuration
from scalarizr.handlers import ServiceCtlHanler
from scalarizr.util.filetool import read_file, write_file
from scalarizr.util import initdv2, system2, PopenError
from scalarizr.util import disttool, cryptotool
from scalarizr.config import BuiltinBehaviours
from scalarizr.bus import bus
from scalarizr.storage import Storage


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

STORAGE_VOLUME_CNF 		= 'mysql.json'
STORAGE_SNAPSHOT_CNF 	= 'mysql-snap.json'

BACKUP_CHUNK_SIZE 		= 200*1024*1024

BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.POSTGRESQL
CNF_SECTION = BEHAVIOUR
CNF_NAME = BEHAVIOUR



class PostgreSql(object):
	sysconfig_path = '/etc/sysconfig/pgsql/postgresql-9.0'
	
	_objects = None
	_instance = None
	
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
	
	@property
	def service(self): 
		#TODO: write initdv2
		#TODO: write _get
		return None
	
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
		self._logger = logging.getLogger(__name__)
	
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
		self.recovery_conf.primary_conninfo = (primary_ip, primary_port, self.root_user.name)
		self.postgresql_conf.hot_standby = 'on'
		
	def register_slave(self, slave_ip):
		self.postgresql_conf.listen_addresses = '*'
		self.pg_hba_conf.add_standby_host(slave_ip)
		self.service.restart(force=True)
		
	def unregister_slave(self, slave_ip):
		self.pg_hba_conf.delete_standby_host(slave_ip)
		self.service.restart(force=True)

	def stop_replication(self):
		self.trigger_file.create()
		
	def start_replication(self):
		self.trigger_file.destroy()
	
	def create_user(self, name):
		self.service.start()
		user = PgUser(name)	
		self.set_trusted_mode()
		password = user.generate_password(20)
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
	
	def __init__(self, name, group='postgres'):
		self.name = name
		self.group = group
		self._logger = logging.getLogger(__name__)
		self.psql = PSQL()
		self._cnf = bus.cnf
	
	def create(self, password, super=True):	
		self._create_system_user(password)
		self._create_pg_database()
		self._create_pg_user(super)
	
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
		else:
			try:
				out = system2([USERADD, '-g', self.group, '-p', password, self.name])[0]
				if out: self._logger.debug(out)
				self._logger.debug('Creating system user %s' % self.name)	
			except PopenError, e:
				self._logger.error('Unable to create system user %s: %s' % (self.name, e))
				raise

	@property
	def _is_role_exist(self):
		out = self.psql.execute('SELECT rolname FROM pg_roles;')
		return self.name in out
	
	@property
	def _is_pg_database_exist(self):
		out = self.psql.execute('SELECT datname FROM pg_database;')
		return self.name in out	
	
	@property
	def _is_system_user_exist(self):
		file = open('/etc/passwd', 'r')
		return -1 != file.read().find(self.name)
	
	def delete(self, delete_db=True, delete_role=True, delete_system_user=True):
		#TODO: implement delete methods
		pass
		
	def _delete_role(self):
		pass

	def _delete_pg_database(self):
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
		self._cnf.rawini.set(CNF_SECTION, self.opt_user_password, new_pass)
		
		return new_pass
	
	@property
	def opt_user_password(self): 
		return '%s_password' % self.username
	
	@property
	def password(self):
		return self._cnf.rawini.get(CNF_SECTION, self.opt_user_password)
		

class PSQL(object):
	path = PSQL_PATH
	user = None
	
	def __init__(self, user='postgres'):	
		self.user = user
		self._logger = logging.getLogger(__name__)
		
	def test_connection(self):
		#TODO: implement connection testing and integrate with initdv2 and pgsql
		pass
		
	def execute(self, query):
		try:
			out = system2([SU_EXEC, '-', self.user, '-c', '%s -c "%s"' % (self.path, query)])[0]
			self._logger.debug(out)	
		except PopenError, e:
			self._logger.error('Unable to execute query %s from user %s: %s' % (query, self.user, e))
			raise		
	
	
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
		return PgHbaRecord('host','replication','postgres',address='%s/30'%ip, auth_method='trust')
	
class ParseError(BaseException):
	pass
	
		
class PostreSqlMessages:
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
	
	NEW_MASTER_UP = "Mysql_NewMasterUp"
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
	Also MySQL behaviour adds params to common messages:
	
	= HOST_INIT_RESPONSE =
	@ivar mysql=dict(
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
	@ivar mysql=dict(
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
	def __init__(self):
		pass
	
			
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return postgresql.behaviour in behaviour and (
					message.name == PostreSqlMessages.NEW_MASTER_UP
				or 	message.name == PostreSqlMessages.PROMOTE_TO_MASTER
				or 	message.name == PostreSqlMessages.CREATE_DATA_BUNDLE
				or 	message.name == PostreSqlMessages.CREATE_BACKUP
				or  message.name == PostreSqlMessages.UPDATE_SERVICE_CONFIGURATION)	
	

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

def init_storage(volume_id, devname, mpoint):
	#self._logger.debug('checking mountpoint %s ' % mpoint)
	if not os.path.exists(mpoint):
		#self._logger.debug('creating %s' % mpoint)
		os.makedirs(mpoint)
		
	#self._logger.debug("creating device %s from volume %s" % (devname, volume_id))
	vol = Storage.create(type='ebs', id=volume_id, fstype='ext3', mpoint=mpoint, device=devname)
	
	#self._logger.debug('volume file system is : "%s"' % vol.fstype)
	if vol.fstype != 'ext3':
		#self._logger.debug('running mkfs')
		vol.mkfs()

	if vol.mounted():
		#self._logger.debug('device is already mounted.')
		pass
	else:
		#self._logger.debug("mounting EBS")
		vol.mount()
	return vol	

def create_snapshot():
	#TODO: implement snapshot creating
	snap = None
	return snap
	
# module init	