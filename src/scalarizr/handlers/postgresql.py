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
from scalarizr.util import disttool, fstool, cryptotool



SU_EXEC = '/bin/su'
USERMOD = '/usr/sbin/usermod'
USERADD = '/usr/sbin/useradd'

PSQL = '/usr/bin/psql'
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



class PostgreSql(object):
	behaviour = 'postgresql'
	sysconfig_path = '/etc/sysconfig/pgsql/postgresql-9.0'
	
	_objects = None
	_instance = None
	
	def _get_config_dir(self):
		pass
		
	def _set_config_dir(self, path):
		pass
	
	def _get_cluster_dir(self):
		pass
		
	def _set_cluster_dir(self):
		pass
		
	def _get_postmaster_conf(self):
		pass
	
	def _set_postmaster_conf(self):
		pass
		
	def _get_pg_hba_conf(self):
		pass
		
	def _set_pg_hba_conf(self):
		pass
		
	def _get_recovery_conf(self):
		pass
	
	def _set_recovery_conf(self):
		pass
	
	def _get_pid_file_path(self):
		pass
	
	def _set_pid_file_path(self):
		pass

	config_dir = property(_get_config_dir, _set_config_dir)
	cluster_dir = property(_get_cluster_dir, _set_cluster_dir)
	postmaster_conf = property(_get_postmaster_conf, _set_postmaster_conf)
	pg_hba_conf = property(_get_pg_hba_conf, _set_pg_hba_conf)
	recovery_conf = property(_get_recovery_conf, _set_recovery_conf)
	pid_file_path = property(_get_pid_file_path, _set_pid_file_path)					
		
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(PostgreSql, cls).__new__(
								cls, *args, **kwargs)
		return cls._instance
	
	def __init__(self):
		self._objects = {}
		#configure all objects and put them into __object
		
	def create_snapshot(self):
		pass
	
	def init_master(self):
		pass
	
	def init_slave(self):
		pass
	
	def register_slave(self):
		pass
	
	def create_user(self):	
		pass	

	def set_trust_mode(self):
		'''
		pg_hba:
		local all postgres trust
		restart
		'''
		pass
	
	def set_password_mode(self):
		'''
		pg_hba:
		local all postgres password
		restart
		'''
		pass
	
postgresql = PostgreSql()


class PgUser(object):
	
	name = None
	password = None
	psql = None
	
	def __init__(self, name, group='postgres'):
		self.name = name
		self.group = group
		self.psql = PSQL()
	
	def create(self, super=True):	
		self._create_system_user()
		self._create_pg_database()
		self._create_pg_user(super)
	
	def exists(self):
		return self._is_system_user_exist and self._is_role_exist and self._is_pg_database_exist
	
	def generate_password(self, length=20):
		return cryptotool.pwgen(length)
		
		
	def _create_role(self, super=True):
		if self._is_role_exist:
			#log 
			pass
		else:
			#log "Creating role %s" % self.name
			try:
				out = system2([SU_EXEC, '-', self.group, '-c', '%s -s %s' % (CREATEUSER, self.name)])[0]
				#log out or 'Role %s has been successfully created.' % self.name
			except PopenError, e:
				#log 'Unable to create role %s: %s' % (self.name, e)
				pass 
		
	def _create_pg_database(self):
		if self._is_pg_database_exist:
			#log 
			pass
		else:
			#log "Creating database %s" % user
			try:
				out = system2([SU_EXEC, '-', self.group, '-c', '%s %s' % (CREATEDB,self.name)])[0]
				#log out or 'DB %s has been successfully created.' % user
			except PopenError, e:
				#log 'Unable to create db %s: %s' % (user, e)
				pass
	
	def _create_system_user(self, password):
		if self._is_system_user_exist:
			#log 
			pass
		else:
			try:
				out = system2([USERADD, '-g', self.group, '-p', password, self.name])[0]
				#log out	
			except PopenError, e:
				#log 'Unable to create db %s: %s' % (user, e)
				pass

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
		pass
		
	def _delete_role(self):
		pass

	def _delete_pg_database(self):
		pass
	
	def _delete_system_user(self):
		pass	

	def change_password(self, new_pass=None):
		new_pass = new_pass or self.generate_password()
		#change system password
		
		'''
		import subprocess
		
		login = 'username'
		password = 'somepassword'
		
		p = subprocess.Popen(('openssl', 'passwd', '-1', password), stdout=subprocess.PIPE)
		shadow_password = p.communicate()[0].strip()
		if p.returncode != 0:
		    print 'Error creating hash for ' + login
		    
		r = subprocess.call(('usermod', '-p', shadow_password, login))
		if r != 0:
		    print 'Error changing password for ' + login
		'''
		
		#or 
		
		'''
		import pexpect
		import time
		
		def ChangePassword(user, pass):
		    passwd = pexpect.spawn("/usr/bin/passwd %s" % user)
		
		    for x in xrange(2):
		        # wait for password: to come out of passwd's stdout
		        passwd.expect("password: ")
		        # send pass to passwd's stdin
		        passwd.sendline(pass)
		        time.sleep(0.1)
		
		ChangePassword('foo', 'bar') # changes user "foo"'s password to "bar"
		'''
		
		#change password in privated/pgsql.ini
		pass

	def get_password(self):
		#get password from privated/pgsql.ini
		pass
		

class PSQL(object):
	path = '/usr/bin/psql'
	user = None
	
	def __init__(self, user='postgres'):	
		self.user = user
		
	def test_connection(self):
		pass
		
	def execute(self, query):
		try:
			out = system2([SU_EXEC, '-', self.user, '-c', '%s -c "%s"' % (self.path, query)])[0]
			#log out	
		except PopenError, e:
			#log 'Unable to create db %s: %s' % (user, e)
			pass		
	
	
class ClusterDir(object):
	def __init__(self, path=None, user = "postgres"):
		self.path = path or 'find path'
		self.user = user

	def move_to(self, dst):
		new_cluster_dir = os.path.join(dst, 'data')
		if os.path.exists(new_cluster_dir) and os.listdir(new_cluster_dir):
			#log 'cluster seems to be already moved to %s' % dst
			pass
		elif os.path.exists(self.path):
			#log "copying cluster files from %s into %s" % (self.path, new_cluster_dir)
			shutil.copytree(self.path, new_cluster_dir)
		
		#log "changing directory owner to %s" % self.user	
		rchown(self.user, dst)
		
		#log "Changing postgres user`s home directory"
		system2([USERMOD, '-d', new_cluster_dir, self.user]) 
	
		return new_cluster_dir


class ConfigDir(object):
	
	path = None
	user = None
	
	def __init__(self, path=None, user = "postgres"):
		self.path = path
		self.user = user
	
	def move_to(self, dst, sysconfig):
		if not os.path.exists(dst):
			#log "creating %s" % dst
			os.makedirs(dst)
		
		for config in ['postgresql.conf', 'pg_ident.conf', 'pg_hba.conf']:
			old_config = os.path.join(self.path, config)
			new_config = os.path.join(dst, config)
			if os.path.exists(old_config):
				#log 'Moving %s' % config
				shutil.move(old_config, new_config)
			elif os.path.exists(new_config):
				#log '%s is already in place. Skipping.' % config
				pass
			else:
				raise BaseException('Postgresql config file not found: %s' % old_config)
			rchown(self.user, new_config)
		
		#log "configuring pid and cluster dir"
		conf = PostgresqlConf(dst, autosave=False)
		conf.data_directory = self.path
		conf.pid_file = os.path.join(dst, 'postmaster.pid')
		conf.save()
		self._make_symlinks(dst)
		self._patch_sysconfig(sysconfig, dst)
		self.path = dst
		
	def _make_symlinks(self, dst_dir):
		#log "creating symlinks required by initscript"
		for obj in ['base', 'PG_VERSION', 'postmaster.pid']:
			src = os.path.join(self.path, obj)
			dst = os.path.join(dst_dir, obj) 
			if os.path.islink(dst):
				#log "%s exists and it is probably old. Unlinking." % dst
				os.unlink(dst)
			elif os.path.exists(dst):
				#log 'Something wrong: %s is not a symlink. Removing.' % dst
				shutil.rmtree(dst)
				
			#log "Creating symlink %s -> %s" % (src, dst)
			os.symlink(src, dst)
			if os.path.exists(src):
				rchown(self.user, dst)
			else:
				#log 'Warning: %s is a dead link' % dst
				pass
			
	def _patch_sysconfig(self, sysconfig, config_dir):
		if config_dir == get_sysconfig_pgdata():
			#log 'sysconfig file already rewrites PGDATA. Skipping.'
			pass
		else:
			set_sysconfig_pgdata(config_dir)


class PidFile(object):
	path = None
	
	def __init__(self, path):
		self.path = path
	
	@property	
	def proc_id(self):
		return open(self.path, 'r').readline().strip() if os.path.exists(self.path) else None


class BasePGConfig(object):
	'''
	Parent class for object representations of postgresql.conf and recovery.conf which fortunately both have similar syntax
	'''
	
	autosave = None
	path = None
	data = None
	
	def __init(self, path, autosave=True):
		self.autosave = autosave
		self.path = path
		
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
		#must bee boolean? default off
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
		self.set('standby_mode')
	
	def _get_primary_conninfo(self):
		self.get('primary_conninfo')
		
	def _set_primary_conninfo(self, info):
		#check first
		self.set('primary_conninfo')
	
	def _get_trigger_file(self):
		self.get('trigger_file')
	
	def _set_trigger_file(self, path):
		self.set('trigger_file')	
	
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
		
		'host  replication  postgres  %s/22  trust'
		'local all postgres trust'
		'local all postgres password'		
		
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
		#TODO: write unit test on this class
		line = '%s\t%s\t%s' % (self.host, self.database, self.user)
		
		if self.address: line += '\t%s' % self.address
		else:
			if self.ip: line += '\t%s' % self.ip
			if self.mask: line += '\t%s' % self.mask
		
		line +=  '\t%s' % self.auth_method
		if self.auth_options: line += '\t%s' % self.auth_options
			
		return line	
	
		
class PgHbaConf(Configuration):
	
	fname = 'pg_hba.conf'
	path = None
	trusted_mode = PgHbaRecord('local', 'all', 'postgres', auth_method = 'trust')
	password_mode = PgHbaRecord('local', 'all', 'postgres', auth_method = 'password')
	
	def __init__(self, config_dir_path):
		self.config_dir_path = config_dir_path
		self.path = os.path.join(self.fname, config_dir_path)
		self._logger = logging.getLogger(__name__)
	
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
	print "chown -r %s %s" % (user, path)
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
		print 'Cannot chown directory %s : %s' % (path, e)	

def set_sysconfig_pgdata(pgdata):
	print "filling sysconfig"
	sysconf_path = '/etc/sysconfig/pgsql/postgresql-9.0'
	file = open(sysconf_path, 'w')
	file.write('PGDATA=%s' % pgdata)
	file.close()
	
	
def get_sysconfig_pgdata():
	sysconf_path = '/etc/sysconfig/pgsql/postgresql-9.0'
	pgdata = None
	
	if os.path.exists(sysconf_path):
		s = open(sysconf_path, 'r').readline().strip()
		if s and len(s)>7:
			pgdata = s[7:]
		else: 
			print 'sysconfig has no PGDATA'
	return pgdata

# module init	