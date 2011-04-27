'''
Created on April 18th, 2011

@author: Dmytro Korsakov
'''

import os

from scalarizr.libs.metaconf import Configuration
from scalarizr.handlers import ServiceCtlHanler
from scalarizr.util.filetool import read_file, write_file

#__all__ = ['get_handlers', 'cluster_dir', 'config_dir', 'postmaster_conf']

su = '/bin/su'
usermod = '/usr/sbin/usermod'
useradd = '/usr/sbin/useradd'

psql = '/usr/bin/psql'
createuser = '/usr/bin/createuser'
createdb = '/usr/bin/createdb'


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
		
	def make_snapshot(self):
		pass
	
	def init_master(self):
		pass
	
	def init_slave(self):
		pass
	
	def register_slave(self):
		pass
	
	def create_user(self):
		
		def _create_pg_user(self):
			pass
	
		def _create_pg_database(self):
			pass
		
		def _create_system_user(self):
			pass
	
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


class ClusterDir(object):
	def __init__(self, path=None):
		self.path = path or 'find path'

	def move_to(self, dst):
		pass
	
class ConfigDir(object):
	def __init__(self, path=None):
		pass
	
	def move_to(self, dst):
		pass
	
	def _make_symlinks(self):
		pass
	
	def _patch_sysconfig(self):
		pass

class PostgresqlConf(Configuration):

	def _get_pid_file_path(self, path):
		pass
	
	def _set_pid_file_path(self, path):
		#check if path exists
		pass
	
	def _get_data_directory(self, path):
		pass
	
	def _set_data_directory(self, path):
		#check if path exists
		pass
	
	def _get_wal_level(self, path):
		pass
	
	def _set_wal_level(self, path):
		pass
	
	def _get_max_wal_senders(self, path):
		pass
	
	def _set_max_wal_senders(self, path):
		pass
	
	def _get_wal_keep_segments(self, path):
		pass
	
	def _set_wal_keep_segments(self, path):
		pass
	
	def _get_listen_addresses(self, path):
		pass
	
	def _set_listen_addresses(self, path):
		pass
	
	def _get_hot_standby(self, path):
		pass
	
	def _set_hot_standby(self, path):
		pass
		
	pid_file = property(_get_pid_file_path, _set_pid_file_path)
	data_directory = property(_get_data_directory, _set_data_directory)
	wal_level = property(_get_wal_level, _set_wal_level)
	max_wal_senders = property(_get_max_wal_senders, _set_max_wal_senders)
	wal_keep_segments = property(_get_wal_keep_segments, _set_wal_keep_segments)
	listen_addresses = property(_get_listen_addresses, _set_listen_addresses)
	hot_standby = property(_get_hot_standby, _set_hot_standby)
	
class RecoveryConf(Configuration):
	pass
	'''
	standby_mode
	primary_conninfo
	trigger_file
	'''

	
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
	
	def add_record(self, record):
		text = read_file(self.path) or ''
		for line in text.splitlines():
			if not line.strip().startswith('#') and PgHbaRecord.fromstring(line) == record:
				#already in file
				return
		write_file(self.path, str(record), 'a')	
			
	def delete_record(self, record):
		lines = []
		text = read_file(self.path)
		for line in text.splitlines():
			if line.strip().startswith('#') or PgHbaRecord.fromstring(line) == record:
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

	

# module init	