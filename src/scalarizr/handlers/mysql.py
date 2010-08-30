'''
Created on 14.06.2010

@author: spike
@author: marat
'''
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours, Configurator, ScalarizrState
from scalarizr.util import validators

from scalarizr.handlers import Handler, HandlerError
from scalarizr.util import fstool, system, cryptotool, initd, disttool,\
		configtool, filetool, ping_service, firstmatched, cached
from scalarizr.platform.ec2 import s3tool, UD_OPT_S3_BUCKET_NAME
from distutils import version
from subprocess import Popen, PIPE, STDOUT
import logging, os, re,  pexpect, tarfile, tempfile
import time
import signal, pwd, random
import shutil, ConfigParser
from boto.exception import BotoServerError


BEHAVIOUR = BuiltinBehaviours.MYSQL
CNF_SECTION = BEHAVIOUR
CNF_NAME = BEHAVIOUR


if disttool.is_redhat_based():
	initd_script = "/etc/init.d/mysqld"
elif disttool.is_debian_based():
	initd_script = "/etc/init.d/mysql"
else:
	initd_script = "/etc/init.d/mysql"
	
if not os.path.exists(initd_script):
	raise HandlerError("Cannot find MySQL init script at %s. Make sure that mysql server is installed" % initd_script)

pid_file = None
try:
	out = system("my_print_defaults mysqld")
	m = re.search("--pid[-_]file=(.*)", out[0], re.MULTILINE)
	if m:
		pid_file = m.group(1)
except:
	pass

# Register mysql service
logger = logging.getLogger(__name__)
logger.debug("Explore MySQL service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("mysql", initd_script, pid_file, tcp_port=3306)


class MysqlOptions(Configurator.Container):
	'''
	mysql behaviour
	'''
	class mysqld_path(Configurator.Option):
		'''
		MySQL daemon binary path
		'''
		name = CNF_NAME + '/mysqld_path'
		required = True

		@property
		@cached
		def default(self):
			return firstmatched(lambda p: os.access(p, os.F_OK | os.X_OK), 
					('/usr/libexec/mysqld', '/usr/sbin/mysqld'), '')
		
		@validators.validate(validators.executable)
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
		
		value = property(Configurator.Option._get_value, _set_value)
			
	class mysql_path(Configurator.Option):
		'''
		MySQL command line tool path
		'''
		name = CNF_NAME + '/mysql_path'
		default = '/usr/bin/mysql'
		required = True

		@validators.validate(validators.executable)
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
			
		value = property(Configurator.Option._get_value, _set_value)

	class mysqldump_path(Configurator.Option):
		'''
		Path to the mysqldump utility
		'''
		name = CNF_NAME + '/mysqldump_path'
		default = '/usr/bin/mysqldump'
		required = True
		
		@validators.validate(validators.executable)
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
			
		value = property(Configurator.Option._get_value, _set_value)
			
	class mycnf_path(Configurator.Option):
		'''
		MySQL configuration file path
		'''
		name = CNF_NAME + '/mycnf_path'
		required = True

		@property		
		@cached
		def default(self):
			return firstmatched(lambda p: os.access(p, os.F_OK), 
					('/etc/my.cnf', '/etc/mysql/my.cnf'), '')
			
		@validators.validate(validators.file_exists)
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
		
		value = property(Configurator.Option._get_value, _set_value)

# Configuration options
OPT_ROOT_USER   		= "root_user"
OPT_ROOT_PASSWORD   	= "root_password"
OPT_REPL_USER   		= "repl_user"
OPT_REPL_PASSWORD   	= "repl_password"
OPT_STAT_USER   		= "stat_user"
OPT_STAT_PASSWORD   	= "stat_password"
OPT_REPLICATION_MASTER  = "replication_master"
OPT_SNAPSHOT_ID			= "snapshot_id"
OPT_STORAGE_VOLUME_ID	= "volume_id" 
OPT_LOG_FILE 			= "log_file"
OPT_LOG_POS				= "log_pos"

OPT_MYSQLD_PATH 		= 'mysqld_path'
OPT_MYCNF_PATH 			= 'mycnf_path'

# Role params
PARAM_MASTER_EBS_VOLUME_ID 	= "mysql_master_ebs_volume_id"
PARAM_DATA_STORAGE_ENGINE 	= "mysql_data_storage_engine"


ROOT_USER = "scalr"
REPL_USER = "scalr_repl"
STAT_USER = "scalr_stat"
PMA_USER = "pma"

STORAGE_DEVNAME = "/dev/sdo"
STORAGE_PATH = "/mnt/dbstorage"
STORAGE_DATA_DIR = "mysql-data"
STORAGE_BINLOG_PATH = "mysql-misc/binlog.log"
BACKUP_CHUNK_SIZE = 200*1024*1024


def get_handlers ():
	return [MysqlHandler()]

class MysqlMessages:
	CREATE_DATA_BUNDLE = "Mysql_CreateDataBundle"
	CREATE_DATA_BUNDLE_RESULT = "Mysql_CreateDataBundleResult"
	
	CREATE_BACKUP = "Mysql_CreateBackup"
	
	CREATE_BACKUP_RESULT = "Mysql_CreateBackupResult"
	"""
	@ivar status: ok|error
	@ivar last_error
	@ivar backup_urls: S3 URL
	"""
	
	CREATE_PMA_USER = "Mysql_CreatePmaUser"
	"""
	@ivar pma_server_ip: User host
	@ivar farm_role_id
	@ivar root_password
	"""
	
	CREATE_PMA_USER_RESULT = "Mysql_CreatePmaUserResult"
	"""
	@ivar status: ok|error
	@ivar last_error
	@ivar pma_user
	@ivar pma_password
	@ivar farm_role_id
	"""
	
	PROMOTE_TO_MASTER	= "Mysql_PromoteToMaster"
	"""
	@ivar root_password: 'scalr' user password 
	@ivar repl_password: 'scalr_repl' user password
	@ivar stat_password: 'scalr_stat' user password
	@ivar volume_id: Master EBS volume id
	"""
	
	PROMOTE_TO_MASTER_RESULT = "Mysql_PromoteToMasterResult"
	"""
	@ivar status: ok|error
	@ivar last_error: Last error message in case of status = 'error'
	@ivar volume_id: Master EBS volume id
	"""
	
	NEW_MASTER_UP = "Mysql_NewMasterUp"
	"""
	@ivar behaviour
	@ivar local_ip
	@ivar remote_ip
	@ivar role_name		
	@ivar repl_password
	"""
	
	"""
	Also MySQL behaviour adds params to common messages:
	
	= HOST_INIT_RESPONSE =
	@ivar mysql=dict(
		replication_master: 	1|0
		volume_id				EBS volume id					(on master)
		snapshot_id: 			Master EBS snapshot id			(on slave)
		root_password:			'scalr' user password  			(on slave)
		repl_password:			'scalr_repl' user password		(on slave)
		stat_password: 			'scalr_stat' user password		(on slave)
		log_file:				Binary log file					(on slave)
		log_pos:				Binary log file position		(on slave)
	)
	
	= HOST_UP =
	@ivar mysql=dict(
		root_password: 	'scalr' user password  					(on master)
		repl_password: 	'scalr_repl' user password				(on master)
		stat_password: 	'scalr_stat' user password				(on master)
		snapshot_id: 	Data volume EBS snapshot				(on master)		 
		log_file: 		Binary log file							(on master) 
		log_pos: 		Binary log file position				(on master)
		volume_id:		EBS volume created from master snapshot (on slave)
		) 
	"""



class MysqlHandler(Handler):
	_logger = None
	
	_queryenv = None
	""" @type _queryenv: scalarizr.queryenv.QueryEnvService	"""
	
	_platform = None
	""" @type _platform: scalarizr.platform.Ec2Platform """
	
	_cnf = None
	''' @type _cnf: scalarizr.config.ScalarizrCnf '''
	
	_storage_path = _data_dir = _binlog_path = None
	""" Storage parameters """
	
	_mycnf_path = None
	_mysqld_path = None

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		self._sect_name = configtool.get_behaviour_section_name(BEHAVIOUR)
		self._sect = configtool.section_wrapper(bus.config, self._sect_name)
		config = bus.config
		self._role_name = config.get(configtool.SECT_GENERAL, configtool.OPT_ROLE_NAME)
		self._mycnf_path = config.get(CNF_SECTION, OPT_MYCNF_PATH)
		self._mysqld_path = config.get(CNF_SECTION, OPT_MYSQLD_PATH)
		
		self._storage_path = STORAGE_PATH
		self._data_dir = os.path.join(self._storage_path, STORAGE_DATA_DIR)
		self._binlog_path = os.path.join(self._storage_path, STORAGE_BINLOG_PATH)
		
		bus.on("init", self.on_init)

	def on_init(self):
		bus.on("start", self.on_start)		
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("before_host_down", self.on_before_host_down)
		
		"""
		@xxx: Storage unplug failed because scalarizr has no EC2 access keys
		bus.on("before_reboot_start", self.on_before_reboot_start)
		bus.on("before_reboot_finish", self.on_before_reboot_finish)
		"""

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and (
					message.name == MysqlMessages.NEW_MASTER_UP
				or 	message.name == MysqlMessages.PROMOTE_TO_MASTER
				or 	message.name == MysqlMessages.CREATE_DATA_BUNDLE
				or 	message.name == MysqlMessages.CREATE_BACKUP
				or 	message.name == MysqlMessages.CREATE_PMA_USER)
		
	def on_Mysql_CreatePmaUser(self, message):
		try:
			if not int(self._sect.get(OPT_REPLICATION_MASTER)):
				raise HandlerError('Cannot add pma user on slave')			
			try:
				root_password = self._sect.get(OPT_ROOT_PASSWORD)
			except Exception, e:
				raise HandlerError('Cannot retrieve mysql password from config: %s' % (e,))
			pma_server_ip = message.pma_server_ip
			farm_role_id  = message.farm_role_id
			
			self._logger.info("Adding phpMyAdmin system user")
			
			# Connecting to mysql 
			myclient = pexpect.spawn('/usr/bin/mysql -u'+ROOT_USER+' -p')
			myclient.expect('Enter password:')
			myclient.sendline(root_password)
			
			# Retrieveing line with version info				
			myclient.expect('mysql>')
			myclient.sendline('SELECT VERSION();')
			myclient.expect('mysql>')
			mysql_ver_str = re.search(re.compile('\d*\.\d*\.\d*', re.MULTILINE), myclient.before)

			# Determine mysql server version 
			if mysql_ver_str:
				mysql_ver = version.StrictVersion(mysql_ver_str.group(0))
				priv_count = 28 if mysql_ver >= version.StrictVersion('5.1.6') else 26
			else:
				raise HandlerError("Cannot extract mysql version from string '%s'" % myclient.before)
			
			# Generating password for pma user
			pma_password = re.sub('[^\w]','', cryptotool.keygen(20))
			# Generating sql statement, which depends on mysql server version 
			sql = "INSERT INTO mysql.user VALUES('"+pma_server_ip+"','"+PMA_USER+"',PASSWORD('"+pma_password+"')" + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
			# Pass statement to mysql client
			myclient.sendline(sql)
			myclient.expect('mysql>')
			
			# Check for errors
			if re.search('error', myclient.before, re.M | re.I):
				raise HandlerError("Cannot add pma user '%s': '%s'" % (PMA_USER, myclient.before))
			
			myclient.sendline('FLUSH PRIVILEGES;')
			myclient.terminate()
			del(myclient)
			
			self._send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
				status       = 'ok',
				pma_user	 = PMA_USER,
				pma_password = pma_password,
				farm_role_id = farm_role_id,
			))
			
		except (Exception, BaseException), e:
			self._send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
				status		= 'error',
				last_error	=  str(e),
				farm_role_id = farm_role_id
			))
	
	def on_Mysql_CreateBackup(self, message):
		
		# Retrieve password for scalr mysql user
		tmpdir = backup_path = None
		try:
			# Do backup only if slave
			if int(self._sect.get(OPT_REPLICATION_MASTER)):
				raise HandlerError('Cannot create databases backup on mysql master')
			
			try:
				root_password = self._sect.get(OPT_ROOT_PASSWORD)
			except Exception, e:
				raise HandlerError('Cannot retrieve mysql password from config: %s' % (e,))
			# Creating temp dir 
			tmpdir = tempfile.mkdtemp()
			
			# Reading mysql config file
			mysql_config = filetool.read_file(self._mycnf_path, self._logger)
			
			if not mysql_config:
				raise HandlerError('Cannot read mysql config file %s' % (self._mycnf_path,))
			
			# Retrieveing datadir 
			datadir_re = re.compile("^\s*datadir\s*=\s*(?P<datadir>.*?)$", re.M)	
			result = re.search(datadir_re, mysql_config)			
			
			if not result:
				raise HandlerError('Cannot get mysql data directory from mysql config file')
			
			datadir = result.group('datadir').strip()
			
			# Defining archive name and path
			backup_filename = 'mysql-backup-'+time.strftime('%Y-%m-%d')+'.tar.gz'
			backup_path = os.path.join('/tmp', backup_filename)
			
			# Creating archive 
			backup = tarfile.open(backup_path, 'w:gz')

			# Dump all databases
			self._logger.info("Dumping all databases")
			data_list = os.listdir(datadir)					
			for file in data_list:
				
				if not os.path.isdir(os.path.join(datadir, file)):
					continue
					
				db_name = os.path.basename(file)
				dump_path = tmpdir + os.sep + db_name + '.sql'
				mysql = pexpect.spawn('/bin/sh -c "/usr/bin/mysqldump -u ' + ROOT_USER + ' -p --create-options' + 
									  ' --add-drop-database -q -Q --flush-privileges --databases ' + 
									  db_name + '>' + dump_path +'"')
				mysql.expect('Enter password:')
				mysql.sendline(root_password)
				
				status = mysql.read()
				if re.search(re.compile('error', re.M | re.I), status):
					raise HandlerError('Error while dumping database %s: %s' % (file, status))
				
				backup.add(dump_path, os.path.basename(dump_path))
				
				mysql.close()
				del(mysql)
			
			backup.close()
			
			# Creating list of full paths to archive chunks
			if os.path.getsize(backup_path) > BACKUP_CHUNK_SIZE:
				parts = [os.path.join(tmpdir, file) for file in filetool.split(backup_path, backup_filename, BACKUP_CHUNK_SIZE , tmpdir)]
			else:
				parts = [backup_path]
					
			self._logger.info("Uploading backup to S3")
			s3_conn = self._platform.new_s3_conn()
			bucket_name = self._platform.get_user_data(UD_OPT_S3_BUCKET_NAME)
			bucket = s3_conn.get_bucket(bucket_name)
			
			uploader = s3tool.S3Uploader()
			result = uploader.upload(parts, bucket, s3_conn)
			self._logger.debug("Backup files(s) uploaded to S3 (%s)", ", ".join(result))
			
			self._send_message(MysqlMessages.CREATE_BACKUP_RESULT, dict(
				status		= 'ok',
				backup_urls	=  result
			))
						
		except (Exception, BaseException), e:
			self._send_message(MysqlMessages.CREATE_BACKUP_RESULT, dict(
				status		= 'error',
				last_error	=  str(e)
			))
		finally:
			if tmpdir:
				shutil.rmtree(tmpdir, ignore_errors=True)
			if backup_path and os.path.exists(backup_path):
				os.remove(backup_path)				
						

	def on_Mysql_CreateDataBundle(self, message):
		# Retrieve password for scalr mysql user
		try:
			try:
				root_password = self._sect.get(OPT_ROOT_PASSWORD)
			except Exception, e:
				raise HandlerError('Cannot retrieve mysql login and password from config: %s' % (e,))
			# Creating snapshot
			(snap_id, log_file, log_pos) = self._create_snapshot(ROOT_USER, root_password)
			# Sending snapshot data to scalr
			self._send_message(MysqlMessages.CREATE_DATA_BUNDLE_RESULT, dict(
				snapshot_id=snap_id,
				log_file=log_file,
				log_pos=log_pos,
				status='ok'			
			))
		except (Exception, BaseException), e:
			self._send_message(MysqlMessages.CREATE_DATA_BUNDLE_RESULT, dict(
				status		='error',
				last_error	= str(e)
			))

				
	def on_Mysql_PromoteToMaster(self, message):
		"""
		Promote slave to master
		@type message: scalarizr.messaging.Message
		@param message: Mysql_PromoteToMaster
		"""
		if not int(self._sect.get(OPT_REPLICATION_MASTER)):
			
			ec2_conn = self._platform.new_ec2_conn()
			slave_vol_id = 	self._sect.get(OPT_STORAGE_VOLUME_ID)
			master_vol_id = self._queryenv.list_role_params(self._role_name)[PARAM_MASTER_EBS_VOLUME_ID]
			master_vol = None
			tx_complete = False
			
			try:
				# Stop mysql
				if initd.is_running("mysql"):
					mysql = self._spawn_mysql(ROOT_USER, message.root_password)
					timeout = 180
					try:
						mysql.sendline("STOP SLAVE;")
						mysql.expect("mysql>", timeout=timeout)
					except pexpect.TIMEOUT:
						raise HandlerError("Timeout (%d seconds) reached " + 
								"while waiting for slave stop" % (timeout,))
					finally:
						mysql.close()
					self._stop_mysql()
					
				# Unplug slave storage and plug master one
				self._unplug_storage(slave_vol_id, self._storage_path)
				master_vol = self._take_master_volume(master_vol_id)
				self._plug_storage(master_vol.id, self._storage_path)
				
				# Continue if master storage is a valid MySQL storage 
				if self._storage_valid():
					# Patch configuration files 
					self._move_mysql_dir('log_bin', self._binlog_path, 'mysqld')
					self._move_mysql_dir('datadir', self._data_dir + os.sep, 'mysqld')
					self._replication_init()
					# Update behaviour configuration
					updates = {
						OPT_ROOT_PASSWORD : message.root_password,
						OPT_REPL_PASSWORD : message.repl_password,
						OPT_STAT_PASSWORD : message.stat_password,
						OPT_STORAGE_VOLUME_ID : master_vol.id,
						OPT_REPLICATION_MASTER 	: "1"
					}
					self._update_config(updates)
					# Send message to Scalr
					self._send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, dict(
						status="ok",
						volume_id=master_vol.id																				
					))
				else:
					raise HandlerError("%s is not a valid MySQL storage" % self._storage_path)
				tx_complete = True
			except (Exception, BaseException), e:
				self._logger.error("Promote to master failed. %s", e)

				# Get back slave storage
				self._plug_storage(slave_vol_id, self._storage_path)
				
				if master_vol and master_vol.id != master_vol_id:
					ec2_conn.delete_volume(master_vol.id)
				
				self._send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, dict(
					status="error",
					last_error=str(e)
				))

			
			# Start MySQL
			self._start_mysql()				
			
			if tx_complete:
				# Delete slave EBS
				ec2_conn.delete_volume(slave_vol_id)
			
		else:
			self._logger.warning('Cannot promote to master. Already master')


	def on_Mysql_NewMasterUp(self, message):
		"""
		Switch replication to a new master server
		@type message: scalarizr.messaging.Message
		@param message:  Mysql_NewMasterUp
		"""
		if not int(self._sect.get(OPT_REPLICATION_MASTER)):
			host = message.local_ip or message.remote_ip
			self._logger.info("Switching replication to a new MySQL master %s", host)
			mysql = self._spawn_mysql(ROOT_USER, message.root_password)
						
			self._logger.debug("Stopping slave i/o thread")
			mysql.sendline("STOP SLAVE IO_THREAD;")
			mysql.expect("mysql>")
			self._logger.debug("Slave i/o thread stopped")
			
			self._logger.debug("Retrieving current log_file and log_pos")
			mysql.sendline("SHOW SLAVE STATUS\\G");
			mysql.expect("mysql>")
			log_file = log_pos = None
			for line in mysql.before.split("\n"):
				pair = map(str.strip, line.split(": ", 1))
				if pair[0] == "Master_Log_File":
					log_file = pair[1]
				elif pair[0] == "Read_Master_Log_Pos":
					log_pos = pair[1]
			self._logger.debug("Retrieved log_file=%s, log_pos=%s", log_file, log_pos)

			self._change_master(
				host=host, 
				user=REPL_USER, 
				password=message.repl_password,
				log_file=log_file, 
				log_pos=log_pos, 
				mysql_user=ROOT_USER,
				mysql_password=message.root_password
			)			
			self._logger.debug("Replication switched")
		else:
			self._logger.debug('Skip NewMasterUp. My replication role is master')		

	def on_start(self):
		if self._cnf.state == ScalarizrState.RUNNING:
			try:
				self._start_mysql()
			except initd.InitdError, e:
				self._logger.error(e)
				
	def on_before_host_down(self, *args):
		self._stop_mysql()	
	
	def on_before_reboot_start(self, *args, **kwargs):
		"""
		Stop MySQL and unplug storage
		"""
		self._stop_mysql()
		'''
		no need to plug/unplug storage since Scalarizr do EBS-root instances bundle 
		try:
			self._unplug_storage(self._sect.get(OPT_STORAGE_VOLUME_ID), self._storage_path)
		except ConfigParser.NoOptionError:
			self._logger.debug("Skip storage unplug. There is no configured storage.")
		'''

	def on_before_reboot_finish(self, *args, **kwargs):
		"""
		Start MySQL and plug storage
		"""
		'''
		try:
			self._plug_storage(self._sect.get(OPT_STORAGE_VOLUME_ID), self._storage_path)
		except ConfigParser.NoOptionError:
			self._logger.debug("Skip storage plug. There is no configured storage.")
		'''
		self._start_mysql()

	def on_host_init_response(self, message):
		"""
		Check mysql data in host init response
		@type message: scalarizr.messaging.Message
		@param message: HostInitResponse
		"""
		if not message.body.has_key("mysql"):
			raise HandlerError("HostInitResponse message for MySQL behaviour must have 'mysql' property")
		self._logger.debug("Update mysql config with %s", message.mysql)
		self._update_config(message.mysql)
		

	def on_before_host_up(self, message):
		"""
		Configure MySQL behaviour
		@type message: scalarizr.messaging.Message		
		@param message: HostUp message
		"""
		
		#role_params = self._queryenv.list_role_params(self._role_name)
		#if role_params[PARAM_DATA_STORAGE_ENGINE]:
		try:
			out = system("my_print_defaults mysqld")
			result = re.search("--datadir=(.*)", out[0], re.MULTILINE)
			if result:
				datadir = result.group(1)
				if os.path.isdir(datadir) and not os.path.isdir(os.path.join(datadir, 'mysql')):
					self._start_mysql()	
					self._stop_mysql()				
		except:
			pass

		if int(self._sect.get(OPT_REPLICATION_MASTER)):
			self._init_master(message)									  
		else:
			self._init_slave(message)		
		
	
	def _init_master(self, message):
		"""
		Initialize MySQL master
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		self._logger.info("Initializing MySQL master")
		
		# Mount EBS
		self._plug_storage(self._sect.get(OPT_STORAGE_VOLUME_ID), self._storage_path)
		
		# Stop MySQL server
		self._stop_mysql()
		self._flush_logs()
		
		msg_data = None
		storage_valid = self._storage_valid() # It's important to call it before _move_mysql_dir

		
		# Patch configuration
		self._move_mysql_dir('datadir', self._data_dir + os.sep, 'mysqld')
		self._move_mysql_dir('log_bin', self._binlog_path, 'mysqld')

				
		self._replication_init(master=True)
		
		# If It's 1st init of mysql master
		if not storage_valid:
			
			if os.path.exists('/etc/mysql/debian.cnf'):
				try:
					self._logger.debug("Copying debian.cnf file to storage")
					shutil.copy('/etc/mysql/debian.cnf', STORAGE_PATH)
				except BaseException, e:
					self._logger.error("Cannot copy debian.cnf file to storage: ", e)
					
			root_password, repl_password, stat_password = \
					self._add_mysql_users(ROOT_USER, REPL_USER, STAT_USER)
			
			# Get binary logfile, logpos and create data snapshot if needed
			snap_id, log_file, log_pos = self._create_snapshot(ROOT_USER, root_password)

			msg_data = dict(
				root_password=root_password,
				repl_password=repl_password,
				stat_password=stat_password,
				snapshot_id=snap_id,
				log_file=log_file,
				log_pos=log_pos			
			)
			
		# If EBS volume had mysql dirs (N-th init)
		else:
			# Retrieve scalr's mysql username and password
			try:
				root_password = self._sect.get(OPT_ROOT_PASSWORD)
			except Exception, e:
				raise HandlerError('Cannot retrieve mysql login and password from config: %s' % (e,))
			
			if disttool._is_debian_based and os.path.exists(os.path.join(STORAGE_PATH, 'debian.cnf')):
				try:
					self._logger.debug("Copying debian.cnf file from storage")
					shutil.copy(os.path.join(STORAGE_PATH, 'debian.cnf'), '/etc/mysql/')
				except BaseException, e:
					self._logger.error("Cannot copy debian.cnf file from storage: ", e)
			
			# Updating snapshot
			snap_id, log_file, log_pos = self._create_snapshot(ROOT_USER, root_password)
			
			# Send updated metadata to Scalr
			msg_data = dict(snapshot_id=snap_id, log_file=log_file, log_pos=log_pos)
			
		if msg_data:
			message.mysql = msg_data
			self._update_config(msg_data)
			
		self._start_mysql()			
			
			
	
	def _init_slave(self, message):
		"""
		Initialize MySQL slave
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		self._logger.info("Initializing MySQL slave")
		if not self._storage_valid():
			self._logger.debug("Initialize slave storage")
			
			ebs_volume = self._create_volume_from_snapshot(self._sect.get(OPT_SNAPSHOT_ID))
			message.mysql = dict(volume_id = ebs_volume.id)
			self._update_config({OPT_STORAGE_VOLUME_ID : ebs_volume.id})
			
			# Waiting until ebs volume will be created							
			#if "ebs" == role_params[PARAM_DATA_STORAGE_ENGINE]:
			self._plug_storage(None, self._storage_path, vol=ebs_volume)

			"""	
			elif "eph" == role_params[PARAM_DATA_STORAGE_ENGINE]:
				# Mount ephemeral device
				try:
					devname = '/dev/' + self._platform.get_block_device_mapping()["ephemeral0"]
				except Exception, e:
					raise HandlerError('Cannot retrieve ephemeral device info. %s' % (e,))
				self._mount_device(devname, self._storage_path)
				
				# Mount EBS with mysql data
				tmpdir = '/mnt/tmpdir'
				self._plug_storage(ebs_volume.id, tmpdir)
				if self._storage_valid(tmpdir):
					# Rsync data from ebs to ephemeral device
					rsync = filetool.Rsync().archive()
					rsync.source(tmpdir + os.sep).dest(self._storage_path + os.sep)
					out, err, retcode = system(str(rsync))
					if err:
						raise HandlerError("Cannot copy data from ebs to ephemeral: %s" % (err,))
					# Detach and delete EBS Volume 
					self._detach_delete_volume(ebs_volume)
					shutil.rmtree(tmpdir)
				else:
					raise HandlerError("EBS Volume does not contain mysql data")
			"""
			
		self._stop_mysql()			
		self._flush_logs()
		# Change configuration files
		self._logger.info("Changing configuration files")
		self._move_mysql_dir('datadir', self._data_dir, 'mysqld')
		self._move_mysql_dir('log_bin', self._binlog_path, 'mysqld')
		self._replication_init(master=False)
		if disttool._is_debian_based and os.path.exists(STORAGE_PATH + os.sep +'debian.cnf') :
			try:
				self._logger.debug("Copying debian.cnf from storage to mysql configuration directory")
				shutil.copy(os.path.join(STORAGE_PATH, 'debian.cnf'), '/etc/mysql/')
			except BaseException, e:
				self._logger.error("Cannot copy debian.cnf file from storage: ", e)
				
					
		self._start_mysql()
		
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
		self._change_master(
			host=host, 
			user=REPL_USER, 
			password=self._sect.get(OPT_REPL_PASSWORD),
			log_file=self._sect.get(OPT_LOG_FILE), 
			log_pos=self._sect.get(OPT_LOG_POS), 
			mysql_user=ROOT_USER,
			mysql_password=self._sect.get(OPT_ROOT_PASSWORD)
		)
		
	def _plug_storage(self, vol_id, mnt_point, vol=None):
		# Getting free letter for device
		dev_list = os.listdir('/dev')
		for letter in map(chr, range(111, 123)):
			device = 'sd'+letter
			if not device in dev_list:
				devname = '/dev/'+device
				break

		self._logger.info("Create EBS storage (volume: %s, devname: %s) and mount to %s", 
				vol.id if vol else vol_id, devname, mnt_point)

		ec2_conn = self._get_ec2_conn()
		if not vol:
			try:
				vol = ec2_conn.get_all_volumes([vol_id])[0]
			except IndexError:
				raise HandlerError("Volume %s not found" % vol_id)

		if 'available' != vol.volume_state():
			self._logger.warning("Volume %s is not available. Force detach it from instance", vol.id)
			vol.detach(force=True)
			self._logger.debug('Checking that volume %s is available', vol.id)
			self._wait_until(lambda: vol.update() == "available")
			self._logger.debug("Volume %s available", vol.id)

		# Attach ebs
		self._logger.debug("Attaching volume %s as device %s", vol.id, devname)
		try:
			vol.attach(self._platform.get_instance_id(), devname)
		except BotoServerError, e:
			if e.code == "VolumeInUse":
				# Sometimes this happens when plugging storage from crashed master server.
				self._logger.warning("Volume status is 'available' " + 
						"but attach operation failed with 'VolumeInUse' error. " +
						"Sometimes this happens when plugging storage from crashed MySQL master server.")
				try:
					self._logger.debug("Force detaching volume %s", vol.id)
					vol.detach(True)
					self._logger.debug("Volume %s detached", vol.id)
				except BotoServerError, e:
					pass
				time.sleep(5)
				self._logger("Attaching volume %s as device %s", vol.id, devname)
				vol.attach(self._platform.get_instance_id(), devname)
			else:
				raise
		self._logger.debug("Checking that volume %s is attached", vol.id)
		self._wait_until(lambda: vol.update() and vol.attachment_state() == "attached")
		self._logger.debug("Volume %s attached",  vol.id)
			
		# Wait when device will be added
		self._logger.debug("Checking that device %s is available", devname)
		self._wait_until(lambda: os.access(devname, os.F_OK | os.R_OK))
		self._logger.debug("Device %s is available", devname)
		
		# Mount EBS
		self._mount_device(devname, mnt_point)


	def _unplug_storage(self, vol_id, mnt_point, vol=None):
		self._logger.info("Unplug EBS storage (volume: %s) from mpoint %s", 
				vol.id if vol else vol_id, mnt_point)
		
		ec2_conn = self._get_ec2_conn()
		if not vol:
			try:
				vol = ec2_conn.get_all_volumes([vol_id])[0]
			except IndexError:
				raise HandlerError("Volume %s not found" % vol_id)		
		
		# Unmount volume
		if os.path.ismount(self._storage_path):
			self._logger.debug("Unmounting storage %s", self._storage_path)
			fstool.umount(self._storage_path, clean_fstab=True)
			self._logger.debug("Storage %s unmounted", self._storage_path)
		
		# Detach volume
		self._logger.debug("Detaching storage volume %s", vol.id)
		vol.detach()
		self._wait_until(lambda: vol.update() == "available")
		self._logger.debug("Volume %s detached", vol.id)

	
	def _storage_valid(self, path=None):
		data_dir = os.path.join(path, STORAGE_DATA_DIR) if path else self._data_dir
		binlog_path = os.path.join(path, STORAGE_BINLOG_PATH) if path else os.path.dirname(self._binlog_path)
		return os.path.exists(data_dir) and os.path.exists(binlog_path)
	
	def _create_volume_from_snapshot(self, snap_id, avail_zone=None):
		ec2_conn = self._get_ec2_conn()
		avail_zone = avail_zone or self._platform.get_avail_zone()
		
		self._logger.debug("Creating EBS volume from snapshot %s in avail zone %s", snap_id, avail_zone)
		ebs_volume = ec2_conn.create_volume(None, avail_zone, snap_id)
		self._logger.debug("Volume %s created from snapshot %s", ebs_volume.id, snap_id)
		
		self._logger.debug('Checking that EBS volume %s is available', ebs_volume.id)
		self._wait_until(lambda: ebs_volume.update() == "available")
		self._logger.debug("Volume %s available", ebs_volume.id)
		
		return ebs_volume
	
	def _wait_until(self, target, args=None, sleep=5):
		args = args or ()
		while not target(*args):
			self._logger.debug("Wait %d seconds before the next attempt", sleep)
			time.sleep(sleep)
	
	def _detach_delete_volume(self, volume):
		if volume.detach():
			if not volume.delete():
				raise HandlerError("Cannot delete volume ID=%s", (volume.id,))
		else:
			raise HandlerError("Cannot detach volume ID=%s" % (volume.id,))

	def _take_master_volume(self, volume_id):
		# Lookup master volume
		self._logger.debug("Taking master EBS volume %s", volume_id)
		ec2_conn = self._get_ec2_conn()
		zone = self._platform.get_avail_zone()						
		try:
			master_vol = ec2_conn.get_all_volumes([volume_id])[0]
		except IndexError:
			raise HandlerError("Cannot find volume %s in EBS volumes list" % volume_id)

		# For EBS in another avail zone we need to snapshot it
		# and create EBS in our avail zone
		self._logger.debug("Taked master volume %s (zone: %s)", master_vol.id, master_vol.zone)
		if master_vol.zone != zone:
			self._logger.debug("Master volume is in another zone (volume zone: %s, server zone: %s) " + 
					"Creating volume in %s zone", 
					master_vol.id, zone, zone)
			self._logger.debug("Creating snapshot from volume %s", master_vol.id)
			master_snap = ec2_conn.create_snapshot(master_vol.id)
			self._logger.debug("Snapshot %s created from volume %s", master_snap.id, master_vol.id)
			try:
				master_vol = self._create_volume_from_snapshot(master_snap.id, zone)
			finally:
				self._logger.debug("Deleting snapshot %s", master_snap.id)
				master_snap.delete()
				self._logger.debug("Snapshot %s deleted", master_snap.id)
				
			self._logger.debug("Use %s as master data volume", master_vol.id)
		
		return master_vol

	def _create_snapshot(self, root_user, root_password, dry_run=False):
		was_running = initd.is_running("mysql")
		try:
			if not was_running:
				self._start_mysql()
				self._ping_mysql()
			
			# Lock tables
			sql = self._spawn_mysql(root_user, root_password)
			sql.sendline('FLUSH TABLES WITH READ LOCK;')
			sql.expect('mysql>')
			system('sync')
			if int(self._sect.get(OPT_REPLICATION_MASTER)):
				sql.sendline('SHOW MASTER STATUS;')
				sql.expect('mysql>')
				
				# Retrieve log file and log position
				lines = sql.before		
				log_row = re.search(re.compile('^\|\s*([\w-]*\.\d*)\s*\|\s*(\d*)', re.M), lines)
				if log_row:
					log_file = log_row.group(1)
					log_pos = log_row.group(2)
				else:
					log_file = log_pos = None
			else:
				sql.sendline('SHOW SLAVE STATUS \G')
				sql.expect('mysql>')
				lines = sql.before
				log_row = re.search(re.compile('Relay_Master_Log_File:\s*(.*?)$.*?Exec_Master_Log_Pos:\s*(.*?)$', re.M | re.S), lines)
				if log_row:
					log_file = log_row.group(1).strip()
					log_pos = log_row.group(2).strip()
				else:
					log_file = log_pos = None

			# Creating EBS snapshot
			snap_id = None if dry_run else self._create_ebs_snapshot()
	
			sql.sendline('UNLOCK TABLES;\n')
			sql.close()
			return snap_id, log_file, log_pos
		
		finally:
			if not was_running:
				self._stop_mysql()

			
	def _create_ebs_snapshot(self):
		self._logger.info("Creating storage EBS snapshot")
		try:
			ec2_conn = self._get_ec2_conn()
			""" @type ec2_conn: boto.ec2.connection.EC2Connection """
			
			snapshot = ec2_conn.create_snapshot(self._sect.get(OPT_STORAGE_VOLUME_ID))
			self._logger.debug("Storage EBS snapshot %s created", snapshot.id)
			return snapshot.id			
		except BotoServerError, e:
			self._logger.error("Cannot create MySQL data EBS snapshot. %s", e.message)
			raise
	
	def _add_mysql_users(self, root_user, repl_user, stat_user):
		self._stop_mysql()
		self._logger.info("Adding mysql system users")

		myd = self._start_mysql_skip_grant_tables()
		myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
		out = myclient.communicate('SELECT VERSION();')[0]
		mysql_ver_str = re.search(re.compile('\d*\.\d*\.\d*', re.MULTILINE), out)
		if mysql_ver_str:
			mysql_ver = version.StrictVersion(mysql_ver_str.group(0))
			priv_count = 28 if mysql_ver >= version.StrictVersion('5.1.6') else 26
		else:
			raise HandlerError("Cannot extract mysql version from string '%s'" % out)
	
		myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
		# Define users and passwords
		root_password, repl_password, stat_password = map(lambda x: re.sub('[^\w]','', cryptotool.keygen(20)), range(3))
		# Add users
		sql = "INSERT INTO mysql.user VALUES('%','"+root_user+"',PASSWORD('"+root_password+"')" + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
		sql += " INSERT INTO mysql.user VALUES('localhost','"+root_user+"',PASSWORD('"+root_password+"')" + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
		sql += " INSERT INTO mysql.user (Host, User, Password, Repl_slave_priv) VALUES ('%','"+repl_user+"',PASSWORD('"+repl_password+"'),'Y');"
		sql += " INSERT INTO mysql.user (Host, User, Password, Repl_client_priv) VALUES ('%','"+stat_user+"',PASSWORD('"+stat_password+"'),'Y');"
		sql += " FLUSH PRIVILEGES;"
		out,err = myclient.communicate(sql)
		if err:
			raise HandlerError('Cannot add mysql users: %s', err)
		
		os.kill(myd.pid, signal.SIGTERM)
		time.sleep(3)
		self._start_mysql()
		self._ping_mysql()		
		"""
		self._logger.debug("Checking that mysqld is terminated")
		self._wait_until(lambda: not initd.is_running("mysql"))
		self._logger.debug("Mysqld terminated")
		"""
		self._update_config(dict(
			root_password=root_password,
			repl_password=repl_password,
			stat_password=stat_password
		))

		self._logger.debug("MySQL system users added")
		return (root_password, repl_password, stat_password)
	
	def _update_config(self, data): 
		self._cnf.update_ini(BEHAVIOUR, {self._sect_name: data})
		
	def _replication_init(self, master=True):
		if not os.path.exists('/etc/mysql'):
			os.makedirs('/etc/mysql')
			
		# Create replication config
		self._logger.info("Creating farm-replication config")
		repl_conf_path = '/etc/mysql/farm-replication.cnf'
		try:
			file = open(repl_conf_path, 'w')
		except IOError, e:
			self._logger.error('Cannot open %s: %s', repl_conf_path, e.strerror)
			raise
		else:
			server_id = 1 if master else int(random.random() * 100000)+1
			file.write('[mysqld]\nserver-id\t\t=\t'+ str(server_id)+'\nmaster-connect-retry\t\t=\t15\n')
			file.close()
			os.chmod(repl_conf_path, 0644)
		self._logger.debug("farm-replication config created")

		# Include farm-replication.cnf in my.cnf
		self._logger.debug("Add farm-replication.cnf include in my.cnf")
		try:
			file = open(self._mycnf_path, 'a+')
		except IOError, e:
			self._logger.error('Cannot open %s: %s', self._mycnf_path, e.strerror)
			raise
		else:
			my_cnf = file.read()
			file.close()
			file = open(self._mycnf_path, 'w')
			# Patch networking
			network_re = re.compile('^([\t ]*((bind-address[\t ]*=)|(skip-networking)).*?)$', re.M)
			my_cnf = re.sub(network_re, '#\\1', my_cnf)
			if not re.search(re.compile('^!include \/etc\/mysql\/farm-replication\.cnf', re.MULTILINE), my_cnf):
				my_cnf += '\n!include /etc/mysql/farm-replication.cnf\n'
			file.write(my_cnf)
		finally:
			file.close()
		if disttool.is_debian_based():
			self._add_apparmor_rules(repl_conf_path)
	

	def _spawn_mysql(self, user, password):
		#mysql = pexpect.spawn('/usr/bin/mysql -u ' + user + ' -p' + password)
		mysql = pexpect.spawn('/usr/bin/mysql -u ' + user + ' -p')
		mysql.expect('Enter password:')
		mysql.sendline(password)
		
		mysql.expect('mysql>')
		return mysql

	def _change_master(self, host, user, password, log_file, log_pos, 
					spawn=None, mysql_user=None, mysql_password=None):
		spawn = spawn or self._spawn_mysql(mysql_user, mysql_password)
		self._logger.info("Changing replication master to host %s (log_file: %s, log_pos: %s)", host, log_file, log_pos)
		# Changing replication master
		spawn.sendline('STOP SLAVE;')
		spawn.expect('mysql>')
		spawn.sendline('CHANGE MASTER TO MASTER_HOST="%(host)s", \
						MASTER_USER="%(user)s", \
						MASTER_PASSWORD="%(password)s", \
						MASTER_LOG_FILE="%(log_file)s", \
						MASTER_LOG_POS=%(log_pos)s;' % vars())
		spawn.expect('mysql>')
		
		# Starting slave
		spawn.sendline('START SLAVE;')
		spawn.expect('mysql>')
		status = spawn.before
		if re.search(re.compile('ERROR', re.MULTILINE), status):
			raise HandlerError('Cannot start mysql slave: %s' % status)
		
		# Sleeping for a while
		time.sleep(3)
		spawn.sendline('SHOW SLAVE STATUS;')
		spawn.expect('mysql>')
		
		# Retrieveing slave status row vith values
		status = spawn.before.split('\r\n')[4].split('|')
		spawn.close()
		io_status = status[11].strip()
		sql_status = status[12].strip()
		
		# Check for errors
		if 'Yes' != io_status:
			raise HandlerError ('IO Error while starting mysql slave: %s %s' %  (status[17], status[18]))
		if 'Yes' != sql_status:
			raise HandlerError('SQL Error while starting mysql slave: %s %s' %  (status[17], status[18]))
		
		self._logger.debug('Replication master is changed to host %s', host)		

	def _start_mysql(self):
		try:
			self._logger.info("Starting MySQL")
			initd.start("mysql")
		except:
			self._logger.error("Cannot start MySQL")
			if not initd.is_running("mysql"):				
				raise

	def _stop_mysql(self):
		try:
			self._logger.info("Stopping MySQL")
			initd.stop("mysql")
		except:
			self._logger.error("Cannot stop MySQL")
			if initd.is_running("mysql"):
				raise

			
	def _restart_mysql(self):
		try:
			self._logger.info("Restarting MySQL")
			initd.restart("mysql")
			self._logger.debug("MySQL restarted")
		except:
			self._logger.error("Cannot restart MySQL")
			raise
	
		
	def _ping_mysql(self):
		ping_service("0.0.0.0", 3306, 5)	
	
	def _start_mysql_skip_grant_tables(self):
		if os.path.exists(self._mysqld_path) and os.access(self._mysqld_path, os.X_OK):
			self._logger.debug("Starting mysql server with --skip-grant-tables")
			myd = Popen([self._mysqld_path, '--skip-grant-tables'], stdin=PIPE, stdout=PIPE, stderr=STDOUT)
		else:
			self._logger.error("MySQL daemon '%s' doesn't exists", self._mysqld_path)
			return False
		self._ping_mysql()
		
		return myd
			
	def _move_mysql_dir(self, directive=None, dirname = None, section=None):
		#Reading Mysql config file		
		try:
			file = open(self._mycnf_path, 'r')
		except IOError, e:
			raise HandlerError('Cannot open %s: %s' % (self._mycnf_path, e.strerror))
		else:
			myCnf = file.read()
			file.close					
		# Retrieveing mysql user from passwd		
		mysql_user	= pwd.getpwnam("mysql")
		directory	= os.path.dirname(dirname)
		sectionrow	= re.compile('(.*)(\['+str(section)+'\])(.*)', re.DOTALL)
		search_row	= re.compile('(^\s*'+directive+'\s*=\s*)((/[\w-]+)+)[/\s]([\n\w-]+\.\w+)?', re.MULTILINE)
		src_dir_row = re.search(search_row, myCnf)
		
		if src_dir_row:
			if not os.path.isdir(directory):
				os.makedirs(directory)
				src_dir = os.path.dirname(src_dir_row.group(2) + "/") + "/"
				if os.path.isdir(src_dir):
					self._logger.debug('Copying mysql directory \'%s\' to \'%s\'', src_dir, directory)
					rsync = filetool.Rsync().archive()
					rsync.source(src_dir).dest(directory).exclude(['ib_logfile*'])
					system(str(rsync))
					myCnf = re.sub(search_row, '\\1'+ dirname + '\n' , myCnf)
				else:
					self._logger.debug('Mysql directory \'%s\' doesn\'t exist. Creating new in \'%s\'', src_dir, directory)
					myCnf = re.sub(search_row, '' , myCnf)
					regexp = re.search(sectionrow, myCnf)
					if regexp:
						myCnf = re.sub(sectionrow, '\\1\\2\n'+ directive + ' = ' + dirname + '\n\\3' , myCnf)
					else:
						myCnf += '\n' + directive + ' = ' + dirname
			else:
				myCnf = re.sub(search_row, '\\1'+ dirname + '\n' , myCnf)
		else:
			if not os.path.isdir(directory):
				os.makedirs(directory)
			regexp = re.search(sectionrow, myCnf)
			if regexp:
				myCnf = re.sub(sectionrow, '\\1\\2\n'+ directive + ' = ' +dirname + '\n\\3' , myCnf)
			else:
				myCnf += '\n' + directive + ' = ' + dirname		
				
		# Recursively setting new directory permissions
		os.chown(directory, mysql_user.pw_uid, mysql_user.pw_gid)
		
		try:
			for root, dirs, files in os.walk(directory):
				for dir in dirs:
					os.chown(os.path.join(root , dir), mysql_user.pw_uid, mysql_user.pw_gid)
				for file in files:
					os.chown(os.path.join(root, file), mysql_user.pw_uid, mysql_user.pw_gid)
		except OSError, e:
			self._logger.error('Cannot chown Mysql directory %s', directory)
					
		# Writing new MySQL config
		file = open(self._mycnf_path, 'w')
		file.write(myCnf)
		file.close()	
			
		# Adding rules to apparmor config 
		if disttool.is_debian_based():
			self._add_apparmor_rules(directory)
			
	def _add_apparmor_rules(self, directory):
		try:
			file = open('/etc/apparmor.d/usr.sbin.mysqld', 'r')
		except IOError, e:
			pass
		else:
			app_rules = file.read()
			file.close()
			if not re.search (directory, app_rules):
				file = open('/etc/apparmor.d/usr.sbin.mysqld', 'w')
				if os.path.isdir(directory):
					app_rules = re.sub(re.compile('(\.*)(\})', re.S), '\\1\n'+directory+'/ r,\n'+'\\2', app_rules)
					app_rules = re.sub(re.compile('(\.*)(\})', re.S), '\\1'+directory+'/** rwk,\n'+'\\2', app_rules)
				else:
					app_rules = re.sub(re.compile('(\.*)(\})', re.S), '\\1\n'+directory+' r,\n'+'\\2', app_rules)
				file.write(app_rules)
				file.close()
				initd.explore('apparmor', '/etc/init.d/apparmor')
				try:
					initd.reload('apparmor', True)
				except initd.InitdError, e:
					self._logger.error('Cannot restart apparmor. %s', e)	

	
	def _mount_device(self, devname, mpoint):
		try:
			self._logger.debug("Mounting device %s to %s", devname, mpoint)
			fstool.mount(devname, mpoint, auto_mount=True)
			self._logger.debug("Device %s is mounted to %s", devname, mpoint)
		except fstool.FstoolError, e:
			if fstool.FstoolError.NO_FS == e.code:
				self._logger.warning("Mount failed with NO_FS error. " 
						+ "Creating file system on device %s and mount it again", devname)
				fstool.mount(devname, mpoint, make_fs=True, auto_mount=True)
			else:
				raise
			
	def _get_ec2_conn(self):
		"""
		Maintains single EC2 connection
		@rtype: boto.ec2.connection.EC2Connection
		"""
		if not hasattr(self, "_ec2_conn"):
			self._ec2_conn = self._platform.new_ec2_conn()
		return self._ec2_conn
	
	def _flush_logs(self):
		if not os.path.exists(self._data_dir):
			return
		
		info_files = ['relay-log.info', 'master.info']
		files = os.listdir(self._data_dir)	
		
		for file in files:
			if file in info_files or file.find('relay-bin') != -1:
				os.remove(os.path.join(self._data_dir, file))		
