'''
Created on 14.06.2010

@author: spike
@author: marat
'''

# Core
from scalarizr import config
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours, Configurator
from scalarizr.service import CnfController, CnfPreset
from scalarizr.messaging import Messages
from scalarizr.handlers import HandlerError, ServiceCtlHanler
from scalarizr.platform.ec2 import s3tool, UD_OPT_S3_BUCKET_NAME, ebstool

# Libs
from scalarizr.libs.metaconf import Configuration, MetaconfError, NoPathError,\
	ParseError
from scalarizr.util import fstool, system, cryptotool, disttool,\
		 filetool, firstmatched, cached, validators, initdv2, software, get_free_devname
from scalarizr.util.initdv2 import ParametrizedInitScript, wait_sock, InitdError

# Stdlibs
from distutils import version
from subprocess import Popen, PIPE, STDOUT
import logging, os, re,  tarfile, tempfile
import time, signal, pwd, random, shutil
import glob

# Extra
from boto.exception import BotoServerError
import pexpect


BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.MYSQL
CNF_SECTION = BEHAVIOUR
CNF_NAME = BEHAVIOUR

class MysqlInitScript(initdv2.ParametrizedInitScript):
	def __init__(self):
		initd_script = disttool.is_redhat_based() and "/etc/init.d/mysqld" or "/etc/init.d/mysql"
		
		if not os.path.exists(initd_script):
			raise HandlerError("Cannot find MySQL init script at %s. Make sure that MySQL is installed" % initd_script)
		
		pid_file = None
		try:
			out = system("my_print_defaults mysqld")
			m = re.search("--pid[-_]file=(.*)", out[0], re.MULTILINE)
			if m:
				pid_file = m.group(1)
		except:
			pass
		
		initdv2.ParametrizedInitScript.__init__(self, SERVICE_NAME, 
				initd_script, pid_file, socks=[initdv2.SockParam(3306, timeout=60)])
		'''
		timeout=60 is no chance
		when server starts after rebundle, mysql takes too long to start on the attached EBS storage.
		
		Scalarizr:
		2010-12-02 10:31:12,086 - INFO - scalarizr.handlers - Starting mysql
		
		MySQL:
		Version: '5.1.41-3ubuntu12.7-log'  socket: '/var/run/mysqld/mysqld.sock'  port: 3306  (Ubuntu)
		101202 10:31:30 [Note] Plugin 'FEDERATED' is disabled.
		101202 10:31:31  InnoDB: Started; log sequence number 0 44556
		101202 10:31:31 [Note] Event Scheduler: Loaded 0 events
		
		Over 15 seconds! OMFG!!
		XXX: Requires investigation
		'''
		
		
	def stop(self):
		if not self.running:
			return True
		initdv2.ParametrizedInitScript.stop(self)

initdv2.explore(SERVICE_NAME, MysqlInitScript)


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

class MysqlServiceConfigurator:
	pass

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
STORAGE_BINLOG = "mysql-misc/binlog"
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


class MysqlCnfController(CnfController):
	_mysql_version = None	
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._init_script = initdv2.lookup(SERVICE_NAME)
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._mycnf_path = ini.get(CNF_SECTION, OPT_MYCNF_PATH)
		self._mysqld_path = ini.get(CNF_SECTION, OPT_MYSQLD_PATH)
		CnfController.__init__(self, BEHAVIOUR, self._mycnf_path, 'mysql') #TRUE,FALSE

	def _start_service(self):
		if not hasattr(self, '_mysql_cnf_err_re'):
			self._mysql_cnf_err_re = re.compile('Unknown option|ERROR')
		stderr = system('%s --help' % self._mysqld_path)[1]
		if re.search(self._mysql_cnf_err_re, stderr):
			raise Exception('Error in mysql configuration detected. Output:\n%s' % stderr)
		
		self._logger.info("Starting %s" % self.behaviour)
		
		if not self._init_script.running:
			try:
				self._init_script.start()
			except:
				if not self._init_script.running:
					raise
			self._logger.debug("%s started" % self.behaviour)
	
	def current_preset(self):
		self._logger.debug('Getting current MySQL preset')
		mysql = None
		preset = CnfPreset(name='System', behaviour=BEHAVIOUR)
		self._start_service()
		try:
			mysql = self._get_connection()
			mysql.sendline('SHOW GLOBAL VARIABLES;')
			mysql.expect('mysql>')
			out = mysql.before
			raw_text = out.splitlines()
			text = raw_text[4:-3]
			vars = {}
			
			for line in text:
				splitted_line = line.split('|')					
				name = splitted_line[1].strip()
				value = splitted_line[2].strip()
				"""
				print name, value 
				try:
					remove odd 
					if hasattr(self._manifest, name):
						vars[name] = value
				except AttributeError:
					self._logger.error('No spec for %s' % name)	
					pass
				"""
				vars[name] = value
				
			for opt in self._manifest:
				if opt.name in vars:
					preset.settings[opt.name] = vars[opt.name]
			return preset
		finally:
			if mysql:
				mysql.close()
	
	def apply_preset(self, preset):
		
		CnfController.apply_preset(self, preset)
	
	def _before_apply_preset(self):
		self.sendline = ''
		
	def _after_set_option(self, option_spec, value):
		self._logger.debug('callback "_after_set_option": %s %s (Need restart: %s)' 
				% (option_spec, value, option_spec.need_restart))
		
		if value != option_spec.default_value and not option_spec.need_restart:
			self._logger.debug('Setting variable %s to %s' % (option_spec.name, value))
			self.sendline += 'SET GLOBAL %s = %s; ' % (option_spec.name, value)
			

	def _after_remove_option(self, option_spec):
		if option_spec.default_value and not option_spec.need_restart:
			self._logger.debug('Setting run-time variable %s to default [%s]' 
						% (option_spec.name,option_spec.default_value))
			self.sendline += 'SET GLOBAL %s = %s; ' % (option_spec.name, option_spec.default_value)
	
	def _after_apply_preset(self):
		mysql = self._get_connection()
		try:
			if self.sendline and mysql:
				self._logger.debug(self.sendline)
				mysql.sendline(self.sendline)
				index = mysql.expect(['mysql>', pexpect.EOF, pexpect.TIMEOUT])
				if 1==index or 2==index:
					self._logger.error('Cannot set global variables: %s' % mysql.before)
				else:
					self._logger.debug('All global variables has been set.')
			elif not self.sendline:
				self._logger.debug('No global variables changed. Nothing to set.')
			elif not mysql:
				self._logger.debug('No connection to MySQL. Skipping SETs.')
		finally:
			if mysql:
				mysql.close()
	
	def _get_version(self):
		if not self._mysql_version:
			info = software.software_info('mysql')
			self._mysql_version = info.version
		return self._mysql_version
		
		
	def _get_connection(self):
		szr_cnf = bus.cnf
		root_password = szr_cnf.rawini.get(CNF_SECTION, OPT_ROOT_PASSWORD)
		return _spawn_mysql(ROOT_USER, root_password)


def _spawn_mysql(user, password):
	try:
		mysql = pexpect.spawn('/usr/bin/mysql -u ' + user + ' -p')
		mysql.expect('Enter password:')
		mysql.sendline(password)
		mysql.expect('mysql>')
	except Exception, e:
		raise HandlerError('Cannot start mysql client tool: %s' % (e,))
	finally:
		return mysql
	

class MysqlHandler(ServiceCtlHanler):
	_logger = None
	
	_mysql_config = None
	
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
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
		self._mycnf_path = ini.get(CNF_SECTION, OPT_MYCNF_PATH)
		self._mysqld_path = ini.get(CNF_SECTION, OPT_MYSQLD_PATH)

		
		self._storage_path = STORAGE_PATH
		self._data_dir = os.path.join(self._storage_path, STORAGE_DATA_DIR)
		self._binlog_base = os.path.join(self._storage_path, STORAGE_BINLOG)

		initd = initdv2.lookup(SERVICE_NAME)
		ServiceCtlHanler.__init__(self, SERVICE_NAME, initd, MysqlCnfController())
			
		bus.on("init", self.on_init)
		bus.define_events(
			'before_mysql_data_bundle',
			
			'mysql_data_bundle',
			
			# @param host: New master hostname 
			'before_mysql_change_master',
			
			# @param host: New master hostname 
			# @param log_file: log file to start from 
			# @param log_pos: log pos to start from 
			'mysql_change_master'
		)

	def _reload_mycnf(f):
		def g(self, *args):
			self._mysql_config = Configuration('mysql')
			try:
				self._mysql_config.read(self._mycnf_path)
			except (OSError, MetaconfError, ParseError), e:
				raise HandlerError('Cannot read mysql config %s : %s' % (self._mycnf_path, str(e)))
			f(self, *args)
		return g

	def on_init(self):		
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		
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
				or 	message.name == MysqlMessages.CREATE_PMA_USER
				or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION)
		
	def on_Mysql_CreatePmaUser(self, message):
		try:
			if not int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)):
				raise HandlerError('Cannot add pma user on slave')			
			try:
				root_password = self._cnf.rawini.get(CNF_SECTION, OPT_ROOT_PASSWORD)
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
			sql = "DELETE FROM mysql.user WHERE User = '"+PMA_USER+"';"
			myclient.sendline(sql)
			myclient.expect("mysql>")
			# Generating sql statement, which depends on mysql server version 
			sql = "INSERT INTO mysql.user VALUES('"+pma_server_ip+"','"+PMA_USER+"',PASSWORD('"+pma_password+"')" + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
			# Pass statement to mysql client
			myclient.sendline(sql)
			myclient.expect('mysql>')
			
			# Check for errors
			if re.search('error', myclient.before, re.M | re.I):
				raise HandlerError("Cannot add PhpMyAdmin system user '%s': '%s'" % (PMA_USER, myclient.before))
			
			myclient.sendline('FLUSH PRIVILEGES;')
			myclient.terminate()
			del(myclient)
			
			self._logger.info('PhpMyAdmin system user successfully added')
			
			self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
				status       = 'ok',
				pma_user	 = PMA_USER,
				pma_password = pma_password,
				farm_role_id = farm_role_id,
			))
			
		except (Exception, BaseException), e:
			self._logger.exception(e)
			self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
				status		= 'error',
				last_error	=  str(e).strip(),
				farm_role_id = farm_role_id
			))
	
	@_reload_mycnf
	def on_Mysql_CreateBackup(self, message):
		
		# Retrieve password for scalr mysql user
		tmpdir = backup_path = None
		try:
			# Do backup only on slave
			if int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)):
				raise HandlerError('Create backup is not allowed on Master')
			
			# Load root password
			try:
				root_password = self._cnf.rawini.get(CNF_SECTION, OPT_ROOT_PASSWORD)
			except Exception, e:
				raise HandlerError('Cannot retrieve mysql password from config: %s' % (e,))
			
			# Get databases list
			mysql = _spawn_mysql(ROOT_USER, root_password)
			mysql.sendline('SHOW DATABASES;')
			mysql.expect('mysql>')
			
			databases = list(line.split('|')[1].strip() for line in mysql.before.splitlines()[4:-3])
			if 'information_schema' in databases:
				databases.remove('information_schema')
			
			mysql.close()
			
			
			# Defining archive name and path
			backup_filename = 'mysql-backup-'+time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
			backup_path = os.path.join('/tmp', backup_filename)
			
			# Creating archive 
			backup = tarfile.open(backup_path, 'w:gz')

			# Dump all databases
			self._logger.info("Dumping all databases")
			tmpdir = tempfile.mkdtemp()			
			for db_name in databases:
				dump_path = tmpdir + os.sep + db_name + '.sql'
				mysql = pexpect.spawn('/bin/sh -c "/usr/bin/mysqldump -u ' + ROOT_USER + ' -p --create-options' + 
									  ' --add-drop-database -q -Q --flush-privileges --databases ' + 
									  db_name + '>' + dump_path +'"', timeout=900)
				mysql.expect('Enter password:')
				mysql.sendline(root_password)
				
				status = mysql.read()
				if re.search(re.compile('error', re.M | re.I), status):
					raise HandlerError('Error while dumping database %s: %s' % (db_name, status))
				
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
			self._logger.info("Mysql backup uploaded to S3 under s3://%s/%s", bucket_name, backup_filename)
			
			self.send_message(MysqlMessages.CREATE_BACKUP_RESULT, dict(
				status		= 'ok',
				backup_urls	=  result
			))
						
		except (Exception, BaseException), e:
			self._logger.exception(e)
			self.send_message(MysqlMessages.CREATE_BACKUP_RESULT, dict(
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
			bus.fire('before_mysql_data_bundle')
			try:
				root_password = self._cnf.rawini.get(CNF_SECTION, OPT_ROOT_PASSWORD)
			except Exception, e:
				raise HandlerError('Cannot retrieve mysql login and password from config: %s' % (e,))
			# Creating snapshot
			(snap_id, log_file, log_pos) = self._create_snapshot(ROOT_USER, root_password)
			
			bus.fire('mysql_data_bundle', snapshot_id=snap_id)			
			
			# Sending snapshot data to scalr
			self.send_message(MysqlMessages.CREATE_DATA_BUNDLE_RESULT, dict(
				snapshot_id=snap_id,
				log_file=log_file,
				log_pos=log_pos,
				status='ok'			
			))

		except (Exception, BaseException), e:
			self.send_message(MysqlMessages.CREATE_DATA_BUNDLE_RESULT, dict(
				status		='error',
				last_error	= str(e)
			))

	@_reload_mycnf				
	def on_Mysql_PromoteToMaster(self, message):
		"""
		Promote slave to master
		@type message: scalarizr.messaging.Message
		@param message: Mysql_PromoteToMaster
		"""
		if not int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)):
			
			ec2_conn = self._platform.new_ec2_conn()
			slave_vol_id = 	self._cnf.rawini.get(CNF_SECTION, OPT_STORAGE_VOLUME_ID)
			#master_vol_id = self._queryenv.list_role_params(self._role_name)[PARAM_MASTER_EBS_VOLUME_ID]
			master_vol_id = message.volume_id
			master_vol = None
			tx_complete = False
			
			try:
				# Stop mysql
				if self._init_script.running:
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
					self._stop_service()
					
				# Unplug slave storage and plug master one
				self._unplug_storage(slave_vol_id, self._storage_path)
				master_vol = self._take_master_volume(master_vol_id)
				self._plug_storage(master_vol.id, self._storage_path)
				
				# Continue if master storage is a valid MySQL storage 
				if self._storage_valid():
					# Patch configuration files 
					self._move_mysql_dir('mysqld/log_bin', self._binlog_base)
					self._move_mysql_dir('mysqld/datadir', self._data_dir + os.sep)
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
					self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, dict(
						status="ok",
						volume_id=master_vol.id																				
					))
				else:
					raise HandlerError("%s is not a valid MySQL storage" % self._storage_path)
				tx_complete = True
			except (Exception, BaseException), e:
				self._logger.error("Promote to master failed. %s", e)

				# Get back slave storage
				self._plug_storage(slave_vol_id, self._storage_path, master = False)
				
				# Delete unborn master volume
				if master_vol and master_vol.id != master_vol_id:
					ec2_conn.delete_volume(master_vol.id)
				
				self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, dict(
					status="error",
					last_error=str(e)
				))

			
			# Start MySQL
			self._start_service()				
			
			if tx_complete:
				# Delete slave EBS
				ec2_conn.delete_volume(slave_vol_id)
			
		else:
			self._logger.warning('Cannot promote to master. Already master')

	@_reload_mycnf
	def on_Mysql_NewMasterUp(self, message):
		"""
		Switch replication to a new master server
		@type message: scalarizr.messaging.Message
		@param message:  Mysql_NewMasterUp
		"""
		if not int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)):
			host = message.local_ip or message.remote_ip
			self._logger.info("Switching replication to a new MySQL master %s", host)
			bus.fire('before_mysql_change_master', host=host)			
			
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
			bus.fire('mysql_change_master', host=host, log_file=log_file, log_pos=log_pos)
		else:
			self._logger.debug('Skip NewMasterUp. My replication role is master')		

	
	def on_before_reboot_start(self, *args, **kwargs):
		"""
		Stop MySQL and unplug storage
		"""
		self._stop_service()
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
		self._start_service()

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
		
	@_reload_mycnf
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
					self._start_service()	
					self._stop_service()				
		except:
			pass

		repl = 'master' if int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)) else 'slave'
		if repl == 'master':
			bus.fire('before_mysql_configure', replication=repl)
			self._init_master(message)									  
		else:
			bus.fire('before_mysql_configure', replication=repl)
			self._init_slave(message)		
		
		bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)
	
	def _init_master(self, message):
		"""
		Initialize MySQL master
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		self._logger.info("Initializing MySQL master")
		
		# Mount EBS
		self._plug_storage(self._cnf.rawini.get(CNF_SECTION, OPT_STORAGE_VOLUME_ID), self._storage_path)
		
		# Stop MySQL server
		self._stop_service()
		self._flush_logs()
		
		msg_data = None
		storage_valid = self._storage_valid() # It's important to call it before _move_mysql_dir


		try:
			if not storage_valid and self._mysql_config.get('mysqld/datadir').find(self._data_dir) == 0:
				# When role was created from another mysql role it contains modified my.cnf settings 
				self._repair_original_mycnf()
			
			# Patch configuration
			self._move_mysql_dir('mysqld/datadir', self._data_dir + os.sep)
			self._move_mysql_dir('mysqld/log_bin', self._binlog_base)
	
					
			self._replication_init(master=True)
			
			# If It's 1st init of mysql master storage
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
					root_password = self._cnf.rawini.get(CNF_SECTION, OPT_ROOT_PASSWORD)
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
		except (BaseException, Exception):
			if not storage_valid and self._storage_path:
				# Perform cleanup
				system('rm -rf %s' % os.path.join(self._storage_path, '*'))
			raise
			
		if msg_data:
			message.mysql = msg_data
			self._update_config(msg_data)
			
		#self._start_service()	
			
			
	
	def _init_slave(self, message):
		"""
		Initialize MySQL slave
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		self._logger.info("Initializing MySQL slave")
		if not self._storage_valid():
			self._logger.debug("Initialize slave storage")

			self._logger.info('Checking that master data bundle snapshot completed')
			snap_id = self._cnf.rawini.get(CNF_SECTION, OPT_SNAPSHOT_ID)
			ebstool.wait_snapshot(self._platform.new_ec2_conn(), snap_id, self._logger)
			ebs_volume = self._create_volume_from_snapshot(snap_id)
			
			message.mysql = dict(volume_id = ebs_volume.id)
			self._update_config({OPT_STORAGE_VOLUME_ID : ebs_volume.id})
			
			self._plug_storage(None, self._storage_path, vol=ebs_volume, master=False)

			
		self._stop_service()			
		self._flush_logs()
		# Change configuration files
		self._logger.info("Changing configuration files")
		self._move_mysql_dir('mysqld/datadir', self._data_dir)
		self._move_mysql_dir('mysqld/log_bin', self._binlog_base)
		self._replication_init(master=False)
		if disttool._is_debian_based and os.path.exists(STORAGE_PATH + os.sep +'debian.cnf') :
			try:
				self._logger.debug("Copying debian.cnf from storage to mysql configuration directory")
				shutil.copy(os.path.join(STORAGE_PATH, 'debian.cnf'), '/etc/mysql/')
			except BaseException, e:
				self._logger.error("Cannot copy debian.cnf file from storage: ", e)
				
					
		self._start_service()
		
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
			password=self._cnf.rawini.get(CNF_SECTION, OPT_REPL_PASSWORD),
			log_file=self._cnf.rawini.get(CNF_SECTION, OPT_LOG_FILE), 
			log_pos=self._cnf.rawini.get(CNF_SECTION, OPT_LOG_POS), 
			mysql_user=ROOT_USER,
			mysql_password=self._cnf.rawini.get(CNF_SECTION, OPT_ROOT_PASSWORD)
		)
		
	def _plug_storage(self, vol_id, mnt_point, vol=None, master=True):
		# Getting free letter for device
		devname = get_free_devname()
		if not master:
			while True:
				devname = devname[:-1]+ chr(ord(devname[-1:])+1)
				if not os.path.exists(devname):
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
		binlog_base = os.path.join(path, STORAGE_BINLOG) if path else self._binlog_base
		return os.path.exists(data_dir) and glob.glob(binlog_base + '*')
	
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
		was_running = self._init_script.running
		try:
			if not was_running:
				self._start_service()
			
			# Lock tables
			sql = self._spawn_mysql(root_user, root_password)
			sql.sendline('FLUSH TABLES WITH READ LOCK;')
			sql.expect('mysql>')
			system('sync')
			if int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)):
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
				self._stop_service()

			
	def _create_ebs_snapshot(self):
		self._logger.info("Creating storage EBS snapshot")
		try:
			ec2_conn = self._get_ec2_conn()
			""" @type ec2_conn: boto.ec2.connection.EC2Connection """
			
			snapshot = ec2_conn.create_snapshot(self._cnf.rawini.get(CNF_SECTION, OPT_STORAGE_VOLUME_ID))
			self._logger.debug("Storage EBS snapshot %s created", snapshot.id)
			return snapshot.id			
		except BotoServerError, e:
			self._logger.error("Cannot create MySQL data EBS snapshot. %s", e.message)
			raise
	
	def _repair_original_mycnf(self):
		self._mysql_config.set('mysqld/datadir', '/var/lib/mysql')
		self._mysql_config.remove('mysqld/log_bin')

	
	def _add_mysql_users(self, root_user, repl_user, stat_user):
		self._stop_service()
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
		
		# Generate passwords
		root_password, repl_password, stat_password = map(lambda x: re.sub('[^\w]','', cryptotool.keygen(20)), range(3))
		
		# Add users
		#sql = "INSERT INTO mysql.user VALUES('%','"+root_user+"',PASSWORD('"+root_password+"')" + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
		
		# scalr@localhost allow all
		sql = "INSERT INTO mysql.user VALUES('localhost','"+root_user+"',PASSWORD('"+root_password+"')" + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
		# scalr_repl@% allow Repl_slave_priv
		sql += " INSERT INTO mysql.user (Host, User, Password, Repl_slave_priv) VALUES ('%','"+repl_user+"',PASSWORD('"+repl_password+"'),'Y');"
		# scalr_stat@% allow Repl_client_priv
		sql += " INSERT INTO mysql.user (Host, User, Password, Repl_client_priv) VALUES ('%','"+stat_user+"',PASSWORD('"+stat_password+"'),'Y');"
		
		sql += " FLUSH PRIVILEGES;"
		out,err = myclient.communicate(sql)
		if err:
			raise HandlerError('Cannot add mysql users: %s', err)
		
		os.kill(myd.pid, signal.SIGTERM)
		time.sleep(3)
		self._start_service()
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
		self._cnf.update_ini(BEHAVIOUR, {CNF_SECTION: data})
		
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
		
		if not repl_conf_path in self._mysql_config.get_list('*/!include'):
			# Include farm-replication.cnf in my.cnf
			self._mysql_config.add('!include', repl_conf_path)
			
		# Patch networking
		for option in ['bind-address','skip-networking']:
			try:
				self._mysql_config.comment('mysqld/'+option)
			except:
				pass
		self.write_config()


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

	def _ping_mysql(self):
		for sock in self._init_script.socks:
			wait_sock(sock)
	
	def _start_mysql_skip_grant_tables(self):
		if os.path.exists(self._mysqld_path) and os.access(self._mysqld_path, os.X_OK):
			self._logger.debug("Starting mysql server with --skip-grant-tables")
			myd = Popen([self._mysqld_path, '--skip-grant-tables'], stdin=PIPE, stdout=PIPE, stderr=STDOUT, close_fds = True)
		else:
			self._logger.error("MySQL daemon '%s' doesn't exists", self._mysqld_path)
			return False
		self._ping_mysql()
		
		return myd
			
	def _move_mysql_dir(self, directive=None, dirname = None):

		# Retrieveing mysql user from passwd		
		mysql_user	= pwd.getpwnam("mysql")
		directory	= os.path.dirname(dirname)

		try:
			raw_value = self._mysql_config.get(directive)
			if not os.path.isdir(directory):
				os.makedirs(directory)
				src_dir = os.path.dirname(raw_value + "/") + "/"
				if os.path.isdir(src_dir):
					self._logger.debug('Copying mysql directory \'%s\' to \'%s\'', src_dir, directory)
					rsync = filetool.Rsync().archive()
					rsync.source(src_dir).dest(directory).exclude(['ib_logfile*'])
					system(str(rsync))
					self._mysql_config.set(directive, dirname)
				else:
					self._logger.debug('Mysql directory \'%s\' doesn\'t exist. Creating new in \'%s\'', src_dir, directory)
			else:
				self._mysql_config.set(directive, dirname)
				
		except NoPathError:
			self._logger.debug('There is no such option "%s" in mysql config.' % directive)
			if not os.path.isdir(directory):
				os.makedirs(directory)
			
			self._mysql_config.add(directive, dirname)

		self.write_config()
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
				apparmor_initd = ParametrizedInitScript('apparmor', '/etc/init.d/apparmor')
				try:
					apparmor_initd.reload()
				except InitdError, e:
					self._logger.error('Cannot restart apparmor. %s', e)	

	
	def _mount_device(self, devname, mpoint):
		try:
			self._logger.debug("Mounting device %s to %s", devname, mpoint)
			fstool.mount(devname, mpoint, auto_mount=True)
			self._logger.debug("Device %s is mounted to %s", devname, mpoint)
		except fstool.FstoolError, e:
			if fstool.FstoolError.NO_FS == e.code:
				self._logger.debug("Mount failed with NO_FS error. " 
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
				
	def write_config(self):
		self._mysql_config.write(self._mycnf_path)
