'''
Created on Nov 15, 2011

@author: dmitry
'''

from __future__ import with_statement
import re
import os
import time
import shutil
import tarfile
import tempfile
import logging

# Core
from scalarizr import config
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.messaging import Messages
from scalarizr.handlers import ServiceCtlHandler, DbMsrMessages, HandlerError
import scalarizr.services.mysql as mysql_svc
from scalarizr.service import CnfController, _CnfManifest
from scalarizr.storage import Storage, StorageError, Snapshot, Volume, transfer
from scalarizr.util import system2, disttool, filetool, \
	firstmatched, cached, validators, initdv2, software, wait_until, cryptotool,\
	PopenError, iptables

# Libs
from scalarizr.libs.metaconf import Configuration, MetaconfError, NoPathError, \
	ParseError


BEHAVIOUR = CNF_SECTION = SERVICE_NAME = BuiltinBehaviours.MYSQL
LOG = logging.getLogger(__name__)


OPT_ROOT_PASSWORD 		= "root_password"
OPT_REPL_PASSWORD 		= "repl_password"
OPT_STAT_PASSWORD   	= "stat_password"
OPT_REPLICATION_MASTER  = "replication_master"

OPT_VOLUME_CNF			= 'volume_config'
OPT_SNAPSHOT_CNF		= 'snapshot_config'

CHANGE_MASTER_TIMEOUT   = '30'

# Mysql storage constants
STORAGE_PATH 			= "/mnt/dbstorage"
STORAGE_DATA_DIR 		= "mysql-data"
STORAGE_TMP_DIR 		= "tmp"
STORAGE_BINLOG 			= "mysql-misc/binlog"
STORAGE_VOLUME_CNF 		= 'mysql.json'
STORAGE_SNAPSHOT_CNF 	= 'mysql-snap.json'

# System users
ROOT_USER 				= "scalr"
REPL_USER 				= "scalr_repl"
STAT_USER 				= "scalr_stat"
PMA_USER 				= "pma"

BACKUP_CHUNK_SIZE 		= 200*1024*1024
STOP_SLAVE_TIMEOUT		= 180


class MysqlMessages:
	
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
	
	
class DBMSRandler(ServiceCtlHandler):
	pass


class MysqlCnfController(CnfController):
	cli = None
	_mysql_version = None
	_merged_manifest = None	
	
	def __init__(self):
		self._init_script = initdv2.lookup(SERVICE_NAME)
		self.cli = mysql_svc.MySQLClient(ROOT_USER, self.root_password)
		definitions = {'ON':'1', 'TRUE':'1','OFF':'0','FALSE':'0'}
		CnfController.__init__(self, BEHAVIOUR, mysql_svc.MYCNF_PATH, 'mysql', definitions) #TRUE,FALSE
		
		
	@property	
	def root_password(self):
		pass


	@property
	def _manifest(self):
		f_manifest = CnfController._manifest
		base_manifest = f_manifest.fget(self)		
		path = self._manifest_path

		s = {}
		out = None
		
		if not self._merged_manifest:
			out = system2([mysql_svc.MYSQLD_PATH, '--no-defaults', '--verbose', '--help'],raise_exc=False,silent=True)[0]
			
		if out:
			raw = out.split('--------------------------------- -----------------------------')
			if raw:
				a = raw[-1].split('\n')
				if len(a) > 7:
					b = a[1:-7]
					for item in b:
						c = item.split()
						if len(c) > 1:
							s[c[0].strip()] = ' '.join(c[1:]).strip()
		
		if s:	
			m_config = Configuration('ini')
			if os.path.exists(path):
				m_config.read(path)		
				
			for variable in base_manifest:
				name = variable.name
				dv_path = './%s/default-value' % name
				
				try:
					old_value =  m_config.get(dv_path)
					if name in s:
						new_value = s[name] 
					else:
						name = name.replace('_','-')
						if name in s:
							new_value = self.definitions[s[name]] if s[name] in self.definitions else s[name]
							if old_value != new_value and new_value != '(No default value)':
								LOG.debug('Replacing %s default value %s with precompiled value %s' % (name, old_value, new_value))
								m_config.set(path=dv_path, value=new_value, force=True)
				except NoPathError, e:
					pass
			m_config.write(path)
					
		self._merged_manifest = _CnfManifest(path)
		return self._merged_manifest

			
	def get_system_variables(self):
		vars = CnfController.get_system_variables(self)
		if self._init_script.running:
			
			out = self.cli.execute('SHOW DATABASES')
			raw_text = out.splitlines()
			text = raw_text[4:-3]
			vars = {}
			
			for line in text:
				splitted_line = line.split('|')					
				name = splitted_line[1].strip()
				value = splitted_line[2].strip()
				vars[name] = value
		return vars
	
	def apply_preset(self, preset):
		
		CnfController.apply_preset(self, preset)
	
	def _before_apply_preset(self):
		self.sendline = ''
		
	def _after_set_option(self, option_spec, value):
		LOG.debug('callback "_after_set_option": %s %s (Need restart: %s)' 
				% (option_spec, value, option_spec.need_restart))
		
		if value != option_spec.default_value and not option_spec.need_restart:
			LOG.debug('Preparing to set run-time variable %s to %s' % (option_spec.name, value))
			self.sendline += 'SET GLOBAL %s = %s; ' % (option_spec.name, value)
			

	def _after_remove_option(self, option_spec):
		if option_spec.default_value and not option_spec.need_restart:
			LOG.debug('Preparing to set run-time variable %s to default [%s]' 
						% (option_spec.name,option_spec.default_value))
			'''
			when removing mysql options DEFAULT keyword must be used instead of
			self.sendline += 'SET GLOBAL %s = %s; ' % (option_spec.name, option_spec.default_value)
			'''
			self.sendline += 'SET GLOBAL %s = DEFAULT; ' % (option_spec.name)
	
	def _after_apply_preset(self):
		if not self._init_script.running:
			LOG.info('MySQL isn`t running, skipping process of applying run-time variables')
			return
		
			if self.sendline and self.cli.has_connection():
				LOG.debug(self.sendline)
				try:
					self.cli.execute(self.sendline)
				except PopenError, e:
					LOG.error('Cannot set global variables: %s' % e)
				else:
					LOG.debug('All global variables has been set.')
			elif not self.sendline:
				LOG.debug('No global variables changed. Nothing to set.')
			elif not self.cli.has_connection():
				LOG.debug('No connection to MySQL. Skipping SETs.')

	
	def _get_version(self):
		if not self._mysql_version:
			info = software.software_info('mysql')
			self._mysql_version = info.version
		return self._mysql_version



class MysqlHandler(DBMSRandler):
	
	
	def __init__(self):
		self.mysql = mysql_svc.Mysql()
		ServiceCtlHandler.__init__(self, SERVICE_NAME, self.mysql.service, MysqlCnfController())
		
		bus.on(init=self.on_init, reload=self.on_reload)
		bus.define_events(
			'before_mysql_data_bundle',
			
			'mysql_data_bundle',
			
			# @param host: New master hostname 
			'before_mysql_change_master',
			
			# @param host: New master hostname 
			# @param log_file: log file to start from 
			# @param log_pos: log pos to start from 
			'mysql_change_master'
			
			'before_slave_promote_to_master',
			
			'slave_promote_to_master'
		)
		self.on_reload()	


	def on_init(self):		
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("before_reboot_start", self.on_before_reboot_start)
		bus.on("before_reboot_finish", self.on_before_reboot_finish)
		
		if self._cnf.state == ScalarizrState.BOOTSTRAPPING:
			self._insert_iptables_rules()
		
		elif self._cnf.state == ScalarizrState.RUNNING:
			# Creating self.storage_vol object from configuration
			storage_conf = Storage.restore_config(self._volume_config_path)
			self.storage_vol = Storage.create(storage_conf)
			if not self.storage_vol.mounted():
				self.storage_vol.mount()
			
			if self.is_replication_master:
				LOG.debug("Checking Scalr's %s system users presence." % BEHAVIOUR)
				root_password, repl_password, stat_password = self._get_ini_options(
						OPT_ROOT_PASSWORD, OPT_REPL_PASSWORD, OPT_STAT_PASSWORD)
				
				if not self.mysql.service.running:
					self.mysql.service.start()
				
				users = {} #TODO: fill in users and passwords	
				self.mysql.setup_users(**users)
					
				'''
				try:
					my_cli = spawn_mysql_cli(ROOT_USER, root_password, timeout=5)
					mysqld=None
				except:
					self._stop_service('Checking mysql users') 
					mysqld = spawn_mysqld()
					self._ping_mysql()
					my_cli = spawn_mysql_cli()
										
				try:					
					check_mysql_password(my_cli, ROOT_USER, root_password)
					check_mysql_password(my_cli, REPL_USER, repl_password)
					check_mysql_password(my_cli, STAT_USER, stat_password)
					self._logger.debug("Scalr's MySQL system users are present. Passwords are correct.")				
				except ValueError:
					self._logger.warning("Scalr's MySQL system users were changed. Recreating.")
					self._add_mysql_users(ROOT_USER, REPL_USER, STAT_USER,
										  root_password, repl_password, stat_password, 
										  mysqld, my_cli)
				finally:
					if mysqld:
						term_mysqld(mysqld)
					self._start_service()
				'''
	
	
	def on_reload(self):
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
		
		#use constants instead of members!
		self._mycnf_path = mysql_svc.MYCNF_PATH
		self._mysqld_path = mysql_svc.MYSQLD_PATH
		self._change_master_timeout = CHANGE_MASTER_TIMEOUT
		
		self._storage_path = STORAGE_PATH
		self._data_dir = os.path.join(self._storage_path, STORAGE_DATA_DIR)
		self._tmp_dir = os.path.join(self._storage_path, STORAGE_TMP_DIR)		
		self._binlog_base = os.path.join(self._storage_path, STORAGE_BINLOG)
	
		self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
		self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))
			
	
		
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and (
					message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
				or 	message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
				or 	message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
				or 	message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
				or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION
				or  message.name == Messages.HOST_INIT
				or  message.name == Messages.BEFORE_HOST_TERMINATE
				or  message.name == MysqlMessages.CREATE_PMA_USER)	

	@property
	def is_replication_master(self):
		value = self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)
		LOG.debug('Got %s : %s' % (OPT_REPLICATION_MASTER, value))
		return True if int(value) else False
	
	
	def on_BeforeHostTerminate(self, message):
		if message.local_ip == self._platform.get_private_ip():
			self.mysql.service.stop(reason='Server will be terminated')
			LOG.info('Detaching MySQL storage')
			self.storage_vol.detach()
	
	
	def on_Mysql_CreatePmaUser(self, message):
		try:
			# Operation allowed only on Master server
			if not self.is_replication_master:
				raise HandlerError('Cannot add pma user on slave. It should be a Master server')
			
			pma_server_ip = message.pma_server_ip
			farm_role_id  = message.farm_role_id
			
			LOG.info("Adding phpMyAdmin system user")
			pma_password = cryptotool.pwgen(20)
			#self.mysql.pma_user = mysql_svc.MySQLUser(login=PMA_USER,pma_password)
			self.mysql.pma_user.create(host=pma_server_ip, privileges=None,password=pma_password)
			
			LOG.info('PhpMyAdmin system user successfully added')
			
			# Notify Scalr
			self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
				status       = 'ok',
				pma_user	 = PMA_USER,
				pma_password = pma_password,
				farm_role_id = farm_role_id,
			))
			
		except (Exception, BaseException), e:
			LOG.exception(e)
			
			# Notify Scalr about error
			self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
				status		= 'error',
				last_error	=  str(e).strip(),
				farm_role_id = farm_role_id
			))
	
	
	def on_DbMsr_CreateBackup(self, message):
		
		# Retrieve password for scalr mysql user
		tmpdir = backup_path = None
		try:
			# Get databases list
			databases = self.mysql.cli.list_databases()
			
			# Defining archive name and path
			if not os.path.exists(self._tmp_dir):
				os.makedirs(self._tmp_dir)
			backup_filename = 'mysql-backup-%s.tar.gz' % time.strftime('%Y-%m-%d-%H:%M:%S') 
			backup_path = os.path.join(self._tmp_dir, backup_filename)
			
			# Creating archive 
			backup = tarfile.open(backup_path, 'w:gz')

			# Dump all databases
			LOG.info("Dumping all databases")
			tmpdir = tempfile.mkdtemp(dir=self._tmp_dir)
			mysqldump = mysql_svc.MySQLDump(root_user=ROOT_USER, root_password=self.root_password)			
			for db_name in databases:
				try:
					dump_path = os.path.join(tmpdir, db_name + '.sql') 
					mysqldump.create(db_name, dump_path)
					backup.add(dump_path, os.path.basename(dump_path))						
				except PopenError, e:
					LOG.exception('Cannot dump database %s. %s', db_name, e)
			
			backup.close()
			
			# Creating list of full paths to archive chunks
			if os.path.getsize(backup_path) > BACKUP_CHUNK_SIZE:
				parts = [os.path.join(tmpdir, file) for file in filetool.split(backup_path, backup_filename, BACKUP_CHUNK_SIZE , tmpdir)]
			else:
				parts = [backup_path]
					
			LOG.info("Uploading backup to cloud storage (%s)", self._platform.cloud_storage_path)
			trn = transfer.Transfer()
			result = trn.upload(parts, self._platform.cloud_storage_path)
			LOG.info("Mysql backup uploaded to cloud storage under %s/%s", 
							self._platform.cloud_storage_path, backup_filename)
			
			# Notify Scalr
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = BEHAVIOUR,
				status = 'ok',
				backup_parts = result
			))
						
		except (Exception, BaseException), e:
			LOG.exception(e)
			
			# Notify Scalr about error
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = BEHAVIOUR,
				status = 'error',
				last_error = str(e)
			))
			
		finally:
			if tmpdir:
				shutil.rmtree(tmpdir, ignore_errors=True)
			if backup_path and os.path.exists(backup_path):
				os.remove(backup_path)		


	def on_DbMsr_CreateDataBundle(self, message):
		try:
			bus.fire('before_mysql_data_bundle')
			
			# Creating snapshot
			snap, log_file, log_pos = self._create_snapshot(ROOT_USER, self.root_password)
			used_size = firstmatched(lambda r: r.mpoint == self._storage_path, filetool.df()).used
				
			bus.fire('mysql_data_bundle', snapshot_id=snap.id)			
			
			# Notify scalr
			msg_data = dict(
				log_file=log_file,
				log_pos=log_pos,
				used_size='%.3f' % (float(used_size) / 1024 / 1024,),
				status='ok'
			)
			msg_data.update(self._compat_storage_data(snap=snap))
			self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, msg_data)
		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, dict(
				status		='error',
				last_error	= str(e)
			))
	
	
	def on_DbMsr_PromoteToMaster(self, message):
		"""
		Promote slave to master
		"""
		old_conf 		= None
		new_storage_vol	= None
		
		if not self.is_replication_master:
			
			bus.fire('before_slave_promote_to_master')
			
			if bus.scalr_version >= (2, 2):
				master_storage_conf = message.body.get('volume_config')
			else:
				if 'volume_id' in message.body:
					master_storage_conf = dict(type='ebs', id=message.body['volume_id'])
				else:
					master_storage_conf = None
				
			tx_complete = False
						
			try:
				# Stop mysql
				if master_storage_conf:
					if self._init_script.running:
						self.mysql.cli.stop_slave(timeout=STOP_SLAVE_TIMEOUT)

						self._stop_service('Swapping storages to promote slave to master')
					
					# Unplug slave storage and plug master one
					#self._unplug_storage(slave_vol_id, self._storage_path)
					old_conf = self.storage_vol.detach(force=True) # ??????
					#master_vol = self._take_master_volume(master_vol_id)
					#self._plug_storage(master_vol.id, self._storage_path)
					new_storage_vol = self._plug_storage(self._storage_path, master_storage_conf)				
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
							OPT_REPLICATION_MASTER 	: "1"
						}
						self._update_config(updates)
						Storage.backup_config(new_storage_vol.config(), self._volume_config_path) 
						
						# Send message to Scalr
						msg_data = dict(status='ok')
						msg_data.update(self._compat_storage_data(vol=new_storage_vol))
						self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, msg_data)
					else:
						raise HandlerError("%s is not a valid MySQL storage" % self._storage_path)
					self._start_service()
				else:
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

					updates = {
						OPT_ROOT_PASSWORD : message.root_password,
						OPT_REPL_PASSWORD : message.repl_password,
						OPT_STAT_PASSWORD : message.stat_password,
						OPT_REPLICATION_MASTER 	: "1"
					}
					self._update_config(updates)
										
					snap, log_file, log_pos = self._create_snapshot(ROOT_USER, message.root_password)
					Storage.backup_config(snap.config(), self._snapshot_config_path)
					
					# Send message to Scalr
					msg_data = dict(
						status="ok",
						log_file = log_file,
						log_pos = log_pos
					)
					msg_data.update(self._compat_storage_data(self.storage_vol, snap))
					self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, msg_data)							
					
				tx_complete = True
				bus.fire('slave_promote_to_master')
				
			except (Exception, BaseException), e:
				self._logger.exception(e)
				if new_storage_vol:
					new_storage_vol.detach()
				# Get back slave storage
				if old_conf:
					self._plug_storage(self._storage_path, old_conf)
				
				self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, dict(
					status="error",
					last_error=str(e)
				))

				# Start MySQL
				self._start_service()
			
			if tx_complete and master_storage_conf:
				# Delete slave EBS
				self.storage_vol.destroy(remove_disks=True)
				self.storage_vol = new_storage_vol
				Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
		else:
			self._logger.warning('Cannot promote to master. Already master')

	
	
	def on_DbMsr_NewMasterUp(self, message):
		pass
	
	
	def on_before_reboot_start(self, *args, **kwargs):
		self.mysql.service.stop('Instance is going to reboot')
	
	
	def on_before_reboot_finish(self, *args, **kwargs):
		self._insert_iptables_rules()
	
	
	def on_host_init_response(self, message):
		"""
		Check mysql data in host init response
		@type message: scalarizr.messaging.Message
		@param message: HostInitResponse
		"""
		if not message.body.has_key("mysql"):
			raise HandlerError("HostInitResponse message for MySQL behaviour must have 'mysql' property")

		dir = os.path.dirname(self._volume_config_path)
		if not os.path.exists(dir):
			os.makedirs(dir)
		
		mysql_data = message.mysql.copy()
		for key, file in ((OPT_VOLUME_CNF, self._volume_config_path), 
						(OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
			if os.path.exists(file):
				os.remove(file)
			if key in mysql_data:
				Storage.backup_config(mysql_data[key], file)
				del mysql_data[key]
				
		# Compatibility with Scalr <= 2.1
		if bus.scalr_version <= (2, 1):
			if 'volume_id' in mysql_data:
				Storage.backup_config(dict(type='ebs', id=mysql_data['volume_id']), self._volume_config_path)
				del mysql_data['volume_id']
			if 'snapshot_id' in mysql_data:
				if mysql_data['snapshot_id']:
					Storage.backup_config(dict(type='ebs', id=mysql_data['snapshot_id']), self._snapshot_config_path)
				del mysql_data['snapshot_id']
		
		self._logger.debug("Update mysql config with %s", mysql_data)
		self._update_config(mysql_data)
	
	
	def on_before_host_up(self, message):
		"""
		Configure MySQL behaviour
		@type message: scalarizr.messaging.Message		
		@param message: HostUp message
		"""
		
		self.generate_datadir()

		repl = 'master' if int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)) else 'slave'
		if repl == 'master':
			bus.fire('before_mysql_configure', replication=repl)
			self._init_master(message)									  
		else:
			bus.fire('before_mysql_configure', replication=repl)
			self._init_slave(message)		
		
		bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)
	
	
	def generate_datadir(self):
		try:
			out = system2("my_print_defaults mysqld", shell=True)
			result = re.search("--datadir=(.*)", out[0], re.MULTILINE)
			if result:
				datadir = result.group(1)
				if os.path.isdir(datadir) and not os.path.isdir(os.path.join(datadir, 'mysql')):
					self._start_service()	
					self._stop_service('Autogenerating datadir')				
		except:
			pass		

	
	def _init_master(self, message):
		pass
	
	
	def _init_slave(self, message):
		pass
	
	
	def _insert_iptables_rules(self):
		iptables = iptables.IpTables()
		if iptables.usable():
			iptables.insert_rule(None, iptables.RuleSpec(dport=mysql_svc.MYSQL_DEFAULT_PORT, jump='ACCEPT', protocol=iptables.P_TCP))	
			
			
	@property
	def root_password(self):
		return bus.cnf.rawini.get('mysql', 'root_password')
	