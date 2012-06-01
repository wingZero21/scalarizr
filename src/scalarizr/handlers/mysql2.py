'''
Created on Nov 15, 2011

@author: dmitry
'''

from __future__ import with_statement
import re
import os
import sys
import time
import shutil
import tarfile
import tempfile
import logging
import glob
import ConfigParser

# Core
from scalarizr import config
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.messaging import Messages
from scalarizr.handlers import ServiceCtlHandler, DbMsrMessages, HandlerError
import scalarizr.services.mysql as mysql_svc
from scalarizr.service import CnfController, _CnfManifest
from scalarizr.services import ServiceError
from scalarizr.platform import UserDataOptions
from scalarizr.storage import Storage, StorageError, Snapshot, Volume, transfer
from scalarizr.util import system2, disttool, filetool, \
	firstmatched, cached, validators, initdv2, software, wait_until, cryptotool,\
	PopenError, iptables

# Libs
from scalarizr.libs.metaconf import Configuration, MetaconfError, NoPathError, \
	ParseError


BEHAVIOUR = CNF_SECTION = SERVICE_NAME = BuiltinBehaviours.MYSQL2
LOG = logging.getLogger(__name__)


OPT_ROOT_PASSWORD 		= "root_password"
OPT_REPL_PASSWORD 		= "repl_password"
OPT_STAT_PASSWORD   	= "stat_password"
OPT_REPLICATION_MASTER  = "replication_master"

OPT_LOG_FILE 			= "log_file"
OPT_LOG_POS				= "log_pos"

OPT_VOLUME_CNF			= 'volume_config'
OPT_SNAPSHOT_CNF		= 'snapshot_config'

CHANGE_MASTER_TIMEOUT   = '30'

# Mysql storage constants
STORAGE_PATH 			= "/mnt/dbstorage"
STORAGE_TMP_DIR 		= "tmp"
STORAGE_VOLUME_CNF 		= 'mysql.json'
STORAGE_SNAPSHOT_CNF 	= 'mysql-snap.json'

# System users
ROOT_USER 				= "scalr"
REPL_USER 				= "scalr_repl"
STAT_USER 				= "scalr_stat"
PMA_USER 				= "pma"

BACKUP_CHUNK_SIZE 		= 200*1024*1024
STOP_SLAVE_TIMEOUT		= 180
DEFAULT_DATADIR			= "/var/lib/mysql"
DEBIAN_CNF_PATH			= "/etc/mysql/debian.cnf"

PRIVILEGES = {REPL_USER:('Repl_slave_priv',), STAT_USER:('Repl_client_priv',),}
	
	
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


def get_handlers():
	return [MysqlHandler()]
	
	
class DBMSRHandler(ServiceCtlHandler):
	pass

initdv2.explore(SERVICE_NAME, mysql_svc.MysqlInitScript)

class MysqlCnfController(CnfController):
	root_client = None
	_mysql_version = None
	_merged_manifest = None	
	
	def __init__(self):
		self._init_script = initdv2.lookup(SERVICE_NAME)
		self.root_client = mysql_svc.MySQLClient(ROOT_USER, self.root_password)
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
			
			out = self.root_client.execute('SHOW DATABASES')
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
		
			if self.sendline and self.root_client.has_connection():
				LOG.debug(self.sendline)
				try:
					self.root_client.execute(self.sendline)
				except PopenError, e:
					LOG.error('Cannot set global variables: %s' % e)
				else:
					LOG.debug('All global variables has been set.')
			elif not self.sendline:
				LOG.debug('No global variables changed. Nothing to set.')
			elif not self.root_client.has_connection():
				LOG.debug('No connection to MySQL. Skipping SETs.')

	
	def _get_version(self):
		if not self._mysql_version:
			info = software.software_info('mysql')
			self._mysql_version = info.version
		return self._mysql_version


class MysqlHandler(DBMSRHandler):
	
	
	def __init__(self):
		#use constants instead of members!
		self._mycnf_path = mysql_svc.MYCNF_PATH
		self._mysqld_path = mysql_svc.MYSQLD_PATH
		
		self._data_dir = os.path.join(STORAGE_PATH, mysql_svc.STORAGE_DATA_DIR)
		self._binlog_base = os.path.join(STORAGE_PATH, mysql_svc.STORAGE_BINLOG)
		
		self.mysql = mysql_svc.MySQL()
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

		
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and (
					message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
				or 	message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
				or 	message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
				or 	message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
				or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION
				or  message.name == Messages.BEFORE_HOST_TERMINATE
				or  message.name == MysqlMessages.CREATE_PMA_USER)	
		
			
	def on_reload(self):
		LOG.info("on_reload")
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
		self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
		self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))


	def on_init(self):	
		LOG.info("on_init")	
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
				creds = self.get_user_creds()
				self.create_users(**creds)
	

	def on_host_init_response(self, message):
		"""
		Check mysql data in host init response
		@type message: scalarizr.messaging.Message
		@param message: HostInitResponse
		"""
		LOG.info("on_host_init_response")
		if not message.body.has_key("mysql2"):
			raise HandlerError("HostInitResponse message for MySQL behaviour must have 'mysql2' property")
		
		dir = os.path.dirname(self._volume_config_path)
		if not os.path.exists(dir):
			os.makedirs(dir)
		
		mysql_data = message.mysql2.copy()
		for key, file in ((OPT_VOLUME_CNF, self._volume_config_path), 
						(OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
			if os.path.exists(file):
				os.remove(file)
			if key in mysql_data:
				if mysql_data[key]:
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
		
		LOG.debug("Update mysql config with %s", mysql_data)
		self._update_config(mysql_data)

	
	def on_before_host_up(self, message):
		LOG.info("on_before_host_up")
		"""
		Configure MySQL behaviour
		@type message: scalarizr.messaging.Message		
		@param message: HostUp message
		"""
		
		self.generate_datadir()
		self.mysql.service.stop('configuring mysql')
		repl = 'master' if int(self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)) else 'slave'
		bus.fire('before_mysql_configure', replication=repl)
		if repl == 'master':
			self._init_master(message)	
		else:
			self._init_slave(message)
		'''
		service configuration is temporary disabled
		'''
		#bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)


	def on_BeforeHostTerminate(self, message):
		LOG.info("on_BeforeHostTerminate")
		"""
		if message.local_ip == self._platform.get_private_ip():
			self.mysql.service.stop(reason='Server will be terminated')
			LOG.info('Detaching MySQL storage')
			self.storage_vol.detach()
			
		"""
		assert message.local_ip
	
	
	def on_Mysql_CreatePmaUser(self, message):
		LOG.info("on_Mysql_CreatePmaUser")
		assert message.pma_server_ip
		assert message.farm_role_id
		
		self.send_message(MysqlMessages.CREATE_PMA_USER_RESULT, dict(
				status       = 'ok',
				pma_user	 = PMA_USER,
				pma_password = 'awesome_pma_password',
				farm_role_id = message.farm_role_id,
			))
		
		
		
		'''
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
		'''
	
	
	def on_DbMsr_CreateBackup(self, message):
		LOG.info("on_DbMsr_CreateBackup")
		self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
			db_type = BEHAVIOUR,
			status = 'ok',
			backup_parts = tuple()
		))		
		
		
		
		
		'''
		tmp_dir = os.path.join(STORAGE_PATH, STORAGE_TMP_DIR)		
		# Retrieve password for scalr mysql user
		tmpdir = backup_path = None
		try:
			# Get databases list
			databases = self.mysql.cli.list_databases()
			
			# Defining archive name and path
			if not os.path.exists(tmp_dir):
				os.makedirs(tmp_dir)
			backup_filename = 'mysql-backup-%s.tar.gz' % time.strftime('%Y-%m-%d-%H:%M:%S') 
			backup_path = os.path.join(tmp_dir, backup_filename)
			
			# Creating archive 
			backup = tarfile.open(backup_path, 'w:gz')

			# Dump all databases
			LOG.info("Dumping all databases")
			tmpdir = tempfile.mkdtemp(dir=tmp_dir)
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
		'''	



	def on_DbMsr_CreateDataBundle(self, message):
		LOG.info("on_DbMsr_CreateDataBundle")
		msg_data = dict(
				log_file='blabla',
				log_pos='lala',
				used_size='1.0',
				status='ok',
				snapshot_config='{my_snapshot_config:1}'
			)
		self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, msg_data)
		
		
		
		'''
		try:
			bus.fire('before_mysql_data_bundle')
			
			# Creating snapshot
			snap, log_file, log_pos = self._create_snapshot(ROOT_USER, self.root_password)
			used_size = firstmatched(lambda r: r.mpoint == STORAGE_PATH, filetool.df()).used
				
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
			LOG.exception(e)
			
			# Notify Scalr about error
			self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, dict(
				status		='error',
				last_error	= str(e)
			))
		'''
	
	
	def on_DbMsr_PromoteToMaster(self, message):
		"""
		Promote slave to master
		"""
		LOG.info("on_DbMsr_PromoteToMaster")
		assert message.body['volume_config']
		assert message.root_password
		assert message.repl_password
		assert message.stat_password

		msg_data = dict(
			status="ok",
			log_file = 'ololo',
			log_pos = 'trololo',
			volume_config = 'super_config',
			snapshot_config = 'uber_config'
		)
		self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, msg_data)	
		
		
		
		'''
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
					#self._unplug_storage(slave_vol_id, STORAGE_PATH)
					old_conf = self.storage_vol.detach(force=True) # ??????
					#master_vol = self._take_master_volume(master_vol_id)
					#self._plug_storage(master_vol.id, STORAGE_PATH)
					new_storage_vol = self._plug_storage(STORAGE_PATH, master_storage_conf)				
					# Continue if master storage is a valid MySQL storage 
					if self._storage_valid():
						# Patch configuration files 
						self.mysqlmove_mysqldir_to(STORAGE_PATH)
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
						raise HandlerError("%s is not a valid MySQL storage" % STORAGE_PATH)
					self.mysql.service.start()
				else:
					self.mysql.service.start()
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
				LOG.exception(e)
				if new_storage_vol:
					new_storage_vol.detach()
				# Get back slave storage
				if old_conf:
					self._plug_storage(STORAGE_PATH, old_conf)
				
				self.send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT, dict(
					status="error",
					last_error=str(e)
				))

				# Start MySQL
				self.mysql.service.start()
			
			if tx_complete and master_storage_conf:
				# Delete slave EBS
				self.storage_vol.destroy(remove_disks=True)
				self.storage_vol = new_storage_vol
				Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
		else:
			LOG.warning('Cannot promote to master. Already master')
		'''
	
	
	def on_DbMsr_NewMasterUp(self, message):
		LOG.info("on_DbMsr_NewMasterUp")
		'''
		mysql2 = message.body["mysql2"]
		assert mysql2.has_key("log_file")
		assert mysql2.has_key("log_pos")		
		'''
		assert message.body.has_key("db_type")
		assert message.body.has_key("local_ip")
		assert message.body.has_key("remote_ip")
		assert message.body.has_key("mysql2")
		mysql2 = message.body["mysql2"]
		assert mysql2["snapshot_config"]
		
		
		if  self.is_replication_master():
			LOG.debug('Skip NewMasterUp. My replication role is master')
		
	
	
	def on_before_reboot_start(self, *args, **kwargs):
		LOG.info("on_before_reboot_start")
		pass
		'''
		self.mysql.service.stop('Instance is going to reboot')
		'''
	
	
	def on_before_reboot_finish(self, *args, **kwargs):
		LOG.info("on_before_reboot_finish")
		pass
		'''
		self._insert_iptables_rules()
		'''
	

	def generate_datadir(self):
		try:
			out = system2("my_print_defaults mysqld", shell=True)
			result = re.search("--datadir=(.*)", out[0], re.MULTILINE)
			if result:
				datadir = result.group(1)
				if os.path.isdir(datadir) and not os.path.isdir(os.path.join(datadir, 'mysql')):
					self.mysql.service.start()
					self.mysql.service.stop('Autogenerating datadir')				
		except:
			#TODO: better error handling
			pass		
	

	def _storage_valid(self, path=None):
		data_dir = os.path.join(path, mysql_svc.STORAGE_DATA_DIR) if path else self._data_dir
		binlog_base = os.path.join(path, mysql_svc.STORAGE_BINLOG) if path else self._binlog_base
		return os.path.exists(data_dir) and glob.glob(binlog_base + '*')
	
		
	def _init_master(self, message):
		"""
		Initialize MySQL master
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		LOG.info("Initializing MySQL master")
		
		# Plug storage
		volume_cnf = Storage.restore_config(self._volume_config_path)
		try:
			snap_cnf = Storage.restore_config(self._snapshot_config_path)
			volume_cnf['snapshot'] = snap_cnf
		except IOError:
			pass
		self.storage_vol = self._plug_storage(mpoint=STORAGE_PATH, vol=volume_cnf)
		Storage.backup_config(self.storage_vol.config(), self._volume_config_path)		
		
		self.mysql.flush_logs(self._data_dir)
		
		storage_valid = self._storage_valid()
		user_creds = self.get_user_creds()

		datadir = self.mysql.my_cnf.datadir
		if not datadir:
			datadir = DEFAULT_DATADIR
			self.mysql.my_cnf.datadir = DEFAULT_DATADIR

		if not storage_valid and datadir.find(self._data_dir) == 0:
			# When role was created from another mysql role it contains modified my.cnf settings 
			self.mysql.my_cnf.datadir = DEFAULT_DATADIR
			self.mysql.my_cnf.log_bin = None
		
		# Patch configuration
		
		self.mysql.move_mysqldir_to(STORAGE_PATH)
		
		# Init replication
		self.mysql._init_replication(master=True)
		
		msg_data = dict()
		
		# If It's 1st init of mysql master storage
		if not storage_valid:
			if os.path.exists(DEBIAN_CNF_PATH):
				LOG.debug("Copying debian.cnf file to mysql storage")
				shutil.copy(DEBIAN_CNF_PATH, STORAGE_PATH)		

			# Update HostUp message 
			passwords = dict(
				root_password=user_creds[ROOT_USER],
				repl_password=user_creds[REPL_USER],
				stat_password=user_creds[STAT_USER])
			msg_data.update(passwords)
			
		# If volume has mysql storage directory structure (N-th init)
		else:
			self._copy_debian_cnf_back()
			self._innodb_recovery()			

		# Add system users	
		self.create_users(**user_creds)	
		
		# Get binary logfile, logpos and create storage snapshot
		snap, log_file, log_pos = self._create_snapshot(ROOT_USER, user_creds[ROOT_USER])
		Storage.backup_config(snap.config(), self._snapshot_config_path)

		# Update HostUp message 
		logs = dict(
			log_file=log_file, 
			log_pos=log_pos
		)	
		msg_data.update(logs)		
		msg_data.update({OPT_REPLICATION_MASTER:str(int(self.is_replication_master))})
		msg_data.update(self._compat_storage_data(self.storage_vol, snap))
			
		message.db_type = BEHAVIOUR
		message.mysql2 = msg_data.copy()
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
		LOG.info("Initializing MySQL slave")
		
		# Read required configuration options
		log_file, log_pos, repl_pass = self._get_ini_options(OPT_LOG_FILE, OPT_LOG_POS, OPT_REPL_PASSWORD)
		
		if not self._storage_valid():
			LOG.debug("Initialize slave storage")
			self.storage_vol = self._plug_storage(STORAGE_PATH, 
					dict(snapshot=Storage.restore_config(self._snapshot_config_path)))
		Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
		
		try:
			self._stop_service('Required by Slave initialization process')			
			self.mysql.flush_logs(self._data_dir)
			
			# Change configuration files
			LOG.info("Changing configuration files")
			if not self.mysql.my_cnf.datadir:
				self.mysql.my_cnf.datadir = DEFAULT_DATADIR
	
			self.mysql.move_mysqldir_to(STORAGE_PATH)
			self.mysql._init_replication(master=False)
			self._copy_debian_cnf_back()
			self._innodb_recovery()
			self.mysql.service.start()
			
			# Change replication master 
			LOG.info("Requesting master server")
			master_host = self.get_master_host()

			self._change_master( 
				host=master_host, 
				user=REPL_USER, 
				password=repl_pass,
				log_file=log_file, 
				log_pos=log_pos)
			# Update HostUp message
			message.mysql = self._compat_storage_data(self.storage_vol)
			message.db_type = BEHAVIOUR
		except:
			exc_type, exc_value, exc_trace = sys.exc_info()
			raise exc_type, exc_value, exc_trace

		
	def get_master_host(self):
		master_host = None
		while not master_host:
			try:
				master_host = list(host 
					for host in self._queryenv.list_roles(self._role_name)[0].hosts 
					if host.replication_master)[0]
			except IndexError:
				LOG.debug("QueryEnv respond with no mysql master. " + 
						"Waiting %d seconds before the next attempt", 5)
			time.sleep(5)
		LOG.debug("Master server obtained (local_ip: %s, public_ip: %s)",
				master_host.internal_ip, master_host.external_ip)
		return master_host.internal_ip or master_host.external_ip

			
	def _copy_debian_cnf_back(self):
		debian_cnf = os.path.join(STORAGE_PATH, 'debian.cnf')
		if disttool.is_debian_based() and os.path.exists(debian_cnf):
			LOG.debug("Copying debian.cnf from storage to mysql configuration directory")
			shutil.copy(debian_cnf, '/etc/mysql/')
			
			
	@property
	def root_password(self):
		cnf = bus.cnf
		return cnf.rawini.get('mysql2', 'root_password')
	
			
	@property
	def root_client(self):
		return mysql_svc.MySQLClient(ROOT_USER, self.root_password)
	
	
	def _compat_storage_data(self, vol=None, snap=None):
		ret = dict()
		if bus.scalr_version >= (2, 2):
			if vol:
				ret['volume_config'] = vol.config()
			if snap:
				ret['snapshot_config'] = snap.config()
		else:
			if vol:
				ret['volume_id'] = vol.config()['id']
			if snap:
				ret['snapshot_id'] = snap.config()['id']
		return ret
			

	def _innodb_recovery(self, storage_path=None):
		storage_path = storage_path or STORAGE_PATH
		binlog_path	= os.path.join(storage_path, mysql_svc.STORAGE_BINLOG)		
		data_dir = os.path.join(storage_path, mysql_svc.STORAGE_DATA_DIR),
		pid_file = os.path.join(storage_path, 'mysql.pid')
		socket_file = os.path.join(storage_path, 'mysql.sock')
		mysqld_safe_bin	= software.whereis('mysqld_safe')[0]
		
		LOG.info('Performing InnoDB recovery')
		mysqld_safe_cmd = (mysqld_safe_bin, 
			'--socket=%s' % socket_file, 
			'--pid-file=%s' % pid_file, 
			'--datadir=%s' % data_dir,
			'--log-bin=%s' % binlog_path, 
			'--skip-networking', 
			'--skip-grant', 
			'--bootstrap', 
			'--skip-slave-start')
		system2(mysqld_safe_cmd, stdin="select 1;")	

		
	def _insert_iptables_rules(self):
		ipt = iptables.IpTables()
		if ipt.usable():
			ipt.insert_rule(None, iptables.RuleSpec(dport=mysql_svc.MYSQL_DEFAULT_PORT, jump='ACCEPT', protocol=iptables.P_TCP))	
			

	@property
	def is_replication_master(self):
		value = self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)
		LOG.debug('Got %s : %s' % (OPT_REPLICATION_MASTER, value))
		return True if int(value) else False
	
	
	def get_user_creds(self):
		options = {ROOT_USER:OPT_ROOT_PASSWORD, REPL_USER:OPT_REPL_PASSWORD, STAT_USER:OPT_STAT_PASSWORD}
		creds = {}
		for login, opt_pwd in options.items():
			password = self._get_ini_options(opt_pwd)[0]
			if not password:
				password = cryptotool.pwgen(20)
				self._update_config({opt_pwd:password})
			creds[login] = password
		return creds
	
	
	def create_users(self, **creds):
		users = {}
		root_cli = mysql_svc.MySQLClient(ROOT_USER, creds[ROOT_USER])
		local_root = mysql_svc.MySQLUser(root_cli, ROOT_USER, creds[ROOT_USER], host='localhost')

		if not self.mysql.service.running:
			self.mysql.service.start()
			
			try:
				if not local_root.exists() or not local_root.check_password():
					users.update({'local_root': local_root})
					self.mysql.service.stop('creating users')
					self.mysql.service.start_skip_grant_tables()
				else:
					LOG.debug('User %s exists and has correct password' % ROOT_USER)
			except ServiceError, e:
				if 'Access denied for user' in str(e):
					users.update({'local_root': local_root})
					self.mysql.service.stop('creating users')
					self.mysql.service.start_skip_grant_tables()
				
		for login, password in creds.items():
			user = mysql_svc.MySQLUser(root_cli, login, password, host='%', privileges=PRIVILEGES.get(login, None))
			users[login] = user
			
		for login, user in users.items():
			if not user.exists():
				LOG.debug('User %s not found. Recreating.' % login)
				user.create()
			elif not user.check_password():
				LOG.warning('Password for user %s was changed. Recreating.' %  login)
				user.remove()
				user.create()
			users[login] = user
			
		self.mysql.service.stop_skip_grant_tables()	
		return users
		

	def _update_config(self, data): 
		#XXX: I just don't like it
		#ditching empty data
		updates = dict()
		for k,v in data.items():
			if v: 
				updates[k] = v
		
		self._cnf.update_ini(BEHAVIOUR, {CNF_SECTION: data})
		


	def _plug_storage(self, mpoint, vol):
		if not isinstance(vol, Volume):
			vol = Storage.create(vol)

		try:
			if not os.path.exists(mpoint):
				os.makedirs(mpoint)
			if not vol.mounted():
				vol.mount(mpoint)
		except StorageError, e:
			''' XXX: Crapy. We need to introduce error codes from fstool ''' 
			if 'you must specify the filesystem type' in str(e):
				vol.mkfs()
				vol.mount(mpoint)
			else:
				raise
		return vol
	

	def _create_snapshot(self, root_user, root_password, tags=None):
		was_running = self.mysql.service.running
		if not was_running:
			self.mysql.service.start()
		try:
			self.root_client.lock_tables()
			system2('sync', shell=True)
			
			if self.is_replication_master:
				log_file, log_pos = self.root_client.master_status()  
			else: 
				data = self.root_client.slave_status()
				log_file = data['Relay_Master_Log_File']
				log_pos = data['Exec_Master_Log_Pos']
	
			# Creating storage snapshot
			snap = self._create_storage_snapshot(tags)
		except BaseException, e:
			LOG.error('Snapshot creation failed with error: %s' % e)
			raise	
		
		finally:
			self.root_client.unlock_tables()
			if not was_running:
				self.mysql.service.stop('Restoring service`s state after making snapshot')
		
		wait_until(lambda: snap.state in (Snapshot.CREATED, Snapshot.COMPLETED, Snapshot.FAILED))
		if snap.state == Snapshot.FAILED:
			raise HandlerError('MySQL storage snapshot creation failed. See log for more details')
		
		LOG.info('MySQL data bundle created\n  snapshot: %s\n  log_file: %s\n  log_pos: %s', 
						snap.id, log_file, log_pos)
		return snap, log_file, log_pos
	
	
	def _get_ini_options(self, *args):
		ret = []
		for opt in args:
			try:
				ret.append(self._cnf.rawini.get(CNF_SECTION, opt))
			except ConfigParser.Error:
				err = 'Required configuration option is missed in mysql.ini: %s' % opt
				raise HandlerError(err)
		return tuple(ret)
	

	def _create_storage_snapshot(self, tags=None):
		LOG.info("Creating storage snapshot")
		tags = tags or dict()
		#tags.update({'storage': 'mysql'})		
		try:
			return self.storage_vol.snapshot(self._data_bundle_description(), tags=tags)
		except StorageError, e:
			LOG.error("Cannot create MySQL data snapshot. %s", e)
			raise
	

	def _data_bundle_description(self):
		pl = bus.platform
		return 'MySQL data bundle (farm: %s role: %s)' % (
					pl.get_user_data(UserDataOptions.FARM_ID), 
					pl.get_user_data(UserDataOptions.ROLE_NAME))
		

	def _change_master(self, host, user, password, log_file, log_pos, timeout=CHANGE_MASTER_TIMEOUT):
		
		LOG.info("Changing replication Master to server %s (log_file: %s, log_pos: %s)", host, log_file, log_pos)
		
		# Changing replication master
		self.root_client.start_slave()
		self.root_client.change_master_to(host, user, password, log_file, log_pos)
		
		# Starting slave
		result = self.root_client.start_slave()
		LOG.debug('Start slave returned: %s' % result)
		if 'ERROR' in result:
			raise HandlerError('Cannot start mysql slave: %s' % result)

		time_until = time.time() + timeout
		status = None
		while time.time() <= time_until:
			status = self.root_client.slave_status()
			if status['Slave_IO_Running'] == 'Yes' and \
				status['Slave_SQL_Running'] == 'Yes':
				break
			time.sleep(5)
		else:
			if status:
				if not status['Last_Error']:
					logfile = firstmatched(lambda p: os.path.exists(p), 
										('/var/log/mysqld.log', '/var/log/mysql.log'))
					if logfile:
						gotcha = '[ERROR] Slave I/O thread: '
						size = os.path.getsize(logfile)
						fp = open(logfile, 'r')
						try:
							fp.seek(max((0, size - 8192)))
							lines = fp.read().split('\n')
							for line in lines:
								if gotcha in line:
									status['Last_Error'] = line.split(gotcha)[-1]
						finally:
							fp.close()
				
				msg = "Cannot change replication Master server to '%s'. "  \
						"Slave_IO_Running: %s, Slave_SQL_Running: %s, " \
						"Last_Errno: %s, Last_Error: '%s'" % (
						host, status['Slave_IO_Running'], status['Slave_SQL_Running'],
						status['Last_Errno'], status['Last_Error'])
				raise HandlerError(msg)
			else:
				raise HandlerError('Cannot change replication master to %s' % (host))

				
		LOG.debug('Replication master is changed to host %s', host)		
