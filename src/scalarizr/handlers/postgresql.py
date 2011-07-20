'''
Created on April 18th, 2011

@author: Dmytro Korsakov
'''
import os
import time
import shlex
import shutil
import tarfile
import logging
import tempfile

from scalarizr import config
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import ServiceCtlHanler, HandlerError
from scalarizr.util.filetool import read_file, write_file, split
from scalarizr.util import initdv2, system2, wait_until, PopenError
from scalarizr.storage import Storage, Snapshot, StorageError, Volume, transfer
from scalarizr.services.postgresql import PostgreSql, PSQL, ROOT_USER, PG_DUMP, OPT_REPLICATION_MASTER,\
	PgUser


BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.POSTGRESQL

STORAGE_PATH 			= "/mnt/pgstorage"
STORAGE_VOLUME_CNF 		= 'postgresql.json'
STORAGE_SNAPSHOT_CNF 	= 'postgresql-snap.json'

OPT_VOLUME_CNF			= 'volume_config'
OPT_SNAPSHOT_CNF		= 'snapshot_config'
OPT_ROOT_PASSWORD 		= "root_password"
OPT_CHANGE_MASTER_TIMEOUT = 'change_master_timeout'

BACKUP_CHUNK_SIZE 		= 200*1024*1024

POSTGRESQL_DEFAULT_PORT	= 5432

		
def get_handlers():
	return (PostgreSqlHander(), )


class PostgreSqlMessages:
	DBMSR_CREATE_DATA_BUNDLE = "DbMsr_CreateDataBundle"
	
	DBMSR_CREATE_DATA_BUNDLE_RESULT = "DbMsr_CreateDataBundleResult"
	'''
	@ivar: db_type: postgresql|mysql
	@ivar: status: Operation status [ ok | error ]
	@ivar: last_error: errmsg if status = error
	@ivar: snapshot_config: snapshot configuration
	@ivar: current_xlog_location:  pg_current_xlog_location() on master after snap was created
	'''
	
	DBMSR_CREATE_BACKUP = "DbMsr_CreateBackup"
	
	DBMSR_CREATE_BACKUP_RESULT = "DbMsr_CreateBackupResult"
	'''
	@ivar: db_type: postgresql|mysql
	@ivar: status: Operation status [ ok | error ]
	@ivar: last_error:  errmsg if status = error
	@ivar: backup_parts: URL List (s3, cloudfiles)
	'''
	
	DBMSR_PROMOTE_TO_MASTER = "DbMsr_PromoteToMaster"
	
	DBMSR_PROMOTE_TO_MASTER_RESULT = "DbMsr_PromoteToMasterResult"
	'''
	@ivar: db_type: postgresql|mysql
	@ivar: status: ok|error
	@ivar: last_error: errmsg if status=error
	@ivar: volume_config: volume configuration
	@ivar: snapshot_config?: snapshot configuration
	@ivar: current_xlog_location_?:  pg_current_xlog_location() on master after snap was created
	'''
	
	DBMSR_NEW_MASTER_UP = "DbMsr_NewMasterUp"
	'''
	@ivar: db_type:  postgresql|mysql
	@ivar: local_ip
	@ivar: remote_ip
	@ivar: snapshot_config
	@ivar: current_xlog_location:  pg_current_xlog_location() on master after snap was created
	'''
	
	"""
	Also Postgresql behaviour adds params to common messages:
	
	= HOST_INIT_RESPONSE =
	@ivar db_type: postgresql|mysql
	@ivar postgresql=dict(
		replication_master:  	 1|0 
		root_user 
		root_password:			 'scalr' user password  					(on slave)
		root_ssh_private_key
		root_ssh_public_key 
		current_xlog_location 
		volume_config:			Master storage configuration			(on master)
		snapshot_config:		Master storage snapshot 				(both)
	)
	
	= HOST_UP =
	@ivar db_type: postgresql|mysql
	@ivar postgresql=dict(
	@ivar replication_master: 1|0 
	@ivar root_user 
	@ivar root_password: 			'scalr' user password  					(on master)
	@ivar root_ssh_private_key
	@ivar root_ssh_public_key
	@ivar current_xlog_location
	@ivar volume_config:			Current storage configuration			(both)
	@ivar snapshot_config:		Master storage snapshot					(on master)	
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
		return BEHAVIOUR in behaviour and (
					message.name == PostgreSqlMessages.DBMSR_NEW_MASTER_UP
				or 	message.name == PostgreSqlMessages.DBMSR_PROMOTE_TO_MASTER
				or 	message.name == PostgreSqlMessages.DBMSR_CREATE_DATA_BUNDLE
				or 	message.name == PostgreSqlMessages.DBMSR_CREATE_BACKUP
				or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION
				or  message.name == Messages.HOST_INIT
				or  message.name == Messages.BEFORE_HOST_TERMINATE)	

	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		bus.on("init", self.on_init)
		bus.define_events(
			'before_postgresql_data_bundle',
			
			'postgresql_data_bundle',
			
			# @param host: New master hostname 
			'before_postgresql_change_master',
			
			# @param host: New master hostname 
			'postgresql_change_master',
			
			'before_slave_promote_to_master',
			
			'slave_promote_to_master'
		)	
		
		self.postgresql = PostgreSql()
		self.on_reload()		


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
			
			if self.postgresql.is_replication_master:
				self._logger.debug("Checking presence of Scalr's PostgreSQL root user.")
				root_password = self.postgresql.root_user.password
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


	def on_reload(self):
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
		
		self._change_master_timeout = int(ini.get(CNF_SECTION, OPT_CHANGE_MASTER_TIMEOUT) or '30')
		
		self._storage_path = STORAGE_PATH
		
		self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
		self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))
		
	
	def on_HostInit(self, message):
		if message.local_ip != self._platform.get_private_ip():
			self._logger.debug('Got new slave IP: %s. Registering in pg_hba.conf' % message.local_ip)
			self.postgresql.register_slave(message.local_ip)
				
	
	def on_host_init_response(self, message):
		"""
		Check postgresql data in host init response
		@type message: scalarizr.messaging.Message
		@param message: HostInitResponse
		"""
		if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
			raise HandlerError("HostInitResponse message for PostgreSQL behaviour must have 'postgresql' property and db_type 'postgresql'")

		dir = os.path.dirname(self._volume_config_path)
		if not os.path.exists(dir):
			os.makedirs(dir)
		
		postgresql_data = message.postgresql.copy()
		for key, file in ((OPT_VOLUME_CNF, self._volume_config_path), 
						(OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
			if os.path.exists(file):
				os.remove(file)
			#omitting empty configs
			if key in postgresql_data and postgresql_data[key]:
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
		#bus.fire('before_postgresql_configure', replication=repl)
		
		if self.postgresql.is_replication_master:
			self._init_master(message)									  
		else:
			self._init_slave(message)		
			
		bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)
					
				
	def on_before_reboot_start(self, *args, **kwargs):
		"""
		Stop MySQL and unplug storage
		"""
		self.postgresql.service.stop('rebooting')


	def on_before_reboot_finish(self, *args, **kwargs):
		#TODO: find out what to do!
		pass

	def on_BeforeHostTerminate(self, message):
		if message.local_ip == self._platform.get_private_ip():
			self.postgresql.service.stop('Server will be terminated')
			self._logger.info('Detaching PgSQL storage')
			self.storage_vol.detach()
		elif self.postgresql.is_replication_master:
			self.postgresql.unregister_slave(message.local_ip)

	def on_DbMsr_CreateDataBundle(self, message):
		
		try:
			bus.fire('before_postgresql_data_bundle')
			# Retrieve password for scalr postgresql root user
			root_password = self.postgresql.root_user.password
			# Creating snapshot		
			snap = self._create_snapshot(ROOT_USER, root_password)
			used_size = int(system2(('df', '-P', '--block-size=M', self._storage_path))[0].split('\n')[1].split()[2][:-1])
			bus.fire('postgresql_data_bundle', snapshot_id=snap.id)			
			
			# Notify scalr
			msg_data = dict(
				used_size='%.3f' % (float(used_size) / 1000,),
				status='ok'
			)
			msg_data.update(self._compat_storage_data(snap=snap))
			self.send_message(PostgreSqlMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, msg_data)

		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(PostgreSqlMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, dict(
				status		='error',
				last_error	= str(e)
			))

	
	def on_DbMsr_NewMasterUp(self, message):
		"""
		Switch replication to a new master server
		@type message: scalarizr.messaging.Message
		@param message:  PostgreSQL_NewMasterUp
		"""
		
		if not self.postgresql.is_replication_master:
			host = message.local_ip or message.remote_ip
			self._logger.info("Switching replication to a new postgresql master %s", host)
			bus.fire('before_postgresql_change_master', host=host)			
			
			if 'snapshot_config' in message.body:
				self._logger.info('Reinitializing Slave from the new snapshot %s', 
						message.snapshot_config['id'])
				self.postgresql.service.stop()
				
				self._logger.debug('Destroying old storage')
				self.storage_vol.destroy()
				self._logger.debug('Storage destroyed')
				
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
			

	def on_DbMsr_PromoteToMaster(self, message):
		"""
		Promote slave to master
		@type message: scalarizr.messaging.Message
		@param message: postgresql_PromoteToMaster
		"""
		
		if self.postgresql.is_replication_master:
			self._logger.warning('Cannot promote to master. Already master')
			return
		
		bus.fire('before_slave_promote_to_master')
		
		master_storage_conf = message.body.get('volume_config')
		tx_complete = False	
		old_conf 		= None
		new_storage_vol	= None		
					
		try:
			# Stop postgresql
			if master_storage_conf:
				self.postgresql.stop_replication()
				self.postgresql.service.stop()

				# Unplug slave storage and plug master one
				old_conf = self.storage_vol.detach(force=True) # ??????
				new_storage_vol = self._plug_storage(self._storage_path, master_storage_conf)	
							
				# Continue if master storage is a valid postgresql storage 
				if not self.postgresql.cluster_dir.is_initialized(self._storage_path):
					raise HandlerError("%s is not a valid postgresql storage" % self._storage_path)
				
				self.postgresql.cluster_dir.move_to(self._storage_path)
				
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
				self.send_message(PostgreSqlMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)							
				
			tx_complete = True
			bus.fire('slave_promote_to_master')
			
		except (Exception, BaseException), e:
			self._logger.exception(e)
			if new_storage_vol:
				new_storage_vol.detach()
			# Get back slave storage
			if old_conf:
				self._plug_storage(self._storage_path, old_conf)
			
			self.send_message(PostgreSqlMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, dict(
				status="error",
				last_error=str(e)
			))

			# Start postgresql
			self.postgresql.service.start()
		
		if tx_complete and master_storage_conf:
			# Delete slave EBS
			self.storage_vol.destroy(remove_disks=True)
			self.storage_vol = new_storage_vol
			Storage.backup_config(self.storage_vol.config(), self._storage_path)


	def on_DbMsr_CreateBackup(self, message):
		#TODO: Think how to move the most part of it into Postgresql class 
		# Retrieve password for scalr mysql user
		tmpdir = backup_path = None
		try:
			# Get databases list
			psql = PSQL(user=self.postgresql.root_user)
			databases = psql.list_pg_databases()
			
			# Defining archive name and path
			backup_filename = 'pgsql-backup-'+time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
			backup_path = os.path.join('/tmp', backup_filename)
			
			# Creating archive 
			backup = tarfile.open(backup_path, 'w:gz')

			# Dump all databases
			self._logger.info("Dumping all databases")
			tmpdir = tempfile.mkdtemp()			
			for db_name in databases:
				dump_path = tmpdir + os.sep + db_name + '.sql'
				args = shlex.split('%s %s --no-privileges -f %s' % (PG_DUMP, db_name, dump_path))
				err = system2(args)[1]
				if err:
					raise HandlerError('Error while dumping database %s: %s' % (db_name, err))
				backup.add(dump_path, os.path.basename(dump_path))
			backup.close()
			
			# Creating list of full paths to archive chunks
			if os.path.getsize(backup_path) > BACKUP_CHUNK_SIZE:
				parts = [os.path.join(tmpdir, file) for file in split(backup_path, backup_filename, BACKUP_CHUNK_SIZE , tmpdir)]
			else:
				parts = [backup_path]
					
			self._logger.info("Uploading backup to cloud storage (%s)", self._platform.cloud_storage_path)
			trn = transfer.Transfer()
			result = trn.upload(parts, self._platform.cloud_storage_path)
			self._logger.info("Postgresql backup uploaded to cloud storage under %s/%s", 
							self._platform.cloud_storage_path, backup_filename)
			
			# Notify Scalr
			self.send_message(PostgreSqlMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				status = 'ok',
				backup_parts = result
			))
						
		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(PostgreSqlMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				status = 'error',
				last_error = str(e)
			))
			
		finally:
			if tmpdir:
				shutil.rmtree(tmpdir, ignore_errors=True)
			if backup_path and os.path.exists(backup_path):
				os.remove(backup_path)				
		
								
	def _init_master(self, message):
		"""
		Initialize postgresql master
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
		
		
		self.postgresql.init_master(mpoint=self._storage_path)
		root_password = self.postgresql.root_user.password
		
		msg_data = dict()
		
		msg_data.update(dict(replication_master = int(self.postgresql.is_replication_master),
							root_user = self.postgresql.root_user.name,
							root_password=root_password,
							root_ssh_private_key = self.postgresql.root_user.private_key, 
							root_ssh_public_key = self.postgresql.root_user.public_key, 
							current_xlog_location = None))	
		#TODO: add xlog
			
		# Create snapshot
		snap = self._create_snapshot(ROOT_USER, root_password)
		Storage.backup_config(snap.config(), self._snapshot_config_path)
	
		# Update HostUp message 
		msg_data.update(self._compat_storage_data(self.storage_vol, snap))
			
		if msg_data:
			message.db_type = BEHAVIOUR
			message.postgresql = msg_data.copy()
			try:
				del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
			except KeyError:
				pass 
			self._update_config(msg_data)
	
	
	def _init_slave(self, message):
		"""
		Initialize postgresql slave
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		self._logger.info("Initializing postgresql slave")
		
		if not self.postgresql.cluster_dir.is_initialized(self._storage_path):
			self._logger.debug("Initialize slave storage")
			self.storage_vol = self._plug_storage(self._storage_path, 
					dict(snapshot=Storage.restore_config(self._snapshot_config_path)))			
			Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
			
		# Change replication master 
		master_host = None
		self._logger.info("Requesting master server")
		while not master_host:
			try:
				master_host = list(host 
					for host in self._queryenv.list_roles(self._role_name)[0].hosts 
					if host.replication_master)[0]
			except IndexError:
				self._logger.debug("QueryEnv respond with no postgresql master. " + 
						"Waiting %d seconds before the next attempt", 5)
				time.sleep(5)
				
		self._logger.debug("Master server obtained (local_ip: %s, public_ip: %s)",
				master_host.internal_ip, master_host.external_ip)
		
		host = master_host.internal_ip or master_host.external_ip
		port = POSTGRESQL_DEFAULT_PORT
		ini = self._cnf.rawini
		private_key = ini.get(CNF_SECTION, 'root_ssh_private_key')
		public_key =  ini.get(CNF_SECTION, 'root_ssh_public_key')
		
		self.postgresql.init_slave(self._storage_path, host, port, private_key, public_key)
		
		# Update HostUp message
		message.postgresql = self._compat_storage_data(self.storage_vol)
		message.db_type = BEHAVIOUR


	def _update_config(self, data): 
		#XXX: I just don't like it
		self._cnf.update_ini(BEHAVIOUR, {CNF_SECTION: data})


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
			raise HandlerError('postgresql storage snapshot creation failed. See log for more details')
		
		return snap


	def _create_storage_snapshot(self):
		self._logger.info("Creating storage snapshot")
		try:
			return self.storage_vol.snapshot()
		except StorageError, e:
			self._logger.error("Cannot create PostgreSQL data snapshot. %s", e)
			raise
		

	def _compat_storage_data(self, vol=None, snap=None):
		ret = dict()
		if vol:
			ret['volume_config'] = vol.config()
		if snap:
			ret['snapshot_config'] = snap.config()
		return ret
	
	
	def _change_master(self, host, user, password, timeout):
		#TODO: WRITE changing process but look in Postgresql class first!
		'''
		fire:
		'before_postgresql_change_master'
		'postgresql_change_master'
		'''
		raise NotImplementedError

