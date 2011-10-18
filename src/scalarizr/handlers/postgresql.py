'''
Created on April 18th, 2011

@author: Dmytro Korsakov
'''
import os
import time
import shutil
import tarfile
import logging
import tempfile

from scalarizr import config
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import ServiceCtlHandler, HandlerError, DbMsrMessages
from scalarizr.util.filetool import split, rchown
from scalarizr.util import system2, wait_until
from scalarizr.storage import Storage, Snapshot, StorageError, Volume, transfer
from scalarizr.services.postgresql import PostgreSql, PSQL, ROOT_USER, PG_DUMP, OPT_REPLICATION_MASTER,\
	PgUser, SU_EXEC



BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.POSTGRESQL

PG_SOCKET_DIR 				= '/var/run/postgresql/'
STORAGE_PATH 				= "/mnt/pgstorage"
STORAGE_VOLUME_CNF 			= 'postgresql.json'
STORAGE_SNAPSHOT_CNF 		= 'postgresql-snap.json'

OPT_VOLUME_CNF				= 'volume_config'
OPT_SNAPSHOT_CNF			= 'snapshot_config'
OPT_ROOT_USER				= 'root_user'
OPT_ROOT_PASSWORD 			= "root_password"
OPT_ROOT_SSH_PUBLIC_KEY 	= "root_ssh_public_key"
OPT_ROOT_SSH_PRIVATE_KEY	= "root_ssh_private_key"
OPT_CURRENT_XLOG_LOCATION	= 'current_xlog_location'

BACKUP_CHUNK_SIZE 		= 200*1024*1024

POSTGRESQL_DEFAULT_PORT	= 5432

		
def get_handlers():
	return (PostgreSqlHander(), )


class PostgreSqlHander(ServiceCtlHandler):	
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
					message.name == DbMsrMessages.DBMSR_NEW_MASTER_UP
				or 	message.name == DbMsrMessages.DBMSR_PROMOTE_TO_MASTER
				or 	message.name == DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE
				or 	message.name == DbMsrMessages.DBMSR_CREATE_BACKUP
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
		#temporary fix for starting-after-rebundle issue
		if not os.path.exists(PG_SOCKET_DIR):
			os.makedirs(PG_SOCKET_DIR)
			rchown(user='postgres', path=PG_SOCKET_DIR)
			
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("before_reboot_start", self.on_before_reboot_start)
		bus.on("before_reboot_finish", self.on_before_reboot_finish)
		
		if self._cnf.state == ScalarizrState.RUNNING:

			storage_conf = Storage.restore_config(self._volume_config_path)
			self.storage_vol = Storage.create(storage_conf)
			if not self.storage_vol.mounted():
				self.storage_vol.mount()
			
			self.postgresql.service.start()
			
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
		self._storage_path = STORAGE_PATH
		self._tmp_path = os.path.join(self._storage_path, 'tmp')
		
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
		
		'''
		if message.postgresql[OPT_REPLICATION_MASTER] != '1'  and \
				(not message.body.has_key(OPT_ROOT_SSH_PUBLIC_KEY) or not 
				message.body.has_key(OPT_ROOT_SSH_PRIVATE_KEY)):
			raise HandlerError("HostInitResponse message for PostgreSQL slave must contain both public and private ssh keys")
		'''
		
		dir = os.path.dirname(self._volume_config_path)
		if not os.path.exists(dir):
			os.makedirs(dir)
		
		postgresql_data = message.postgresql.copy()

		root = PgUser(ROOT_USER)
		root.store_keys(postgresql_data[OPT_ROOT_SSH_PUBLIC_KEY], postgresql_data[OPT_ROOT_SSH_PRIVATE_KEY])
		del postgresql_data[OPT_ROOT_SSH_PUBLIC_KEY]
		del postgresql_data[OPT_ROOT_SSH_PRIVATE_KEY]		
		
		for key, file in ((OPT_VOLUME_CNF, self._volume_config_path), 
						(OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
			if os.path.exists(file):
				os.remove(file)
			
			if key in postgresql_data:
				if postgresql_data[key]:
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
				db_type 	= BEHAVIOUR,
				used_size	= '%.3f' % (float(used_size) / 1000,),
				status		= 'ok'
			)
			msg_data[BEHAVIOUR] = self._compat_storage_data(snap=snap)
			self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, msg_data)

		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(DbMsrMessages.DBMSR_CREATE_DATA_BUNDLE_RESULT, dict(
				db_type 	= BEHAVIOUR,
				status		='error',
				last_error	= str(e)
			))
			

	def on_DbMsr_PromoteToMaster(self, message):
		"""
		Promote slave to master
		@type message: scalarizr.messaging.Message
		@param message: postgresql_PromoteToMaster
		"""
		
		if message.db_type != BEHAVIOUR:
			self._logger.error('Wrong db_type in DbMsr_PromoteToMaster message: %s' % message.db_type)
			return
		
		if self.postgresql.is_replication_master:
			self._logger.warning('Cannot promote to master. Already master')
			return
		
		bus.fire('before_slave_promote_to_master')
		
		master_storage_conf = message.body.get('volume_config')
		tx_complete = False	
		old_conf 		= None
		new_storage_vol	= None		
					
		try:
						
			msg_data = dict(
					db_type=BEHAVIOUR, 
					status="ok",
			)
			
			self.postgresql.stop_replication()
			slaves = [host.internal_ip for host in self._get_slave_hosts()]
			
			if master_storage_conf:

				self.postgresql.service.stop('Unplugging slave storage and then plugging master one')

				old_conf = self.storage_vol.detach(force=True) # ??????
				new_storage_vol = self._plug_storage(self._storage_path, master_storage_conf)	
							
				# Continue if master storage is a valid postgresql storage 
				if not self.postgresql.cluster_dir.is_initialized(self._storage_path):
					raise HandlerError("%s is not a valid postgresql storage" % self._storage_path)
				
				Storage.backup_config(new_storage_vol.config(), self._volume_config_path) 
				msg_data[BEHAVIOUR] = self._compat_storage_data(vol=new_storage_vol)
				
			self.postgresql.init_master(self._storage_path, slaves)
			self._update_config({OPT_REPLICATION_MASTER : "1"})	
				
			if not master_storage_conf:
									
				snap = self._create_snapshot(ROOT_USER, message.root_password)
				Storage.backup_config(snap.config(), self._snapshot_config_path)
				msg_data[BEHAVIOUR] = self._compat_storage_data(self.storage_vol.config(), snap)
				
			msg_data[BEHAVIOUR].update({OPT_CURRENT_XLOG_LOCATION: None})		
			self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, msg_data)	
								
			tx_complete = True
			bus.fire('slave_promote_to_master')
			
		except (Exception, BaseException), e:
			self._logger.exception(e)
			if new_storage_vol:
				new_storage_vol.detach()
			# Get back slave storage
			if old_conf:
				self._plug_storage(self._storage_path, old_conf)
			
			self.send_message(DbMsrMessages.DBMSR_PROMOTE_TO_MASTER_RESULT, dict(
				db_type=BEHAVIOUR, 															
				status="error",
				last_error=str(e)
			))

			# Start postgresql
			self.postgresql.service.start()
		
		if tx_complete and master_storage_conf:
			# Delete slave EBS
			self.storage_vol.destroy(remove_disks=True)
			self.storage_vol = new_storage_vol
			Storage.backup_config(self.storage_vol.config(), self._volume_config_path)

	
	def on_DbMsr_NewMasterUp(self, message):
		"""
		Switch replication to a new master server
		@type message: scalarizr.messaging.Message
		@param message:  DbMsr_NewMasterUp
		"""
		if not message.body.has_key(BEHAVIOUR) or message.db_type != BEHAVIOUR:
			raise HandlerError("DbMsr_NewMasterUp message for PostgreSQL behaviour must have 'postgresql' property and db_type 'postgresql'")
		
		postgresql_data = message.postgresql.copy()
		
		if self.postgresql.is_replication_master:
			self._logger.debug('Skipping NewMasterUp. My replication role is master')	
			return 
		
		host = message.local_ip or message.remote_ip
		self._logger.info("Switching replication to a new postgresql master %s", host)
		bus.fire('before_postgresql_change_master', host=host)			
		
		if OPT_SNAPSHOT_CNF in postgresql_data:
			snap_data = postgresql_data[OPT_SNAPSHOT_CNF]
			self._logger.info('Reinitializing Slave from the new snapshot %s', 
					snap_data['id'])
			self.postgresql.service.stop()
			
			self._logger.debug('Destroying old storage')
			self.storage_vol.destroy()
			self._logger.debug('Storage destroyed')
			
			self._logger.debug('Plugging new storage')
			vol = Storage.create(snapshot=snap_data.copy())
			self._plug_storage(self._storage_path, vol)
			self._logger.debug('Storage plugged')
			
			Storage.backup_config(vol.config(), self._volume_config_path)
			Storage.backup_config(snap_data, self._snapshot_config_path)
			self.storage_vol = vol
			
		self.postgresql.init_slave(self._storage_path, host, POSTGRESQL_DEFAULT_PORT)
			
		self._logger.debug("Replication switched")
		bus.fire('postgresql_change_master', host=host)
			

	def on_DbMsr_CreateBackup(self, message):
		#TODO: Think how to move the most part of it into Postgresql class 
		# Retrieve password for scalr mysql user
		tmpdir = backup_path = None
		try:
			# Get databases list
			psql = PSQL(user=self.postgresql.root_user.name)
			databases = psql.list_pg_databases()
			
			if not os.path.exists(self._tmp_path):
				os.makedirs(self._tmp_path)
				
			# Defining archive name and path
			backup_filename = 'pgsql-backup-'+time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
			backup_path = os.path.join(self._tmp_path, backup_filename)
			
			# Creating archive 
			backup = tarfile.open(backup_path, 'w:gz')

			# Dump all databases
			self._logger.info("Dumping all databases")
			tmpdir = tempfile.mkdtemp(dir=self._tmp_path)		
			rchown(self.postgresql.root_user.name, tmpdir)	
			
			for db_name in databases:
				if db_name == 'template0':
					continue
				
				dump_path = tmpdir + os.sep + db_name + '.sql'
				pg_args = '%s %s --no-privileges -f %s' % (PG_DUMP, db_name, dump_path)
				su_args = [SU_EXEC, '-', self.postgresql.root_user.name, '-c', pg_args]
				err = system2(su_args)[1]
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
			self.send_message(DbMsrMessages.DBMSR_CREATE_BACKUP_RESULT, dict(
				db_type = BEHAVIOUR,
				status = 'ok',
				backup_parts = result
			))
						
		except (Exception, BaseException), e:
			self._logger.exception(e)
			
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
		
		msg_data.update({OPT_REPLICATION_MASTER 		: 	str(int(self.postgresql.is_replication_master)),
							OPT_ROOT_USER				:	self.postgresql.root_user.name,
							OPT_ROOT_PASSWORD			:	root_password,
							OPT_CURRENT_XLOG_LOCATION	: 	None})	
		#TODO: add xlog
			
		# Create snapshot
		snap = self._create_snapshot(ROOT_USER, root_password)
		
		Storage.backup_config(snap.config(), self._snapshot_config_path)
	
		# Update HostUp message 
		msg_data.update(self._compat_storage_data(self.storage_vol, snap))
			
		if msg_data:
			message.db_type = BEHAVIOUR
			message.postgresql = msg_data.copy()
			message.postgresql.update({
							OPT_ROOT_SSH_PRIVATE_KEY	: 	self.postgresql.root_user.private_key, 
							OPT_ROOT_SSH_PUBLIC_KEY 	: 	self.postgresql.root_user.public_key
							})
			try:
				del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
			except KeyError:
				pass 
			self._update_config(msg_data)
	
	def _get_master_host(self):
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
		return master_host
	
	def _get_slave_hosts(self):
		self._logger.info("Requesting standby servers")
		return list(host for host in self._queryenv.list_roles(self._role_name)[0].hosts 
				if not host.replication_master)
				
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
		master_host = self._get_master_host()
				
		self._logger.debug("Master server obtained (local_ip: %s, public_ip: %s)",
				master_host.internal_ip, master_host.external_ip)
		
		host = master_host.internal_ip or master_host.external_ip
		self.postgresql.init_slave(self._storage_path, host, POSTGRESQL_DEFAULT_PORT)
		
		# Update HostUp message
		message.postgresql = self._compat_storage_data(self.storage_vol)
		message.db_type = BEHAVIOUR


	def _update_config(self, data): 
		#XXX: I just don't like it
		#ditching empty data
		updates = dict()
		for k,v in data.items():
			if v: 
				updates[k] = v
		
		self._cnf.update_ini(BEHAVIOUR, {CNF_SECTION: updates})


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


	def _create_snapshot(self, root_user, root_password, dry_run=False):
		psql = PSQL()
		if self.postgresql.service.running:
			psql.start_backup()
		
		system2('sync', shell=True)
		# Creating storage snapshot
		snap = None if dry_run else self._create_storage_snapshot()
		if self.postgresql.service.running:
			psql.stop_backup()
		
		wait_until(lambda: snap.state in (Snapshot.CREATED, Snapshot.COMPLETED, Snapshot.FAILED))
		if snap.state == Snapshot.FAILED:
			raise HandlerError('postgresql storage snapshot creation failed. See log for more details')
		
		self._logger.info('PostgreSQL data bundle created\n  snapshot: %s', snap.id)
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
