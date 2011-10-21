'''
Created on Sep 30, 2011

@author: Dmytro Korsakov
'''
import os
import time
import shutil
import logging
import tarfile
import tempfile

from scalarizr import config
from scalarizr.bus import bus
from scalarizr.messaging import Messages
from scalarizr.util import system2, wait_until
from scalarizr.util.filetool import split, rchown
from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import ServiceCtlHandler, HandlerError
from scalarizr.storage import Storage, Snapshot, StorageError, Volume, transfer
from scalarizr.services.mongodb import MongoDB, MongoCLI, MongoDump


BEHAVIOUR = SERVICE_NAME = CNF_SECTION = BuiltinBehaviours.MONGODB

STORAGE_PATH				= "/mnt/mongodb-storage"
STORAGE_VOLUME_CNF		  = 'mongodb.json'
STORAGE_SNAPSHOT_CNF		= 'mongodb-snap.json'

OPT_VOLUME_CNF			  = 'volume_config'
OPT_SNAPSHOT_CNF			= 'snapshot_config'
OPT_KEYFILE			= "mongodb_keyfile"

BACKUP_CHUNK_SIZE		 = 200*1024*1024

MONGODB_DEFAULT_PORT	= 27017

		
def get_handlers():
	return (MongoDBHandler(), )


class MongoDBMessages:

	CREATE_DATA_BUNDLE = "MongoDB_CreateDataBundle"
	
	CREATE_DATA_BUNDLE_RESULT = "MongoDB_CreateDataBundleResult"
	'''
	@ivar status: ok|error
	@ivar last_error
	@ivar snapshot_config
	@ivar used_size
	'''
	
	CREATE_BACKUP = "MongoDB_CreateBackup"
	
	CREATE_BACKUP_RESULT = "MongoDB_CreateBackupResult"
	"""
	@ivar status: ok|error
	@ivar last_error
	@ivar backup_urls: S3 URL
	"""

	"""
	Also MySQL behaviour adds params to common messages:
	
	= HOST_INIT_RESPONSE =
	@ivar MongoDB=dict(
		key_file				 A key file with at least 6 Base64 characters
		volume_config			Master storage configuration			(on master)
		snapshot_config			Master storage snapshot				 (both)
	)
	
	= HOST_UP =
	@ivar mysql=dict(
		root_password:			 'scalr' user password					  (on master)
		repl_password:			 'scalr_repl' user password				(on master)
		stat_password:			 'scalr_stat' user password				(on master)
		log_file:				 Binary log file							(on master) 
		log_pos:				 Binary log file position				(on master)
		volume_config:			Current storage configuration			(both)
		snapshot_config:		Master storage snapshot					(on master)		 
	) 
	"""



class MongoDBHandler(ServiceCtlHandler):
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
				message.name == MongoDBMessages.CREATE_DATA_BUNDLE
				or  message.name == MongoDBMessages.CREATE_BACKUP
				or  message.name == Messages.UPDATE_SERVICE_CONFIGURATION
				or  message.name == Messages.HOST_INIT
				or  message.name == Messages.BEFORE_HOST_TERMINATE)	
	
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		bus.on("init", self.on_init)
		bus.define_events(
			'before_%s_data_bundle' % BEHAVIOUR,
			
			'%s_data_bundle',
			
			# @param host: New master hostname 
			'before_%s_change_master',
			
			# @param host: New master hostname 
			'%s_change_master',
			
			'before_slave_promote_to_master',
			
			'slave_promote_to_master'
		)	
		
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
			
			self.mongodb.mongod.start()
			
	
	def on_reload(self):
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._role_name = ini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)
		self._storage_path = STORAGE_PATH
		
		self._volume_config_path  = self._cnf.private_path(os.path.join('storage', STORAGE_VOLUME_CNF))
		self._snapshot_config_path = self._cnf.private_path(os.path.join('storage', STORAGE_SNAPSHOT_CNF))
		self.mongodb = MongoDB()
		

	def on_host_init_response(self, message):
		"""
		Check MongoDB data in host init response
		@type message: scalarizr.messaging.Message
		@param message: HostInitResponse
		"""
		if not message.body.has_key(BEHAVIOUR):
			raise HandlerError("HostInitResponse message for %s behaviour must have '%s' property " 
							% (BEHAVIOUR, BEHAVIOUR))
		
		dir = os.path.dirname(self._volume_config_path)
		if not os.path.exists(dir):
			os.makedirs(dir)
		
		mongodb_data = message.mongodb.copy()	
		self._logger.info('Got %s part of HostInitResponse: %s' % (BEHAVIOUR, mongodb_data))
		
		for key, file in ((OPT_VOLUME_CNF, self._volume_config_path), 
						(OPT_SNAPSHOT_CNF, self._snapshot_config_path)):
			if os.path.exists(file):
				os.remove(file)
			
			if key in mongodb_data:
				if mongodb_data[key]:
					Storage.backup_config(mongodb_data[key], file)
				del mongodb_data[key]
				
		self._logger.debug("Update %s config with %s", (BEHAVIOUR, mongodb_data))
		self._update_config(mongodb_data)
		
		is_master = len(self._queryenv.list_roles(self._role_name)[0].hosts) > 0
		repl = 'master' if is_master else 'slave'
		
		if is_master:
			self._init_master(message)									  
		else:
			self._init_slave(message)
			
		bus.fire('service_configured', service_name=SERVICE_NAME, replication=repl)  
	
	def on_before_host_up(self, message):
		"""
		Check that replication is up in both master and slave cases
		@type message: scalarizr.messaging.Message		
		@param message: HostUp message
		"""
	
		self.mongodb.check_replication_status()	  
			

	def on_HostInit(self, message):
		if message.local_ip != self._platform.get_private_ip():
			if self.mongodb.is_replication_master:
				self.mongodb.register_slave(message.local_ip)

	def on_HostUp(self, message):
		if message.local_ip != self._platform.get_private_ip():
			if self.mongodb.is_replication_master:			   
				r = len(self.mongodb.replicas) 
				a = len(self.mongodb.arbiters)
				if r % 2 == 0 and not a:
					self.mongodb.register_arbiter('127.0.0.1')
				elif r % 2 != 0 and a:
					for arbiter in self.mongodb.arbiters:
						self.mongodb.unregister_slave(arbiter)
					
					
	def on_before_reboot_start(self, *args, **kwargs):
		self.mongodb.mongod.stop('Rebooting instance')
		pass

	def on_before_reboot_finish(self, *args, **kwargs):
		#self.mongodb.working_dir.unlock()
		pass


	def on_BeforeHostTerminate(self, message):
		if message.local_ip == self._platform.get_private_ip():
			self.mongodb.mongod.stop('Server will be terminated')
			self._logger.info('Detaching %s storage' % BEHAVIOUR)
			self.storage_vol.detach()
	
	
	def on_MongoDB_CreateDataBundle(self, message):
		
		try:
			bus.fire('before_%s_data_bundle' % BEHAVIOUR)
			# Creating snapshot		
			snap = self._create_snapshot()
			used_size = int(system2(('df', '-P', '--block-size=M', self._storage_path))[0].split('\n')[1].split()[2][:-1])
			bus.fire('%s_data_bundle' % BEHAVIOUR, snapshot_id=snap.id)			
			
			# Notify scalr
			msg_data = dict(
				used_size	= '%.3f' % (float(used_size) / 1000,),
				status		= 'ok'
			)
			msg_data[BEHAVIOUR] = self._compat_storage_data(snap=snap)
			self.send_message(MongoDBMessages.CREATE_DATA_BUNDLE_RESULT, msg_data)

		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(MongoDBMessages.CREATE_DATA_BUNDLE_RESULT, dict(
				status		='error',
				last_error	= str(e)
			))
	
			
	def on_MongoDB_CreateBackup(self, message):
		tmpdir = backup_path = None
		try:
			# Get databases list
			cli = MongoCLI()
			dbs = cli.list_databases()
			
			# Defining archive name and path
			backup_filename = '%s-backup-'%BEHAVIOUR + time.strftime('%Y-%m-%d-%H:%M:%S')+'.tar.gz'
			backup_path = os.path.join('/tmp', backup_filename)
			
			# Creating archive 
			backup = tarfile.open(backup_path, 'w:gz')

			# Dump all databases
			self._logger.info("Dumping all databases")
			tmpdir = tempfile.mkdtemp()		
			rchown('mongodb', tmpdir)  
			
			md = MongoDump()  
			
			for db_name in dbs:
				dump_path = tmpdir + os.sep + db_name + '.bson'
				err = md.create(db_name, dump_path)[1]
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
			self._logger.info("%s backup uploaded to cloud storage under %s/%s", 
							BEHAVIOUR, self._platform.cloud_storage_path, backup_filename)
			
			# Notify Scalr
			self.send_message(MongoDBMessages.CREATE_BACKUP_RESULT, dict(
				status = 'ok',
				backup_parts = result
			))
						
		except (Exception, BaseException), e:
			self._logger.exception(e)
			
			# Notify Scalr about error
			self.send_message(MongoDBMessages.CREATE_BACKUP_RESULT, dict(
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
		Initialize mongodb master
		@type message: scalarizr.messaging.Message 
		@param message: HostInitResponse message
		"""
		
		self._logger.info("Initializing %s master" % BEHAVIOUR)
		
		# Plug storage
		volume_cnf = Storage.restore_config(self._volume_config_path)
		try:
			snap_cnf = Storage.restore_config(self._snapshot_config_path)
			volume_cnf['snapshot'] = snap_cnf
		except IOError:
			pass
		self.storage_vol = self._plug_storage(mpoint=self._storage_path, vol=volume_cnf)
		Storage.backup_config(self.storage_vol.config(), self._volume_config_path)	
		
		self.mongodb.prepare(message.mongodb.replica_set_name, self._storage_path, message.mongodb.http_interface)
		self.mongodb.mongod.start()  
		self.mongodb.cli.initiate_rs()
		
		msg_data = dict()

		# Create snapshot
		snap = self._create_snapshot()
		Storage.backup_config(snap.config(), self._snapshot_config_path)
	
		# Update HostInitResponse message 
		msg_data.update(self._compat_storage_data(self.storage_vol, snap))
			
		if msg_data:
			message.mongodb = msg_data.copy()
			try:
				del msg_data[OPT_SNAPSHOT_CNF], msg_data[OPT_VOLUME_CNF]
			except KeyError:
				pass 
			self._update_config(msg_data)		

	def _get_keyfile(self):
		password = None 
		if self._cnf.rawini.has_option(CNF_SECTION, OPT_KEYFILE):
			password = self._cnf.rawini.get(CNF_SECTION, OPT_KEYFILE)	
		return password		
	
				
	def _init_slave(self, message):
		"""
		Initialize mongodb slave
		@type message: scalarizr.messaging.Message 
		@param message: HostUp message
		"""
		self._logger.info("Initializing %s slave" % BEHAVIOUR)
		
		# Plug storage
		volume_cfg = Storage.restore_config(self._volume_config_path)
		volume = Storage.create(Storage.blank_config(volume_cfg))	
		self.storage_vol =	 self._plug_storage(self._storage_path, volume)
		Storage.backup_config(self.storage_vol.config(), self._volume_config_path)
		
		self.mongodb.prepare(message.mongodb.replica_set_name, self._storage_path, message.mongodb.http_interface)
		self.mongodb.mongod.start()  
		
		# Update HostInitResponse message
		message.mongodb = self._compat_storage_data(self.storage_vol)


	def _update_config(self, data): 
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
			if 'you must specify the filesystem type' in str(e):
				vol.mkfs()
				vol.mount(mpoint)
			else:
				raise
		return vol


	def _create_snapshot(self):
		
		system2('sync', shell=True)
		# Creating storage snapshot
		snap = self._create_storage_snapshot()
			
		wait_until(lambda: snap.state in (Snapshot.CREATED, Snapshot.COMPLETED, Snapshot.FAILED))
		if snap.state == Snapshot.FAILED:
			raise HandlerError('%s storage snapshot creation failed. See log for more details' % BEHAVIOUR)
		
		return snap


	def _create_storage_snapshot(self):
		#TODO: check mongod journal option if service is running!
		self._logger.info("Creating storage snapshot")
		try:
			return self.storage_vol.snapshot()
		except StorageError, e:
			self._logger.error("Cannot create %s data snapshot. %s", (BEHAVIOUR, e))
			raise
		

	def _compat_storage_data(self, vol=None, snap=None):
		ret = dict()
		if vol:
			ret['volume_config'] = vol.config()
		if snap:
			ret['snapshot_config'] = snap.config()
		return ret

	
		