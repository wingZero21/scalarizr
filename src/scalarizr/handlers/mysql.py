'''
Created on 14.06.2010

@author: spike
@author: marat
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler, HandlerError
from scalarizr.util import fstool, system, cryptotool, initd, disttool,\
		configtool, filetool, ping_service
from distutils import version
from subprocess import Popen, PIPE, STDOUT
import logging, os, re, time, pexpect
import signal, pwd, random
import shutil
from boto.exception import BotoServerError


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
	m = re.search("--pid[-_]file=(.*)", out, re.MULTILINE)
	if m:
		pid_file = m.group(1)
except:
	pass

# Register mysql service
logger = logging.getLogger(__name__)
logger.debug("Explore MySQL service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("mysql", initd_script, pid_file, tcp_port=3306)


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

# Role params
PARAM_MASTER_EBS_VOLUME_ID 	= "mysql_master_ebs_volume_id"
PARAM_DATA_STORAGE_ENGINE 	= "mysql_data_storage_engine"


ROOT_USER = "scalr"
REPL_USER = "scalr_repl"
STAT_USER = "scalr_stat"

STORAGE_DEVNAME = "/dev/sdo"
STORAGE_PATH = "/mnt/dbstorage"
STORAGE_DATA_DIR = "mysql-data"
STORAGE_BINLOG_PATH = "mysql-misc/binlog.log"

if disttool.is_redhat_based():
	MY_CNF_PATH = "/etc/my.cnf"
else:
	MY_CNF_PATH = "/etc/mysql/my.cnf"


def get_handlers ():
	return [MysqlHandler()]

class MysqlMessages:
	CREATE_DATA_BUNDLE = "Mysql_CreateDataBundle"
	CREATE_DATA_BUNDLE_RESULT = "Mysql_CreateDataBundleResult"
	CREATE_BACKUP = "Mysql_CreateBackup"
	CREATE_PMA_USER = "Mysql_CreatePmaUser"
	CREATE_PMA_USER_RESULT = "Mysql_CreatePmaUserResult"
	
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
	@ivar log_file
	@ivar log_pos
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
	
	_storage_path = _data_dir = _binlog_path = None
	""" Storage parameters """

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._sect_name = configtool.get_behaviour_section_name(Behaviours.MYSQL)
		self._sect = configtool.section_wrapper(bus.config, self._sect_name)
		config = bus.config
		self._role_name = config.get(configtool.SECT_GENERAL, configtool.OPT_ROLE_NAME)
		
		self._storage_path = STORAGE_PATH
		self._data_dir = os.path.join(self._storage_path, STORAGE_DATA_DIR)
		self._binlog_path = os.path.join(self._storage_path, STORAGE_BINLOG_PATH)
		
		bus.on("init", self.on_init)

	def on_init(self):
		bus.on("host_init_response", self.on_host_init_response)
		bus.on("before_host_up", self.on_before_host_up)

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return Behaviours.MYSQL in behaviour and (
					message.name == MysqlMessages.NEW_MASTER_UP
				or 	message.name == MysqlMessages.PROMOTE_TO_MASTER
				or 	message.name == MysqlMessages.CREATE_DATA_BUNDLE
				or 	message.name == MysqlMessages.CREATE_BACKUP
				or 	message.name == MysqlMessages.CREATE_PMA_USER)

	def on_Mysql_CreateDataBundle(self, message):
		# Retrieve password for salr mysql user
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
			log_pos=log_pos
		))

				
	def on_Mysql_PromoteToMaster(self, message):
		"""
		Promote slave to master
		@type message: scalarizr.messaging.Message
		@param message: Mysql_PromoteToMaster
		"""
		if not int(self._sect.get(OPT_REPLICATION_MASTER)):
			self._stop_mysql()
			
			ec2_conn = self._platform.new_ec2_conn()
			slave_vol_id = 	self._sect.get(OPT_STORAGE_VOLUME_ID)
			master_vol_id = self._queryenv.list_role_params(self._role_name)[PARAM_MASTER_EBS_VOLUME_ID]
			master_vol = None
			tx_complete = False
			try:
				self._unplug_storage(slave_vol_id, self._storage_path)
				
				master_vol = self._take_master_volume(master_vol_id)
				
				# Mount previous master's EBS volume						
				self._plug_storage(master_vol.id, self._storage_path)
				if self._storage_valid():
					# Point datadir and log_bin to ebs 
					self._move_mysql_dir('log_bin', self._binlog_path, 'mysqld')
					self._move_mysql_dir('datadir', self._data_dir + os.sep, 'mysqld')
					# Starting master replication			
					self._replication_init()
					# Save previous master's logins and passwords to config
					updates = {
						OPT_ROOT_PASSWORD : message.root_password,
						OPT_REPL_PASSWORD : message.repl_password,
						OPT_STAT_PASSWORD : message.stat_password,
						OPT_STORAGE_VOLUME_ID : master_vol.id,
						OPT_REPLICATION_MASTER 	: 1
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
				ec2_conn.detach_volume(slave_vol_id, force=True)
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
			self._change_master(
				host=host, 
				user=REPL_USER, 
				password=message.repl_password,
				log_file=message.log_file, 
				log_pos=message.log_pos, 
				mysql_user=ROOT_USER,
				mysql_password=message.root_password
			)			
			self._logger.debug("Replication switched")
		else:
			self._logger.debug('Skip NewMasterUp. My replication role is master')		

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
							
		msg_data = None
		storage_valid = self._storage_valid() # It's important to call it before _move_mysql_dir

		# Patch configuration
		self._move_mysql_dir('datadir', self._data_dir + os.sep, 'mysqld')
		self._move_mysql_dir('log_bin', self._binlog_path, 'mysqld')
		self._replication_init(master=True)
		
		# If It's 1st init of mysql master
		if not storage_valid:
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
			
			# Updating snapshot metadata
			snap_id, log_file, log_pos = self._create_snapshot(ROOT_USER, root_password, dry_run=True)
			
			# Send updated metadata to Scalr
			msg_data = dict(log_file=log_file, log_pos=log_pos)
			
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
			self._logger.info("Initialize slave storage")
			
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
					#TODO: Umount ebs device
					# Detach and delete EBS Volume 
					self._detach_delete_volume(ebs_volume)
					shutil.rmtree(tmpdir)
				else:
					raise HandlerError("EBS Volume does not contain mysql data")
			"""
			
		self._stop_mysql()			
				
		# Change configuration files
		self._logger.info("Changing configuration files")
		self._move_mysql_dir('datadir', self._data_dir, 'mysqld')
		self._move_mysql_dir('log_bin', self._binlog_path, 'mysqld')
		self._replication_init(master=False)
		if disttool._is_debian_based and os.path.exists(STORAGE_PATH + os.sep +'debian.cnf') :
			try:
				self._logger.info("Copying debian.cnf from storage to mysql configuration directory")
				shutil.copy(STORAGE_PATH + os.sep +'debian.cnf', '/etc/mysql/')
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
		devname = STORAGE_DEVNAME
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
		self._logger.info("Attaching volume %s as device %s", vol.id, devname)
		vol.attach(self._platform.get_instance_id(), devname)
		self._logger.debug("Checking that volume %s is attached", vol.id)
		self._wait_until(lambda: vol.update() and vol.attachment_state() == "attached")
		self._logger.debug("Volume %s attached",  vol.id)
			
		# Wait when device will be added
		self._logger.info("Checking that device %s is available", devname)
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
			self._logger.info("Unmounting storage %s", self._storage_path)
			fstool.umount(self._storage_path, clean_fstab=True)
			self._logger.debug("Storage %s unmounted", self._storage_path)
		
		# Detach volume
		self._logger.info("Detaching storage volume %s", vol.id)
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
		
		self._logger.info("Creating EBS volume from snapshot %s in avail zone %s", snap_id, avail_zone)
		ebs_volume = ec2_conn.create_volume(None, avail_zone, snap_id)
		self._logger.debug("Volume %s created from snapshot %s", ebs_volume.id, snap_id)
		
		self._logger.info('Checking that EBS volume %s is available', ebs_volume.id)
		self._wait_until(lambda: ebs_volume.update() == "available")
		self._logger.info("Volume %s available", ebs_volume.id)
		
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
		self._logger.info("Taking master EBS volume %s", volume_id)
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
			self._logger.info("Master volume is in another zone (volume zone: %s, server zone: %s) " + 
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
				
			self._logger.info("Use %s as master data volume", master_vol.id)
		
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
			sql.sendline('SHOW MASTER STATUS;')
			sql.expect('mysql>')
			
			# Retrieve log file and log position
			lines = sql.before		
			log_row = re.search(re.compile('^\|\s*([\w-]*\.\d*)\s*\|\s*(\d*)', re.MULTILINE), lines)
			if log_row:
				log_file = log_row.group(1)
				log_pos = log_row.group(2)
			else:
				log_file = log_pos = None
			
			if os.path.exists('/etc/mysql/debian.cnf'):
				try:
					self._logger.info("Copying debian.cnf file to storage")
					shutil.copy('/etc/mysql/debian.cnf', STORAGE_PATH)
				except BaseException, e:
					self._logger.error("Cannot copy debian.cnf file to storage: ", e)
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
		updates = {self._sect_name: data}
		configtool.update(configtool.get_behaviour_filename(Behaviours.MYSQL, ret=configtool.RET_PRIVATE), updates)
		
	"""
	def _update_config_users(self, root_user, root_password, repl_user, repl_password,
							       stat_user, stat_password):
		conf_updates = {self._section : {
			OPT_ROOT_USER		: root_user,
			OPT_ROOT_PASSWORD	: root_password,
			OPT_REPL_USER		: repl_user,
			OPT_REPL_PASSWORD	: repl_password,
			OPT_STAT_USER		: stat_user,
			OPT_STAT_PASSWORD	: stat_password
		}}
		configtool.update(configtool.get_behaviour_filename(Behaviours.MYSQL, ret=configtool.RET_PRIVATE),
			conf_updates)
	"""
		
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
			file = open(MY_CNF_PATH, 'a+')
		except IOError, e:
			self._logger.error('Cannot open %s: %s', MY_CNF_PATH, e.strerror)
			raise
		else:
			my_cnf = file.read()
			if not re.search(re.compile('^!include \/etc\/mysql\/farm-replication\.cnf', re.MULTILINE), my_cnf):
				file.write('\n!include /etc/mysql/farm-replication.cnf\n')
		finally:
			file.close()
		self._logger.debug("Include added")
		self._add_apparmor_rules(repl_conf_path)			
		self._restart_mysql()
		self._stop_mysql()
	

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
		if disttool.is_redhat_based():
			daemon = "/usr/libexec/mysqld"
		else:
			daemon = "/usr/sbin/mysqld"		
		if os.path.exists(daemon) and os.access(daemon, os.X_OK):
			self._logger.info("Starting mysql server with --skip-grant-tables")
			myd = Popen([daemon, '--skip-grant-tables'], stdin=PIPE, stdout=PIPE, stderr=STDOUT)
		else:
			self._logger.error("MySQL daemon '%s' doesn't exists", daemon)
			return False
		self._ping_mysql()
		
		return myd
			
	def _move_mysql_dir(self, directive=None, dirname = None, section=None):
		#Reading Mysql config file		
		try:
			file = open(MY_CNF_PATH, 'r')
		except IOError, e:
			raise HandlerError('Cannot open %s: %s' % (MY_CNF_PATH, e.strerror))
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
					self._logger.info('Copying mysql directory \'%s\' to \'%s\'', src_dir, directory)
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
				
		# Setting new directory permissions
		try:
			os.chown(directory, mysql_user.pw_uid, mysql_user.pw_gid)
		except OSError, e:
			self._logger.error('Cannot chown Mysql directory %s', directory)
					
		# Writing new MySQL config
		file = open(MY_CNF_PATH, 'w')
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
			self._logger.info("Mounting device %s to %s", devname, mpoint)
			fstool.mount(devname, mpoint, auto_mount=True)
			self._logger.debug("Device %s is mounted to %s", devname, mpoint)
		except fstool.FstoolError, e:
			if fstool.FstoolError.NO_FS == e.code:
				self._logger.warning("Mount failed with NO_FS error")
				self._logger.info("Creating file system on device %s and mount it again", devname)
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
