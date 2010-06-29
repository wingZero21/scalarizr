'''
Created on 14.06.2010

@author: spike
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler, HandlerError
from scalarizr.util import fstool, system, cryptotool, initd, disttool,\
		configtool, filetool, ping_service, UtilError
from distutils import version
from subprocess import Popen, PIPE, STDOUT
import logging, os, re, time, pexpect
import signal, pwd, random, tempfile, shutil

if disttool.is_redhat_based():
	initd_script = "/etc/init.d/mysqld"
elif disttool.is_debian_based():
	initd_script = "/etc/init.d/mysql"
else:
	raise HandlerError("Cannot find MySQL init script. Make sure that mysql server is installed")

pid_file = None
try:
	out = system("my_print_defaults mysqld")
	m = re.search("--pid_file=(.*)", out, re.MULTILINE)
	if m:
		pid_file = m.group(1)
except:
	pass

# Register mysql service
logger = logging.getLogger(__name__)
logger.debug("Explore MySQL service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("mysql", initd_script, pid_file, tcp_port=3306)


OPT_ROOT_USER   		= "root_user"
OPT_ROOT_PASSWORD   	= "root_password"
OPT_REPL_USER   		= "repl_user"
OPT_REPL_PASSWORD   	= "repl_password"
OPT_STAT_USER   		= "stat_user"
OPT_STAT_PASSWORD   	= "stat_password"
OPT_ROLE_NAME			= "role_name"

ROOT_USER = "scalr"
REPL_USER = "scalr_repl"
STAT_USER = "scalr_stat"

def get_handlers ():
	return [MysqlHandler()]

class MysqlMessages:
	CREATE_DATA_BUNDLE = "Mysql_CreateDataBundle"
	CREATE_DATA_BUNDLE_RESULT = "Mysql_CreateDataBundleResult"
	CREATE_BACKUP = "Mysql_CreateBackup"
	CREATE_PMA_USER = "Mysql_CreatePmaUser"
	CREATE_PMA_USER_RESULT = "Mysql_CreatePmaUserResult"
	PROMOTE_TO_MASTER	= "Mysql_PromoteToMaster"
	PROMOTE_TO_MASTER_RESULT = "Mysql_PromoteToMasterResult"
	NEW_MASTER_UP = "Mysql_NewMasterUp"
	"""
	@ivar behaviour
	@ivar local_ip
	@ivar remote_ip
	@ivar role_name		
	@ivar log_file
	@ivar log_pos
	@ivar repl_user
	@ivar repl_password
	"""


class MysqlHandler(Handler):
	_logger = None
	_queryenv = None

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._iid = self._platform.get_instance_id()
		self._config 		= bus.config
		self._role_name 	= self._config.get(configtool.SECT_GENERAL, OPT_ROLE_NAME)
		self._section	= configtool.get_behaviour_section_name(Behaviours.MYSQL)
		bus.on("init", self.on_init)

	def on_init(self):
		bus.on("before_host_up", self.on_before_host_up)

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return behaviour == Behaviours.MYSQL and \
				(message.name == MysqlMessages.MASTER_UP\
			or   message.name == MysqlMessages.PROMOTE_TO_MASTER\
			or   message.name == MysqlMessages.CREATE_DATA_BUNDLE\
			or   message.name == MysqlMessages.CREATE_BACKUP\
			or   message.name == MysqlMessages.CREATE_PMA_USER)

	def on_Mysql_CreateDataBundle(self, message):
		# Retrieve password for salr mysql user
		try:
			root_password	= self._config.get(self._section, OPT_ROOT_PASSWORD)
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
		if 'mysql_slave' == self._role_name:
			role_params = self._queryenv.list_role_params(self._role_name)
			self._stop_mysql()
			#TODO: Umount ephemeral or ebs
			vol_id = role_params["mysql_master_ebs_volume_id"]
			# Mount previous master's ebs
			self._init_storage(vol_id, '/mnt/dbstorage')
			if os.path.exists('/mnt/dbstorage/mysql-data') and os.path.exists('/mnt/dbstorage/mysql-misc'):
				# Point datadir and log_bin to ebs
				self._change_mysql_dir('log_bin', '/mnt/dbstorage/mysql-misc/binlog.log', 'mysqld')
				self._change_mysql_dir('datadir', '/mnt/dbstorage/mysql-data/', 'mysqld')
				# Starting master replication			
				self._replication_init()
				# Save previous master's logins and passwords to config
				self._update_config_users(message.root_user, message.root_password, 
										  message.repl_user, message.repl_password,
								          message.stat_user, message.stat_password)
				# Set role_name to master 
				self._config.set(configtool.SECT_GENERAL, OPT_ROLE_NAME, 'mysql_master')
				self._role_name = 'mysql_master'
				# Send message to Scalr
				self._send_message(MysqlMessages.PROMOTE_TO_MASTER_RESULT)
		else:
			self._logger.warning('Cannot promote to master. Already master')

	def on_Mysql_NewMasterUp(self, message):
		if 'mysql_slave' == self._role_name:
			# Get replication info from message
			ip = message.local_ip or message.remote_ip
			log_file	= message.log_file
			log_pos		= message.log_pos
			master_repl_user 	= message.repl_user
			master_repl_password = message.repl_password
			# Init slave replication
			self._replication_init(master=False)
			# Get password for scalr mysql user
			try:
				root_password	= self._config.get(self._section, OPT_ROOT_PASSWORD)
			except (Exception, BaseException):
				raise HandlerError('Cannot retrieve mysql login and password from config')
			# Changing replication master
			sql = pexpect.spawn('/usr/bin/mysql -u' + ROOT_USER + ' -p' + root_password)
			sql.expect('mysql>')
			sql.sendline('STOP SLAVE;')
			sql.expect('mysql>')
			sql.sendline('CHANGE MASTER TO MASTER_HOST="'+ip+'", \
							MASTER_USER="'+master_repl_user+'",\
		  					MASTER_PASSWORD="'+master_repl_password+'",\
							MASTER_LOG_FILE="'+log_file+'", \
							MASTER_LOG_POS='+str(log_pos)+';')
			sql.expect('mysql>')
			# Starting slave
			sql.sendline('START SLAVE;')
			sql.expect('mysql>')
			status = sql.before
			if re.search(re.compile('ERROR', re.MULTILINE), status):
				raise HandlerError('Cannot start mysql slave: %s' % status)
			
			# Sleeping for a while
			time.sleep(3)
			sql.sendline('SHOW SLAVE STATUS;')
			sql.expect('mysql>')
			# Retrieveing slave status row vith values
			status = sql.before.split('\r\n')[4].split('|')
			sql.close()
			io_status = status[11].strip()
			sql_status = status[12].strip()
			# Check for errors
			if 'Yes' != io_status:
				raise HandlerError ('IO Error while starting mysql slave: %s %s' %  (status[17], status[18]))
			if 'Yes' != sql_status:
				raise HandlerError('SQL Error while starting mysql slave: %s %s' %  (status[17], status[18]))
			
			self._logger.info('Successfully switched replication to a new MySQL master server')
		else:
			self._logger.error('Cannot change master host: our role_name is master')		

	def on_before_host_up(self, message):
		role_params = self._queryenv.list_role_params(self._role_name)
		if role_params["mysql_data_storage_engine"]:
			if "mysql_master" == self._role_name:
				self._before_host_up_master(role_params, message)					
			elif "mysql_slave" == self._role_name:
				self._before_host_up_slave(role_params)
	
	def _before_host_up_master(self, role_params, message):
		# Mount EBS
		vol_id = role_params["mysql_master_ebs_volume_id"]
		self._init_storage(vol_id, '/mnt/dbstorage')
		# If It's 1st init of mysql master
		if not os.path.exists('/mnt/dbstorage/mysql-data') and not os.path.exists('/mnt/dbstorage/mysql-misc'):					
			self._stop_mysql()					
			# Move datadir to EBS
			self._change_mysql_dir('datadir', '/mnt/dbstorage/mysql-data/', 'mysqld')
			self._change_mysql_dir('log_bin', '/mnt/dbstorage/mysql-misc/binlog.log', 'mysqld')
			self._replication_init()										
			root_password, repl_password, stat_password =  self._add_mysql_users(ROOT_USER,
																				 REPL_USER,
																				 STAT_USER)
			dry_run = role_params.get("create_ec2_snapshot", True)
			snap_id, log_file, log_pos = self._create_snapshot(ROOT_USER, root_password, dry_run)
			if None != snap_id:
				message.snapshot_id = snap_id
			message.mysql_root_user 	= ROOT_USER
			message.mysql_root_password = root_password
			message.mysql_repl_user 	= REPL_USER
			message.mysql_repl_password = repl_password
			message.mysql_stat_user 	= STAT_USER
			message.mysql_stat_password = stat_password
			message.log_file			= log_file
			message.log_pos				= log_pos
		# If EBS volume had mysql dirs (N-th init)
		else:
			self._stop_mysql()
			self._change_mysql_dir('log_bin', '/mnt/dbstorage/mysql-misc/binlog.log', 'mysqld')
			self._change_mysql_dir('datadir', '/mnt/dbstorage/mysql-data/', 'mysqld')
			self._replication_init()
			# Retrieve scalr's mysql username and password
			try:
				root_password	= self._config.get(self._section, OPT_ROOT_PASSWORD)
			except Exception, e:
				raise HandlerError('Cannot retrieve mysql login and password from config: %s' % (e,))
			# Updating snapshot metadata
			snap_id, log_file, log_pos = self._create_snapshot(ROOT_USER, root_password)
			# Sending updated metadata to scalr
			message.log_file = log_file
			message.log_pos = log_pos
			self._start_mysql()
	
	def _before_host_up_slave(self, role_params):
		if not os.path.exists('mnt/dbstorage/mysql-data') and not os.path.exists('mnt/dbstorage/mysql-misc'):
			snap_id = role_params['ebs_snap_id']
			ebs_volume = self._create_volume_from_snap(snap_id)
			# Waiting until ebs volume will be created							
			if "ebs" == role_params["mysql_data_storage_engine"]:
				self._init_storage(ebs_volume.id, '/mnt/dbstorage')
			elif "eph" == role_params["mysql_data_storage_engine"]:
				# Mount ephemeral device
				try:
					devname = '/dev/' + self._platform.get_block_device_mapping()["ephemeral0"]
				except Exception, e:
					raise HandlerError('Cannot retrieve device %s info: %s' % (devname, e))
				self._mount_device(devname, '/mnt/dbstorage/')
				# Mount ebs with mysql data
				# tmpdir = tempfile.mkdtemp()
				tmpdir = '/mnt/tmpdir'
				self._init_storage(ebs_volume.id, tmpdir)
				if os.path.exists(tmpdir+'/mysql-data') and os.path.exists(tmpdir+'/mysql-misc'):
					# Rsync data from ebs to ephemeral device
					rsync = filetool.Rsync().archive()
					rsync.source(tmpdir+'/').dest('/mnt/dbstorage/')
					out, err, retcode = system(str(rsync))
					if err:
						raise HandlerError("Cannot copy data from ebs to ephemeral: %s" % (err,))
					#TODO: Umount ebs device
					# Detach and delete EBS Volume 
					self._detach_delete_volume(ebs_volume)
					shutil.rmtree(tmpdir)
				else:
					raise HandlerError("EBS Volume does not contain mysql data")
				
			# Change datadir and binary logs dir to ephemeral or ebs
		self._change_mysql_dir('datadir', '/mnt/dbstorage/mysql-data/', 'mysqld')
		self._change_mysql_dir('log_bin', '/mnt/dbstorage/mysql-misc/binlog.log', 'mysqld')
		# Initialize replication
		self._replication_init(master=False)
		# Adding mysql users
					
	def _init_storage(self, vol_id, mnt_point):
			devname = '/dev/sdo'
			ec2connection = self._platform.new_ec2_conn()
			# Attach ebs
			ebs_volumes = ec2connection.get_all_volumes([vol_id])
			if 1 == len(ebs_volumes):
				ebs_volume = ebs_volumes[0]
				if 'available' != ebs_volume.volume_state():
					ebs_volume.detach(force=True)
					while ebs_volume.attachment_state() != 'available':
						time.sleep(5)
				ebs_volume.attach(self._iid, devname)
				self._logger.info('Waiting while volume ID=%s attaching', vol_id)
				while ebs_volume.attachment_state() != 'attached':
					time.sleep(5)
			else:
				raise HandlerError('Cannot find volume with ID =  %s ' % (vol_id,))
			# Mount ebs # fstool.mount()
			self._mount_device(devname, mnt_point)
	
	def _create_volume_from_snap(self, snap_id):
		ec2connection = self._platform.new_ec2_conn()
		avail_zone = self._platform.get_avail_zone()
		ebs_volume = ec2connection.create_volume(zone=avail_zone, snapshot=snap_id)
		self._logger.info('Waiting until ebs volume will be created from snapshot "%s"', snap_id)
		while 'available' != ebs_volume.volume_state():
			time.sleep(5)
		del ec2connection
		return ebs_volume
	
	def _detach_delete_volume(self, volume):
		if volume.detach():
			if not volume.delete():
				raise HandlerError("Cannot delete volume ID=%s", (volume.id,))
		else:
			raise HandlerError("Cannot detach volume ID=%s" % (volume.id,))
		
	def _start_mysql_skip_grant_tables(self):
		self._stop_mysql()
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
		ping_service('127.0.0.1', 3306, 5)
		return myd


	def _create_snapshot(self, root_user, root_password, dry_run = True):
		self._start_mysql()
		# Lock tables
		sql = pexpect.spawn('/usr/bin/mysql -u' + root_user + ' -p' + root_password)
		#sql = pexpect.spawn('/usr/bin/mysql -uroot -p123')
		sql.expect('mysql>')
		sql.sendline('FLUSH TABLES WITH READ LOCK;')
		sql.expect('mysql>')
		sql.sendline('SHOW MASTER STATUS;')
		sql.expect('mysql>')
		lines = sql.before
		# Retrieve log file and log position
		log_row = re.search(re.compile('^\|\s*([\w-]*\.\d*)\s*\|\s*(\d*)', re.MULTILINE), lines)
		if log_row:
			log_file = log_row.group(1)
			log_pos = log_row.group(2)
		else:
			log_file = log_pos = None
		# Creating ebs snapshot
		if not dry_run:
			snapshot = self._ebs_volume.create_snapshot()
			snap_id = snapshot.id
		else:
			snap_id = None		
		sql.sendline('UNLOCK TABLES;\n')
		sql.close()
		return snap_id, log_file, log_pos

		# Sending snapshot data to scalr


	def _add_mysql_users(self, root_user, repl_user, stat_user):		
		myd = self._start_mysql_skip_grant_tables()
		myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
		out,err = myclient.communicate('SELECT VERSION();')
		mysql_ver_str = re.search(re.compile('\d*\.\d*\.\d*', re.MULTILINE), out)
		if mysql_ver_str:
			mysql_ver = version.StrictVersion(mysql_ver_str.group(0))
			if mysql_ver >= version.StrictVersion('5.1.6'):
				priv_count = 28
			else:
				priv_count = 26
		else:
			self._logger.error("Cannot determine mysql version.")
			raise			
		myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
		# Define users and passwords
		root_password, repl_password, stat_password = map(lambda x: re.sub('[^\w]','', cryptotool.keygen(20)), range(3))
		# Add users
		sql = "INSERT INTO mysql.user VALUES('%','"+root_user+"',PASSWORD('"+root_password+"')" + ",'Y'"*priv_count + ",''"*4 +',0'*4+");"
		sql += " INSERT INTO mysql.user (Host, User, Password, Repl_slave_priv) VALUES ('%','"+repl_user+"',PASSWORD('"+repl_password+"'),'Y');"
		sql += " INSERT INTO mysql.user (Host, User, Password, Repl_client_priv) VALUES ('%','"+stat_user+"',PASSWORD('"+stat_password+"'),'Y');"
		sql += " FLUSH PRIVILEGES;"
		out,err = myclient.communicate(sql)
		# Save root user to /etc/scalr/private.d/behaviour.mysql.ini
		self._update_config_users(root_user, root_password, repl_user, repl_password,
								  stat_user, stat_password)
		os.kill(myd.pid, signal.SIGTERM)
		time.sleep(3)
		self._start_mysql()
		ping_service('127.0.0.1', 3306, 5)
		return (root_password, repl_password, stat_password)
	
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
		
	def _replication_init(self, master=True):
		# Create /etc/mysql if Hats
		if disttool.is_redhat_based():
			if not os.path.exists('/etc/mysql/'):
				try:
					os.makedirs('/etc/mysql/')
				except OSError, e:
					raise HandlerError('Couldn`t create directory /etc/mysql/: %s' % (e,))
		# Writting replication config
		try:
			file = open('/etc/mysql/farm-replication.cnf', 'w')
		except IOError, e:
			raise HandlerError('Cannot open /etc/mysql/farm-replication.cnf: %s' % (e.strerror,))
		else:
			server_id = 1 if master else int(random.random() * 100000)+1
			file.write('[mysqld]\nserver-id\t\t=\t'+ str(server_id)+'\nmaster-connect-retry\t\t=\t15\n')
			file.close()
		# Get my.cnf location
		if disttool.is_redhat_based():
			my_cnf_file = "/etc/my.cnf"
		else:
			my_cnf_file = "/etc/mysql/my.cnf"
		# Include farm-replication.cnf to my.cnf
		try:
			file = open(my_cnf_file, 'a+')
		except IOError, e:
			self._logger.error('Can\'t open %s: %s', my_cnf_file, e.strerror )
			raise
		else:
			my_cnf = file.read()
			if not re.search(re.compile('^!include \/etc\/mysql\/farm-replication\.cnf', re.MULTILINE), my_cnf):
				file.write('\n!include /etc/mysql/farm-replication.cnf\n')
		finally:
			file.close()
		self._restart_mysql()

	def _start_mysql(self):
		try:
			initd.start("mysql")
			ping_service('127.0.0.1', 3306, 5)
		except initd.InitdError, e:
			self._logger.error(e)
		except UtilError, e:
			self._logger.error(e)

	def _stop_mysql(self):
		try:
			initd.stop("mysql")
		except initd.InitdError, e:
			logger.error(e)
			
	def _restart_mysql(self):
		self._stop_mysql()
		self._start_mysql()
			
	def _change_mysql_dir(self, directive=None, dirname = None, section=None):
		# Locating mysql config file			
		if disttool.is_redhat_based():
			my_cnf_file = "/etc/my.cnf"
		else:
			my_cnf_file = "/etc/mysql/my.cnf"		
		#Reading Mysql config file		
		try:
			file = open(my_cnf_file, 'r')
		except IOError, e:
			raise HandlerError('Cannot open %s: %s' % my_cnf_file, e.strerror)
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
		file = open('/etc/mysql/my.cnf', 'w')
		file.write(myCnf)
		file.close()		
		# Adding rules to apparmor config 
		if disttool.is_debian_based():
			try:
				file = open('/etc/apparmor.d/usr.sbin.mysqld', 'r')
			except IOError, e:
				pass
			else:
				app_rules = file.read()
				file.close()
				if not re.search (directory, app_rules):
					file = open('/etc/apparmor.d/usr.sbin.mysqld', 'w')
					app_rules = re.sub(re.compile('(\.*)(\})', re.S), '\\1\n'+directory+' r,\n'+'\\2', app_rules)
					app_rules = re.sub(re.compile('(\.*)(\})', re.S), '\\1'+directory+'* rwk,\n'+'\\2', app_rules)
					file.write(app_rules)
					file.close()
					initd.explore('apparmor', '/etc/init.d/apparmor')
					try:
						initd.reload('apparmor', True)
					except initd.InitdError, e:
						self._logger.error('Cannot restart apparmor. %s', e)									
	"""
	def _mount_device(self, devname, mnt_point):
			fstab = fstool.Fstab()
			if None != devname:
					try:
						fstool.mount(devname, '/mnt/dbstorage/', ["-t auto"])
					except fstool.FstoolError, e:
						if fstool.FstoolError.NO_FS == e.code:
							system("/sbin/mkfs.ext3 -F " + devname + " 2>&1")
							try:
								fstool.mount(devname, mnt_point, ["-t auto"])
							except fstool.FstoolError, e:
								raise
						else:
							raise
			if not fstab.contains(devname, rescan=True):
				self._logger.info("Adding a record to fstab")
				fstab.append(fstool.TabEntry(devname, mnt_point, "auto", "defaults\t0\t0"))
	"""
	
	def _mount_device(self, devname):
		try:
			self._logger.debug("Trying to mount device %s and add it to fstab", devname)
			fstool.mount(device = devname, options=["-t auto"], auto_mount = True)
		except fstool.FstoolError, e:
			if fstool.FstoolError.NO_FS == e.code:
				self._logger.debug("Trying to create file system on device %s, mount it and add to fstab", devname)
				fstool.mount(device = devname, options=["-t auto"], make_fs = True, auto_mount = True)
			else:
				raise