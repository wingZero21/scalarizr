'''
Created on 14.06.2010

@author: spike
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler, HandlerError
from scalarizr.util import fstool, system, cryptotool, initd, disttool,\
	configtool

from subprocess import Popen, PIPE, STDOUT
import logging, os, re, shutil, time, pexpect
import signal



if disttool.is_redhat_based():
	initd_script = "/etc/init.d/mysqld"
elif disttool.is_debian_based():
	initd_script = "/etc/init.d/mysql"
else:
	raise HandlerError("Cannot find MySQL init script. Make sure that mysql server is installed")

pid_file = None
try:
	out = system("my_print_defaults mysqld_safe")
	m = re.search("--pid-file=(.*)", out, re.MULTILINE)
	if m:
		pid_file = m.group(1)
except:
	pass

# Register mysql service
logger = logging.getLogger(__name__)
logger.debug("Explore MySQL service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("mysql", initd_script, pid_file)



OPT_ROOT_USER 		= "root_user"
OPT_ROOT_PASSWORD 	= "root_password"
OPT_REPL_USER 		= "repl_user"
OPT_REPL_PASSWORD 	= "repl_password"
OPT_STAT_USER 		= "stat_user"
OPT_STAT_PASSWORD	= "stat_password"

def get_handlers ():
	return [MysqlHandler()]

class MysqlMessages:
	CREATE_DATA_BUNDLE = "Mysql_CreateDataBundle"
	CREATE_DATA_BUNDLE_RESULT = "Mysql_CreateDataBundleResult"
	CREATE_BACKUP = "Mysql_CreateBackup"
	CREATE_PMA_USER = "Mysql_CreatePmaUser"
	CREATE_PMA_USER_RESULT = "Mysql_CreatePmaUserResult"
	MASTER_UP = "Mysql_MasterUp"


class MysqlHandler(Handler):
	
	_logger = None
	_queryenv = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._iid = self._platform.get_instance_id()
		bus.on("init", self.on_init)		
				
	def on_init(self):
		bus.on("before_host_up", self.on_before_host_up)
		
	def on_before_host_up(self, message):
		config = bus.config
		role_name = config.get('general','role_name')
		role_params = self._queryenv.list_role_params(role_name)
		if role_params["mysql_data_storage_engine"]:
			# Poneslas' pizda po ko4kam
			if "master" == role_name:
				vol_id = role_params["mysql_ebs_vol_id"]
				devname = '/dev/sdo'
				ec2connection = self._platform.new_ec2_conn()
				# Attach ebs
				ebs_volumes = ec2connection.get_all_volumes([vol_id])
				
				if 1 == len(ebs_volumes):
					ebs_volume = ebs_volumes[0]
					if ebs_volume.volume_state() == 'available':
						ec2connection.attach_volume(vol_id, self._iid, devname)
						while ebs_volume.attachment_state() != 'attached':
							time.sleep(5)
				else:
					self._logger.error('Can\'t find volume with ID =  %s ', vol_id)
					raise

				# Mount ebs # fstool.mount()
				self._mount_device(devname)

				# Stop mysql server
				if initd.is_running("mysql"):
					try:
						initd.stop("mysql")
					except initd.InitdError, e:
						self._logger.error(e)
				# TODO: check that mysqld is really stopped
				# /var/run/mysqld.pid
				

				# If It's 1st init of mysql master			
				if not os.path.exists('/mnt/mysql-data') and not os.path.exists('/mnt/mysql-misc'):
					
					# Move datadir to EBS 
					self._change_mysql_dir('log_bin', '/mnt/mysql-misc')
					self._change_mysql_dir('datadir', '/mnt/mysql-data')
					
					# Start mysqld --skip-grant-tables
					
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
					
					myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
					
					# Define users and passwords
					root_user = "scalarizr"
					repl_user = "scalarizr_repl"
					stat_user = "scalarizr_stat"
					root_password, repl_password, stat_password = map(lambda x: re.sub('[\n=]','', cryptotool.keygen(32)), range(3))

					# Add users
					sql = "INSERT INTO mysql.user VALUES('%','"+root_user+"',PASSWORD('"+root_password+"')" + ",'Y'"*28 + ",''"*4 +',0'*4+");"
					sql += " INSERT INTO mysql.user (Host, User, Password, Repl_slave_priv) VALUES ('%','"+repl_user+"',PASSWORD('"+repl_password+"'),'Y');"
					sql += " INSERT INTO mysql.user (Host, User, Password, Repl_client_priv) VALUES ('%','"+stat_user+"',PASSWORD('"+stat_password+"'),'Y');"
					sql += " FLUSH PRIVILEGES;"
					myclient.communicate(sql)
					
					# Terminate mysqld --skip-grant-tables				
					myd.send_signal(signal.SIGTERM)

					# Save root user to /etc/scalr/private.d/behaviour.mysql.ini
					conf_updates = {configtool.get_behaviour_section_name(Behaviours.MYSQL) : {
						OPT_ROOT_USER			: root_user,
						OPT_ROOT_PASSWORD		: root_password,
						OPT_REPL_USER			: repl_user,
						OPT_REPL_PASSWORD		: repl_password,
						OPT_STAT_USER			: stat_user,
						OPT_STAT_PASSWORD		: stat_password
					}}
					configtool.update(configtool.get_behaviour_filename(Behaviours.MYSQL, ret=configtool.RET_PRIVATE), 
							conf_updates)
					
					self._master_replication_init()
					self._start_mysql()
					
					message.mysql_repl_user = repl_user
					message.mysql_repl_password = repl_password
					
				# If EBS volume had mysql dirs (N-th init)
				else:
					self._change_mysql_dir('log_bin', '/mnt/mysql-misc')
					self._change_mysql_dir('datadir', '/mnt/mysql-data')
					
					self._master_replication_init()
					self._start_mysql()
					
				# Creating snapshot 
				
				# Lock tables
				sql = pexpect.spawn('/usr/bin/mysql -u' + root_user + ' -p' + root_password)
				sql.sendline('FLUSH TABLES WITH READ LOCK;')
				sql.sendline('SHOW MASTER STATUS;')
				# Finding log file and log pos
				lines = sql.readline()
				try:
					while True:
						lines += sql.readline()
				except pexpect.TIMEOUT:
					pass
				log_row = re.search(re.compile('^\|\s*([\w-]*\.\d*)\s*\|\s*(\d*)', re.MULTILINE), lines)
				if log_row:
					log_file = log_row.group(1)
					log_pos = log_row.group(2)
				
				# Creating e2 snapshot
				snapshot = ebs_volume.create_snapshot()
				
				sql.sendline('UNLOCK TABLES;')
				sql.close()
				
				# Sending snapshot data to scalr
				message.snapshot_id = snapshot.id
				message.log_file = log_file
				message.log_pos = log_pos
				
					
			elif "slave" == role_name or "eph" == role_params["mysql_data_storage_engine"]:
				try:
					devname = '/dev/' + self._platform.get_block_device_mapping()["ephemeral0"]
				except Exception, e:
					self._logger.error('Cannot retrieve device %s info: %s', devname, e)
					raise
								
				self._mount_device(devname)					
			
				
	def _master_replication_init(self):
						
		# Create /etc/mysql if Hats
		if disttool.is_redhat_based():
			try:
				os.makedirs('/etc/mysql/')
			except OSError, e:
				self._logger.error('Couldn`t create directory /etc/mysql/: %s', e)
		
		# Writting replication config
		try:
			file = open('/etc/mysql/farm-replication.cnf', 'w')
		except IOError, e:
			self._logger.error('Cannot open /etc/mysql/farm-replication.cnf: %s', e.strerror )
			raise
		else:
			file.write('[mysqld]\nserver-id\t\t=\t1\nmaster-connect-retry\t\t=\t15\n')
			file.close()
		
		# Get my.cnf location
		if disttool.is_redhat_based():
			my_cnf_file = "/etc/my.cnf"
		else:
			my_cnf_file = "/etc/mysql/my.cnf"
		
		# Include farm-replication.cnf to my.cnf
		try:
			file = open(my_cnf_file, 'a')
		except IOError, e:
			self._logger.error('Can\'t open %s: %s', my_cnf_file, e.strerror )
			raise
		else:
			file.write('\n!include /etc/mysql/farm-replication.cnf\n')
			file.close()
	
	def _start_mysql(self):
		try:
			initd.start("mysql")
		except initd.InitdError, e:
			self._logger.error(e)
			raise
				
	def _change_mysql_dir(self, directive=None, dirname = None):
		if dirname and directive:
			
			if disttool.is_redhat_based():
				my_cnf_file = "/etc/my.cnf"
			else:
				my_cnf_file = "/etc/mysql/my.cnf"
				
			try:
				file = open(my_cnf_file, 'r')
			except IOError, e:
				self._logger.error('Can\'t open %s: %s', my_cnf_file, e.strerror )
				raise
			else:
				myCnf = file.readlines()
				file.close
			
			search_row = re.compile('(^\s*'+directive+'\s*=\s*)((/[\w-]+)+)[/\s](/[\w-]+\.\w+)?', re.MULTILINE)	
			src_dir_row = re.search(search_row, myCnf)
			if src_dir_row:
				if not os.path.isdir(dirname):
					src_dir = src_dir_row.group(2)
					if os.path.isdir(src_dir):
						self._logger.info('Copying mysql directory \'%s\' to \'%s\'', src_dir, dirname)
						shutil.copytree(src_dir, dirname)
						myCnf = re.sub(search_row, '\\1'+ dirname + '\n' , myCnf)
					else:
						self._logger.error('Mysql directory \'%s\' doesn\'t exist. Creating new in \'%s\'', src_dir, dirname)
						myCnf = re.sub(search_row, '' , myCnf)
						os.makedirs(dirname)
						myCnf += '\n' + directive +' = ' + dirname
				else:
					myCnf = re.sub(search_row, '\\1'+ dirname + '\n' , myCnf)
			else:
				if not os.path.isdir(dirname):
					os.makedirs(dirname)
				myCnf += '\n' + directive +' = ' + dir
				
			file = open('/etc/mysql/my.cnf', 'w')
			file.write(myCnf)
			file.close()
	
	def _mount_device(self, devname):
		fstab = fstool.Fstab()			
		if None != devname:
			try:
				fstool.mount(devname, '/mnt', ["-t auto"])
			except fstool.FstoolError, e:
				if -666 == e.code:
					system("/sbin/mkfs.ext3 -F " + devname + " 2>&1")
					try:
						fstool.mount(devname, '/mnt', ["-t auto"])
					except fstool.FstoolError, e:
						raise
				else:
					raise

		if not fstab.contains(devname, rescan=True):
			self._logger.info("Adding a record to fstab")
			fstab.append(fstool.TabEntry(devname, '/mnt', "auto", "defaults\t0\t0"))