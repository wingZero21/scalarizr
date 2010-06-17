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
import logging, os, re, shutil, time
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



OPT_ROOT_USER = "root_user"
OPT_ROOT_PASSWORD = "root_password"
OPT_REPL_USER = "repl_user"
OPT_REPL_PASSWORD = "repl_password"


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
				volId = role_params["mysql_ebs_vol_id"]
				devname = '/dev/sdo'
				ec2connection = self._platform.new_ec2_conn()
				# Attach ebs
				ebsVolumes = ec2connection.get_all_volumes([volId])
				
				if 1 == len(ebsVolumes):
					ebsVolume = ebsVolumes[0]
					if ebsVolume.volume_state() == 'available':
						ec2connection.attach_volume(volId, self._iid, devname)
						while ebsVolume.attachment_state() != 'attached':
							time.sleep(5)
				else:
					self._logger.error('Can\'t find volume with ID =  %s ', volId)
					raise

				# Mount ebs # fstool.mount()
				self._mount_device(devname)
			
			elif "slave" == role_name or "eph" == role_params["mysql_data_storage_engine"]:
				try:
					devname = '/dev/' + self._platform._fetch_ec2_meta('latest/meta-data/block-device-mapping/ephemeral0')
				except Exception, e:
					self._logger.error('Can\'t retrieve device %s info: %s', devname, e)
					raise
								
				self._mount_device(devname)	
			
			if not os.path.exists('/mnt/mysql-data') and not os.path.exists('/mnt/mysql-misc'):
				# Stop mysql server
				if initd.is_running("mysql"):
					try:
						initd.stop("mysql")
					except initd.InitdError, e:
						self._logger.error(e)
				
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
				
				# Add root user
				myclient = Popen(["/usr/bin/mysql"], stdin=PIPE, stdout=PIPE, stderr=PIPE)
				
				user = "scalr"
				password = re.sub('[\n=]','', cryptotool.keygen(32))
				sql = "INSERT INTO mysql.user VALUES('%','"+user+"',PASSWORD('"+password+"')" + ",'Y'"*26 + ",''"*4 +',0'*4+");"
				sql += "FLUSH PRIVILEGES;"
				myclient.communicate(sql)
				
				# Terminate mysqld --skip-grant-tables
				myd.send_signal(signal.SIGTERM)	
				
				"""
				try:
					out, err, retcode = system(start_command, shell=False)
					if retcode or (out and out.find("ERROR") != -1):
						self._logger.error("Cannot add mysql user 'scalarizr': %s", out)
					else:
						self._logger.info("Mysql user 'scalarizr' successfully added.")
				except OSError, e:
					self._logger.error("Mysql user 'scalarizr' adding failed by running %s. %s",
						''.join(reload_command), e.strerror)
				"""
				
				
				# Save root user to /etc/scalr/private.d/behaviour.mysql.ini
				conf_updates = {configtool.get_behaviour_section_name(Behaviours.MYSQL) : {
					OPT_ROOT_USER : user,
					OPT_ROOT_PASSWORD : password
				}}
				configtool.update(configtool.get_behaviour_filename(Behaviours.MYSQL, ret=configtool.RET_PRIVATE), 
						conf_updates)

				

			if "master":
				message.mysql_repl_user = "scalarizr"
				message.mysql_repl_password = password

	
	def _change_mysql_dir(self, directive=None, dirname = None):
		if dirname and directive:
			try:
				file = open('/etc/mysql/my.cnf', 'r')
			except IOError, e:
				self._logger.error('Can\'t open /etc/mysql/my.cnf: %s', e.strerror )
				raise
			else:
				myCnf = file.readlines()
				file.close
			
			searchRow = re.compile('(^\s*'+directive+'\s*=\s*)((/[\w-]+)+)[/\s](/[\w-]+\.\w+)?', re.MULTILINE)	
			srcDirRow = re.search(searchRow, myCnf)
			if srcDirRow:
				srcDir = srcDirRow.group(2)
				if os.path.isdir(srcDir):
					self._logger.info('Copying mysql directory \'%s\' to \'%s\'', srcDir, dirname)
					shutil.copytree(srcDir, dirname)
					myCnf = re.sub(searchRow, '\\1'+ dir + '\n' , myCnf)
				else:
					self._logger.error('Mysql directory \'%s\' doesn\'t exist. Creating new in \'%s\'', srcDir, dirname)
					myCnf = re.sub(searchRow, '' , myCnf)
					os.makedirs(dirname)
					myCnf += '\n' + directive +' = ' + dirname

			else:
				os.makedirs(dir)
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