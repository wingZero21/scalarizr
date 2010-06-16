'''
Created on 14.06.2010

@author: spike
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler
from scalarizr.util import fstool, system
import logging, os

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
		role_name = None
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
#						while ebsVolume.attachment_state() != 'attached':
				else:
					self._logger.error('Can\'t find volume with ID =  %s ', volId)
					raise
							
				#TODO: check if the volume has been successfully attached 
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
				# Stop
				init_script		= '/etc/init.d/mysql'
				reload_command	= [init_script, 'stop']
				if os.path.exists(init_script) and os.access(init_script, os.X_OK):
					self._logger.info("Trying to stop mysql server..")
					try:
						out, err, retcode = system(reload_command, shell=False)
						if retcode or (out and out.find("FAILED") != -1):
							self._logger.error("Mysql stopping failed. %s", out)
						else:
							self._logger.info("Mysql was successfully stopped")
					except OSError, e:
						self._logger.error('Mysql stopping failed by running %s. %s',
						''.join(reload_command), e.strerror)

				try:
					file = open('/etc/mysql/my.cnf', 'r')
				except IOError, e:
					self._logger.error('Can\'t open /etc/mysql/my.cnf: %s', e.strerror )
					raise
				else:
					myCnf = file.readlines()
					file.close
					
								
				# Move datadir to EBS 
				# /mnt/mysql-data
				# Start --skip-grant-tables
				# Add root user
				# Save root user to /etc/scalr/private.d/behaviour.mysql.ini
				pass
		
			if "master":
				message.mysql_repl_user = ""
				message.mysql_repl_password = ""
				mtab = fstool.Mtab()
				fstab = fstool.Fstab()	
	
	def _mount_device(self, devname):
		fstab = fstool.Fstab()			
		if None != devname:
			while True:
				try:
					fstool.mount(devname, '/mnt', ["-t auto"])
					break
				except fstool.FstoolError, e:
					if -666 == e.code:
						system("/sbin/mkfs.ext3 -F " + devname + " 2>&1")
					else:
						self._logger.error('Can\'t mount device %s : %s', devname, e)
						raise
					
		if not fstab.contains(devname, rescan=True):
			self._logger.info("Adding a record to fstab")
			fstab.append(fstool.TabEntry(devname, '/mnt', "auto", "defaults\t0\t0"))
	