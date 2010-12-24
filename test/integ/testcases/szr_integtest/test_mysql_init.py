'''
Created on Oct 2010

@author: spike
'''
from szr_integtest import config
from szr_integtest import MutableLogFile
from szr_integtest.roleinit import RoleHandler
from szr_integtest_libs import SshManager, exec_command
from szr_integtest_libs.szrdeploy import ScalarizrDeploy
from szr_integtest_libs.scalrctl import  EC2_MYSQL_ROLE_DEFAULT_SETTINGS, EC2_ROLE_DEFAULT_SETTINGS
import re
import unittest
import time
from boto.ec2.connection import EC2Connection




class MysqlTest(unittest.TestCase):
	def setUp(self):
		pass
	def tearDown(self):
		pass
	
	def _test_init(self, role_name, storage_test_fn):
		rh = MysqlRoleHandler('role name')
		rh.test_init()
		storage_test_fn()
		pass
	
	def test_with_raid_storage(self):
		pass
	

class MysqlRoleHandler(RoleHandler):
	slaves_ssh = []
	
	def test_slave_init(self):
		self._logger.info('>>>>>> Running slave instance <<<<<<')
		self.scalr_ctl.exec_cronjob('ScalarizrMessaging')
		
		out = self.scalr_ctl.exec_cronjob('Scaling')		

		result = re.search(self.server_id_re, out)
		if not result:
			raise Exception('Farm hasn\'t been scaled up')
		
		slave_server_id = result.group('server_id')
		self._logger.info("Slave server's id: %s" % slave_server_id)
		slave_ip = self.farm.get_public_ip(slave_server_id, 180)
		self._logger.info("Slave server's ip: %s" % slave_ip)
		
		slave_ssh = SshManager(slave_ip, self.farm_key, 180)
		self._logger.info('Sleeping for 30 sec while instance stands up')
		time.sleep(30)
		slave_ssh.connect()
		self.slaves_ssh.append(slave_ssh)
		
		# Temporary solution
		channel = slave_ssh.get_root_ssh_channel()
		#exec_command(channel, '/etc/init.d/scalarizr stop')
		#exec_command(channel, 'echo "" > /var/log/scalarizr.log')
		#deployer = ScalarizrDeploy(slave_ssh)
		#deployer.apply_changes_from_tarball()
		#del(deployer)
		#slave_ssh.close_all_channels()
		#channel = slave_ssh.get_root_ssh_channel()
		#exec_command(channel, '/etc/init.d/scalarizr start')	
		
		log = MutableLogFile(channel)
		reader = log.head()
		reader.expect("Message 'HostInit' delivered", 90)
		self.scalr_ctl.exec_cronjob('ScalarizrMessaging')
		
		sequence = ['HostInitResponse', 'Initializing MySQL slave', 'Creating EBS volume from snapshot',
			'farm-replication config created', 'Replication master is changed to host', "Message 'HostUp' delivered"]
		
		self.expect_sequence(reader, sequence)
		self._logger.info('>>>>> Mysql slave successfully initialized <<<<<')
		
	def test_add_pma_users(self):
		self._logger.info('>>>>> Starting MySQL add pma users test. <<<<<')
		channel = self.ssh.get_root_ssh_channel()
		log = MutableLogFile(channel)
		reader = log.tail()
		self.farm.create_pma_users()
		sequence = ['Adding phpMyAdmin system user', 'PhpMyAdmin system user successfully added']		
		self.expect_sequence(reader, sequence)
		self._logger.info('>>>>> PhpMyAdmin system users were added. <<<<<')
		
	def test_create_mysql_backup(self):
		self._logger.info('>>>>> Starting MySQL create backup test. <<<<<')
		slave_ssh = self.slaves_ssh[0]
		channel = slave_ssh.get_root_ssh_channel()
		log = MutableLogFile(channel)
		reader = log.tail()
		self.farm.create_mysql_backup()
		sequence = ['Dumping all databases', 'Uploading backup to S3', 'Backup files\(s\) uploaded to S3']
		self.expect_sequence(reader, sequence)
		self._logger.info('>>>>> Successfully created MySQL backup. <<<<<<')
			
	def test_promote_to_master(self):
		self._logger.info('>>>>> Starting MySQL promote to master test <<<<<')
		self._logger.info('Terminating MySQL master instance.')		
		try:
			ec2_key_id = config.get('./boto-ec2/ec2_key_id')
			ec2_key    = config.get('./boto-ec2/ec2_key')
		except:
			raise Exception('Configuration file doesn\'t contain ec2 credentials')
		ec2 = EC2Connection(ec2_key_id, ec2_key)
		ec2.terminate_instances([self.inst_id])				
		
		self._logger.info('Sleeping for 15 sec while instance sending HostDown message')
		time.sleep(15)
		
		first_slave_channel = self.slaves_ssh[0].get_root_ssh_channel()
		second_slave_channel = self.slaves_ssh[1].get_root_ssh_channel()
		first_slave_log = MutableLogFile(first_slave_channel)
		second_slave_log = MutableLogFile(second_slave_channel)
		first_slave_reader = first_slave_log.tail()
		second_slave_reader = second_slave_log.tail()
		
		### Detect 'promote to master' message receiver
		out = self.scalr_ctl.exec_cronjob('ScalarizrMessaging')
		new_master_ip_res = re.search("Mysql_PromoteToMaster\\\\' via REST to server \\\\'(?P<ip>[\d\.]+)", out)
		if not new_master_ip_res:
			raise Exception('Promote to master message hasn\'t been sent')
		new_master_ip = new_master_ip_res.group('ip')
		### Define sequences 
		promote_sequence = ['Unplug EBS storage \(volume:', 'Volume [\w-]+ detached', 'Taking master EBS volume',
				    'Taked master volume', 'Create EBS storage \(volume:', 'Attaching volume [\w-]+ as device',
				    'Volume [\w-]+ attached', 'Device [\/\w-]+ is available', 'Device [\/\w-]+ is mounted', 
				    'farm-replication config created', 'name="Mysql_PromoteToMasterResult".+<status>ok</status>',
				     "Message 'Mysql_PromoteToMasterResult' delivered"]
		new_master_sequence = ['Switching replication to a new MySQL master', 'Replication switched']
		### Expect 
		if self.slaves_ssh[0].ip == new_master_ip:
			self.expect_sequence(first_slave_reader, promote_sequence, 200)
			self.scalr_ctl.exec_cronjob('ScalarizrMessaging')
			self.expect_sequence(second_slave_reader, new_master_sequence, 200)
		else:
			self.expect_sequence(second_slave_reader, promote_sequence, 200)
			self.scalr_ctl.exec_cronjob('ScalarizrMessaging')
			self.expect_sequence(first_slave_reader, new_master_sequence, 200)
		self._logger.info('>>> Successfully promoted to master and switched to new master. <<<<<')
				
class TestMysqlInit(unittest.TestCase):

	def setUp(self):
		role_name = 'Test_mysql_2010_10_20_1348'
		opts = {}
		opts.update(EC2_MYSQL_ROLE_DEFAULT_SETTINGS)
		opts.update(EC2_ROLE_DEFAULT_SETTINGS)
		self.role_init = MysqlRoleHandler(role_name, opts)

	def test_init(self):
		sequence = ['HostInitResponse', 'Initializing MySQL master', 'Create EBS storage \(volume:',
					'farm-replication config created', 'MySQL system users added', 'Creating storage EBS snapshot',
					"Message 'HostUp' delivered"]
		self.role_init.test_init(sequence)
		self.role_init.test_slave_init()
		self.role_init.test_add_pma_users()
		self.role_init.test_create_mysql_backup()
		self.role_init.test_slave_init()
		self.role_init.test_promote_to_master()
	
	
	def tearDown(self):
		if hasattr(self.role_init, 'ssh'):
			self.role_init.ssh.close_all_channels()
		for slave_ssh in self.role_init.slaves_ssh:
			slave_ssh.close_all_channels()
	

if __name__ == "__main__":
	unittest.main()

"""
#from scalarizr.util import system
role_name = 'mysql-058-u1004'
farm_id = config.get('./test-farm/farm_id')
farm_key = config.get('./test-farm/farm_key')

server_id_re = re.compile('\[FarmID:\s+%s\].*?%s\s+scaling\s+\up.*?ServerID\s+=\s+(?P<server_id>[\w-]+)' % (farm_id, role_name), re.M)

farm = FarmUI(get_selenium())
print "Launching farm"
farm.use(farm_id)
farm.launch()

print "Farm launched"
out = exec_cronjob('Scaling')
#out = system('php -q /home/spike/workspace/scalr/scalr.net-trunk/app/cron-ng/cron.php --Scaling')[0]

result = re.search(server_id_re, out)
if not result:
	raise Exception('Farm hasn\'t been scaled up')

server_id = result.group('server_id')
print "New server id: %s" % server_id
ip = farm.get_public_ip(server_id)
print "New server's ip: %s" % ip

ssh = SshManager(ip, farm_key)
ssh.connect()
channel = ssh.get_root_ssh_channel()
tail_log_channel(channel)

sequence = ['HostInitResponse', 'Initializing MySQL master', "Message 'HostUp' delivered"]
exec_cronjob('ScalarizrMessaging')
#system('php -q /home/spike/workspace/scalr/scalr.net-trunk/app/cron-ng/cron.php --ScalarizrMessaging')

for regexp in sequence:
	expect(channel, regexp, 60)
	print "regexp OK"
"""