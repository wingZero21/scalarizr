'''
Created on Oct 2010

@author: spike
'''
from szr_integtest import get_selenium, config
from szr_integtest_libs import tail_log_channel, expect, SshManager
from szr_integtest_libs.scalrctl import FarmUI, exec_cronjob, EC2_MYSQL_ROLE_DEFAULT_SETTINGS, EC2_ROLE_DEFAULT_SETTINGS
import logging
import re
import unittest

class RoleHandler:
	
	def __init__(self, role_name, role_opts):
		self.role_opts = role_opts
		self._logger = logging.getLogger(__name__)
		self.role_name = role_name
		self.farm_id = config.get('./test-farm/farm_id')
		self.farm_key = config.get('./test-farm/farm_key')
		self.server_id_re = re.compile(
				'\[FarmID:\s+%s\].*?%s\s+scaling\s+\up.*?ServerID\s+=\s+(?P<server_id>[\w-]+)' \
				% (self.farm_id, self.role_name), re.M)
	
	def test_init(self, sequence):
		self.farm = FarmUI(get_selenium())
		self._logger.info("Launching farm")
		self.farm.use(self.farm_id)
		self.farm.remove_all_roles()
		self.farm.add_role(self.role_name, 1, 2, self.role_opts)
		self.farm.save()
		self.farm.launch()
		
		self._logger.info("Farm launched")
		out = exec_cronjob('Scaling')

		result = re.search(self.server_id_re, out)
		if not result:
			raise Exception('Farm hasn\'t been scaled up')
		
		self.server_id = result.group('server_id')
		self._logger.info("New server id: %s" % self.server_id)
		self.ip = self.farm.get_public_ip(self.server_id)
		self._logger.info("New server's ip: %s" % self.ip)
		
		self.ssh = SshManager(self.ip, self.farm_key)
		self.ssh.connect()
		channel = self.ssh.get_root_ssh_channel()
		tail_log_channel(channel)
		
		exec_cronjob('ScalarizrMessaging')
	
		for regexp in sequence:
			expect(channel, regexp, 60)
			self._logger.info("%s appeared in scalarizr.log", regexp)
			
		self._logger.info('Role has been successfully initialized')

		
		
class MysqlRoleHandler(RoleHandler):
	
	def test_slave_init(self):
		self._logger.info('Running slave instance')
		exec_cronjob('ScalarizrMessaging')
		
		out = exec_cronjob('Scaling')		

		result = re.search(self.server_id_re, out)
		if not result:
			raise Exception('Farm hasn\'t been scaled up')
		
		self.slave_server_id = result.group('server_id')
		self._logger.info("Slave server's id: %s" % self.slave_server_id)
		self.slave_ip = self.farm.get_public_ip(self.slave_server_id)
		self._logger.info("Slave server's ip: %s" % self.slave_ip)
	
		self.slave_ssh = SshManager(self.ip, self.farm_key)
		self.slave_ssh.connect()
		channel = self.slave_ssh.get_root_ssh_channel()
		tail_log_channel(channel)
		
		exec_cronjob('ScalarizrMessaging')
		
		sequence = ['HostInitResponse', 'Initializing MySQL slave', 'Creating EBS volume from snapshot',
			'farm-replication config created', 'Replication master is changed to host', "Message 'HostUp' delivered"]
		
		for regexp in sequence:
			expect(channel, regexp, 60)
			self._logger.info("%s appeared in scalarizr.log", regexp)

		
				
class TestMysqlInit(unittest.TestCase):

	def setUp(self):
		role_name = 'Test_mysql_2010_10_12_1324'
		role_opts = EC2_MYSQL_ROLE_DEFAULT_SETTINGS
		role_opts.update(EC2_ROLE_DEFAULT_SETTINGS)
		self.role_init = MysqlRoleHandler(role_name, role_opts)

	def test_init(self):
		sequence = ['HostInitResponse', 'Initializing MySQL master', 'Create EBS storage (volume:',
					'farm-replication config created', 'MySQL system users added', 'Creating storage EBS snapshot',
					"Message 'HostUp' delivered"]
		self.role_init.test_init(sequence)
		self.role_init.test_slave_init()
		
	
	def tearDown(self):
		pass
	

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