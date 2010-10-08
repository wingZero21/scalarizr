'''
Created on Oct 2010

@author: spike
'''
from szr_integtest import get_selenium, config
from szr_integtest_libs import tail_log_channel, expect, SshManager
from szr_integtest_libs.scalrctl import FarmUI, exec_cronjob
import logging
import re
import unittest

class RoleInit:
	
	def __init__(self, role_name, sequence):
		self._logger = logging.getLogger(__name__)
		self.role_name = role_name
		self.sequence = sequence
		self.farm_id = config.get('./test-farm/farm_id')
		self.farm_key = config.get('./test-farm/farm_key')
		self.key_password   = config.get('./boto-ec2/ssh_key_password')
		self.server_id_re = re.compile(
				'\[FarmID:\s+%s\].*?%s\s+scaling\s+\up.*?ServerID\s+=\s+(?P<server_id>[\w-]+)' \
				% (self.farm_id, self.role_name), re.M)
	
	def test_init(self):
		farm = FarmUI(get_selenium())
		self._logger.debug("Launching farm")
		farm.use(self.farm_id)
		farm.launch()
		
		self._logger.debug("Farm launched")
		out = exec_cronjob('Scaling')
		

		result = re.search(self.server_id_re, out)
		if not result:
			raise Exception('Farm hasn\'t been scaled up')
		
		server_id = result.group('server_id')
		self._logger.debug("New server id: %s" % server_id)
		ip = farm.get_public_ip(server_id)
		self._logger.debug("New server's ip: %s" % ip)
		
		ssh = SshManager(ip, self.farm_key, self.key_password)
		ssh.connect()
		channel = ssh.get_root_ssh_channel()
		tail_log_channel(channel)
		
		exec_cronjob('ScalarizrMessaging')
	
		for regexp in self.sequence:
			expect(channel, regexp, 60)
			self._logger.debug("regexp OK")
			
			
class TestMysqlInit(unittest.TestCase):
	
	def setUp(self):
		role_name = 'mysql-058-u1004'
		sequence = ['HostInitResponse', 'Initializing MySQL master', "Message 'HostUp' delivered"]
		self.test_role = RoleInit(role_name, sequence)
	
	def test_init(self):
		self.test_role.test_init()
	
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