'''
Created on Oct 16, 2010

@author: marat
'''

from szr_integtest import  config
from szr_integtest_libs import  SshManager
import unittest
import szr_integtest
import pexpect
from szr_integtest_libs import MutableLogFile


roles = {
	'test-base-euca-2' : ('x86_64', 'eucalyptus', 'centos')
}

server_id = None
server_ip = None
ssh = None
root_ssh_channel = None



'''
class TestLaunch(unittest.TestCase):
	role_name = None
	role_opts = None
	terminate = False
	
	_logger = None
	
	def __init__(self, methodName='runTest', **kwargs):
		unittest.TestCase.__init__(self, methodName)
		for k, v in kwargs:
			setattr(self, k, v)
	
	def setUp(self):
		self._logger = logging.getLogger(__name__)		
		self.scalr_ctl = ScalrCtl()
		self.farm = scalrctl.FarmUI(get_selenium())
	
	def test_it(self):
		global server_id, server_ip, ssh, root_ssh_channel
		
		
		# Launch farm
		self._logger.info('Launching farm')
		self.farm.use(self.farm_id)
		self.farm.remove_all_roles()
		self.farm.add_role(self.role_name, 1, 2, self.role_opts or {})
		self.farm.save()
		self.farm.launch()
		self._logger.info("Farm launched")
		
		# Scale farm
		out = self.scalr_ctl.exec_cronjob('Scaling')
		server_id = self.scalrctl.parse_scaled_up_server_id(out)
		self._logger.info("New server id: %s" % server_id)
		
		# Wait when cloud launches server
		server_ip = self.farm.get_public_ip(server_id)
		self._logger.info("New server's ip: %s" % server_ip)
		
		# Wait for ssh connection
		ssh = SshManager(self.ip, self.farm_key)
		ssh.connect()
		self._logger.info('Connected to instance')
		
		root_ssh_channel = ssh.get_root_ssh_channel()		
		
		# Temporary solution
#		self._logger.info("Deploying dev branch")
#		deployer = ScalarizrDeploy(self.ssh)
#		deployer.apply_changes_from_tarball()
#		del(deployer)		
#		self.ssh.close_all_channels()
#		

#
#		exec_command(channel, '/etc/init.d/scalarizr stop')
#		exec_command(channel, 'rm -f /etc/scalr/private.d/.state')
#		exec_command(channel, '/etc/init.d/scalarizr start')
#		time.sleep(2)
		
		
		
		
		tail_log_channel(channel)

	
		self.expect_sequence(channel, sequence)
			
		self._logger.info('>>> Role has been successfully initialized.')		
		pass
	
	def tearDown(self):
		if self.terminate:
			# TODO: terminate farm
			pass

'''

log_file = None


	
#def attach_log(head=False, tail=False, lines=0):
#	fifo = log_file.head(lines) if head else log_file.tail(lines)
#	return pexpect.spawn('cat', [fifo])
#
#def detach_log(exp):
#	exp.close(force=True)
#	log_file.detach(exp.args[0])
	

class TestHostInit(unittest.TestCase):
	
	timeout_start_main = 120
	timeout_start_snmp = 60
	timeout_host_init = 60
	
	_log = None
	
	def setUp(self):
		ssh = SshManager('ec2-174-129-177-52.compute-1.amazonaws.com', config.get('test-farm/farm_key'))
		ssh.connect()	
		self.log_file = MutableLogFile(ssh.get_root_ssh_channel())
		self._log = log_file.head()
		#self._log = attach_log(head=True)
	
	def tearDown(self):
		self.log_file.detach(self._log)
		
	def test(self):
		self._log.expect(r'\[pid: \d+\] Starting scalarizr', self.timeout_start_main)
		self._log.expect(r'Build message consumer server', 15)
		self._log.expect(r'\[pid: \d+\] Starting scalarizr', self.timeout_start_snmp)
		self._log.expect("Message 'HostInit' delivered", self.timeout_host_init)


'''
class TestHostUp(unittest.TestCase):
	_log_channel = None
	_scalr_ctl = None
	
	def setUp(self):
		self._scalr_ctl = ScalrCtl()
		self._log_channel = tail_log_channel(ssh.get_root_ssh_channel())
	
	def test(self):
		self._scalr_ctl.exec_cronjob('ScalarizrMessaging')

		pass
'''

class TestReboot(unittest.TestCase):
	pass

class TestHostDown(unittest.TestCase):
	pass

class TestExecuteScript(unittest.TestCase):
	pass

class TestIpListBuilder(unittest.TestCase):
	pass

def suite():
	global log_file
	
	suite = unittest.TestSuite()
	suite.addTest(TestHostInit())
	return suite

if __name__ == '__main__':
	szr_integtest.main()
	

	ssh = SshManager('ec2-174-129-177-52.compute-1.amazonaws.com', config.get('test-farm/farm_key'))
	ssh.connect()	
	log_file = MutableLogFile(ssh.get_root_ssh_channel())
	
		
	'''
	alltests = unittest.TestSuite((
		TestLaunch(role_name='test-base-euca-2'),
		TestHostInit(),
		TestHostUp(),
		TestIpListBuilder(),
		TestExecuteScript(),
		TestReboot(),		
		#TestHostDown()
	))
	'''
	unittest.main()
