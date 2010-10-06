'''
Created on Oct 2, 2010

@author: spike
'''
import unittest

from szr_integtest_libs.scalrctl import FarmUI, FarmUIError, import_server, ScalrConsts, exec_cronjob
from szr_integtest import get_selenium, Ec2TestAmis, config
import socket
from boto.ec2.connection import EC2Connection
from scalarizr.libs.metaconf import NoPathError
import time
from szr_integtest_libs import SshManager, exec_command, tail_log_channel, expect
from szr_integtest_libs.szrdeploy import ScalarizrDeploy
from scalarizr.util import init_tests
import logging
from optparse import OptionParser

UBUNTU_AMI = 'ami-714ba518'
SECURITY_GROUP = 'webta.scalarizr'

parser = OptionParser()
parser.add_option('-d', '--delete-role', dest='del_role', action='store_true', help='Delete role and ami')
parser.add_option('-m', '--ami', dest='ami', default=Ec2TestAmis.UBUNTU_1004_EBS, help='Amazon AMI')
parser.parse_args()
vals = parser.values

class TestImportEc2Server(unittest.TestCase):

	def _install_software(self, channel, distr):
		pass
	
	def test_import(self):
		logger = logging.getLogger(__name__)
		try:
			ec2_key_id = config.get('./boto-ec2/ec2_key_id')
			ec2_key    = config.get('./boto-ec2/ec2_key')
			key_name   = config.get('./boto-ec2/key_name')
			key_path   = config.get('./boto-ec2/key_path')
		except NoPathError:
			raise Exception("Configuration file doesn't contain ec2 credentials")
		
		ec2 = EC2Connection(ec2_key_id, ec2_key)
		
		reservation = ec2.run_instances(UBUNTU_AMI, security_groups = [SECURITY_GROUP], instance_type='m1.small', placement = 'us-east-1a', key_name = key_name)
		instance = reservation.instances[0]
		logger.info('Started instance %s', instance.id)
		while not instance.state == 'running':
			instance.update()
			time.sleep(10)
		logger.info("Instance's %s state is 'running'" , instance.id)
		ip_address = socket.gethostbyname(instance.infopublic_dns_name)
		
		sshmanager = SshManager(ip_address, key_path)
		sshmanager.connect()
		
		deployer = ScalarizrDeploy(sshmanager)
		distr = deployer.distr
		
		# TODO: add nightly-build support
		logger.info("Adding repository")
		deployer.add_repos('release')
		logger.info("Installing package")
		deployer.install_package()
		logger.info("Apply changes from dev branch (tarball)")
		deployer.apply_changes_from_tarball()
		
		role_name = 'Test_base_%s' % time.strftime('%Y_%m_%d_%H%M')
		logger.info("Role name: %s", role_name)
		logger.info("Importing server in scalr's interface")
		import_server_str = import_server(get_selenium(), ScalrConsts.Platforms.PLATFORM_EC2 ,\
										ScalrConsts.Behaviours.BEHAVIOUR_BASE , ip_address, role_name)
		
		import_server_str += ' &'
		channel = sshmanager.get_root_ssh_channel()
		logger.info("Hacking configuration files")
		exec_command(channel, 'mv /etc/scalr/logging-debug.ini /etc/scalr/logging.ini')
		exec_command(channel, "sed -i 's/consumer_url = http:\/\/localhost/consumer_url = http:\/\/0.0.0.0/g' /etc/scalr/public.d/config.ini")

		self._install_software(channel, distr)

		exec_command(channel, import_server_str)
		tail_log_channel(channel)
		expect(channel, "Message 'Hello' delivered", 15)
		logger.info("Hello delivered")
		
		exec_cronjob('ScalarizrMessaging')
		
		expect(channel, "Make EBS volume /dev/sd.+ from volume /", 240)
		expect(channel, "Volume bundle complete!", 240)
		logger.info("Volume with / bundled")
		ami_result = expect(channel, "Image (?P<ami>ami-\w+) available", 240)
		ami = ami_result.group('ami')
		logger.info("Ami created: %s", ami)
		expect(channel, "Image registered and available for use", 240)
		expect(channel, "Rebundle complete!", 240)
		logger.info("Rebundle complete!")
		
		exec_cronjob('ScalarizrMessaging')
		exec_cronjob('BundleTasksManager')
		exec_cronjob('BundleTasksManager')

		
		#exec_command(channel,)
		# TODO: run <import_server_str> on instance, read log while bundle not complete, return ami id . 
		# Don't forget to run crons!
			
if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_ ']
	init_tests()
	unittest.main()