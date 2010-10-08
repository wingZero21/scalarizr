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
import logging
from optparse import OptionParser
import sys

SECURITY_GROUP = 'webta.scalarizr'

class ImportEc2Server:
	ami        = None
	ip_address = None
	ec2 	   = None
	instance   = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self.passed = True
		self.sys_args = sysargs

	def _install_software(self, channel, distr):
		pass
	
	def _import_server(self, role_name):
		return import_server(get_selenium(), ScalrConsts.Platforms.PLATFORM_EC2 ,\
			ScalrConsts.Behaviours.BEHAVIOUR_BASE , self.ip_address, role_name)
		
	def _get_role_name(self):
		return 'Test_base_%s' % time.strftime('%Y_%m_%d_%H%M')
	
	def cleanup(self):
		if not self.sys_args.no_cleanup:
			if self.instance:

				self._logger.debug('Terminating instance %s ' % str(self.instance.id))
				self.ec2.terminate_instances([str(self.instance.id)])
			
			if self.ami and self.ec2:
				image = self.ec2.get_image(self.ami)
				snap_id = image.block_device_mapping['/dev/sda1'].snapshot_id
				self.ec2.deregister_image(self.ami)
				self.ec2.delete_snapshot(snap_id)
				#TODO: Clean scalr's database 

	def test_import(self):
		
		try:
			ec2_key_id = config.get('./boto-ec2/ec2_key_id')
			ec2_key    = config.get('./boto-ec2/ec2_key')
			key_name   = config.get('./boto-ec2/key_name')
			key_path   = config.get('./boto-ec2/key_path')
		except NoPathError:
			raise Exception("Configuration file doesn't contain ec2 credentials")
		
		self.ec2 = EC2Connection(ec2_key_id, ec2_key)

		if not self.sys_args.inst_id:
			reservation = self.ec2.run_instances(self.sys_args.ami, security_groups = [SECURITY_GROUP], instance_type='m1.small', placement = 'us-east-1a', key_name = key_name)
			self.instance = reservation.instances[0]
			self._logger.info('Started instance %s', self.instance.id)
			while not self.instance.state == 'running':
				self.instance.update()
				time.sleep(5)
			self._logger.info("Instance's %s state is 'running'" , self.instance.id)
		else:
			try:
				reservation = self.ec2.get_all_instances(self.sys_args.inst_id)[0]
			except:
				raise Exception('Instance %s does not exist' % self.sys_args.inst_id)
			
			self.instance = reservation.instances[0]
			
		self.root_device = self.instance.rootDeviceType

		self.ip_address = socket.gethostbyname(self.instance.public_dns_name)
		
		sshmanager = SshManager(self.ip_address, key_path)
		sshmanager.connect()

		deployer = ScalarizrDeploy(sshmanager)
		distr = deployer.distr
		
		# TODO: add nightly-build support
		self._logger.info("Adding repository")
		deployer.add_repos('release')

		self._logger.info("Installing package")

		deployer.install_package()

		self._logger.info("Apply changes from dev branch (tarball)")

		deployer.apply_changes_from_tarball()
		
		role_name = self._get_role_name()
		self._logger.info("Role name: %s", role_name)
		self._logger.info("Importing server in scalr's interface")	#import sys;sys.argv = ['', 'Test.test_ ']
		import_server_str = self._import_server(role_name)
		
		import_server_str += ' &'
		channel = sshmanager.get_root_ssh_channel()
		self._logger.info("Hacking configuration files")
		exec_command(channel, 'mv /etc/scalr/logging-debug.ini /etc/scalr/logging.ini')
		exec_command(channel, "sed -i 's/consumer_url = http:\/\/localhost/consumer_url = http:\/\/0.0.0.0/g' /etc/scalr/public.d/config.ini")

		self._install_software(channel, distr)

		exec_command(channel, import_server_str)
		tail_log_channel(channel)
		# RegExp   																		# Timeout
		
		expect(channel, "Message 'Hello' delivered", 									15)
		
		self._logger.info("Hello delivered")
		exec_cronjob('ScalarizrMessaging')
		
		if self.root_device == 'instance-store':
			out = expect(channel, "Make image .+ from volume .+",			 			240)
			self._logger.info(out)
		else:
			expect(channel, "Make EBS volume /dev/sd.+ from volume /", 					240)
			
		expect(channel, "Volume bundle complete!", 										240)
		
		self._logger.info("Volume with / bundled")
		
		expect(channel, "Creating snapshot of root device image", 						240)
		expect(channel, "Checking that snapshot (?P<snap_id>snap-\w+) is completed",	240)
		expect(channel, "Snapshot snap-\w+ completed", 									240)
		
		self._logger.info("Snapshot completed")
		
		expect(channel, "Registering image", 											120)
		self.ami = expect(
			   channel, "Checking that (?P<ami_id>ami-\w+) is available", 				120).group('ami_id')

		self._logger.info("Checking for %s completed", self.ami)
		
		expect(channel, "Image (?P<ami>ami-\w+) available", 							360)
		
		self._logger.info("Ami created: %s", self.ami)
		
		expect(channel, "Image registered and available for use", 						240)
		expect(channel, "Rebundle complete!", 											240)
		
		self._logger.info("Rebundle complete!")
		
		exec_cronjob('ScalarizrMessaging')
		exec_cronjob('BundleTasksManager')
		exec_cronjob('BundleTasksManager')
		
		#exec_command(channel,)
		# TODO: run <import_server_str> on instance, read log while bundle not complete, return ami id . 
		# Don't forget to run crons!


def _parse_args():
	parser = OptionParser()
	parser.add_option('-c', '--no-cleanup', dest='no_cleanup', action='store_true', default=False, help='Do not cleanup test data')
	parser.add_option('-m', '--ami', dest='ami', default=Ec2TestAmis.UBUNTU_1004_EBS, help='Amazon AMI')
	parser.add_option('-i', '--instance-id', dest='inst_id', default=None, help='Running instance')
	parser.parse_args()
	return parser.values


class TestImportEc2Server(unittest.TestCase):
	
	importer = None
	
	def setUp(self):
		self.importer = ImportEc2Server()

	def test_import(self):
		self.importer.test_import()

	def tearDown(self):
		self.importer.cleanup()
	
			
if __name__ == "__main__":
	sysargs = _parse_args()
	del sys.argv[1:]
	unittest.main()
	