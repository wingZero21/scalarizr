'''
Created on Oct 2, 2010

@author: spike
'''
import unittest

from szr_integtest_libs.scalrctl import FarmUI, FarmUIError, import_server, ScalrConsts
from szr_integtest import get_selenium
from szr_integtest import config
import socket
from boto.ec2.connection import EC2Connection
from scalarizr.libs.metaconf import NoPathError
import time

from szr_integtest_libs import SshManager, exec_command
from szr_integtest_libs.szrdeploy import ScalarizrDeploy

UBUNTU_AMI = 'ami-714ba518'
SECURITY_GROUP = 'webta.scalarizr'

class TestImportEc2Server(unittest.TestCase):

	def test_import(self):
		try:
			ec2_key_id = config.get('./boto-ec2/ec2_key_id')
			ec2_key    = config.get('./boto-ec2/ec2_key')
			key_name   = config.get('./boto-ec2/key_name')
			key_path   = config.get('./boto-ec2/key_path')
			farm_id    = config.get('./test-farm/key_path')
		except NoPathError:
			raise Exception("Configuration file doesn't contain ec2 credentials")
		
		ec2 = EC2Connection(ec2_key_id, ec2_key)
		
		reservation = ec2.run_instances(UBUNTU_AMI, security_groups = [SECURITY_GROUP], instance_type='m1.small', placement = 'us-east-1a', key_name = key_name)
		instance = reservation.instances[0]
		while not instance.state == 'running':
			instance.update()
			time.sleep(10)
			
		ip_address = socket.gethostbyname(instance.public_dns_name)
		
		sshmanager = SshManager(ip_address, key_path)
		sshmanager.connect()
		
		deployer = ScalarizrDeploy(sshmanager)
		
		# TODO: add nightly-build support
		deployer.add_repos('local')
		deployer.install_package()
		deployer.apply_changes_from_tarball()
		
		role_name = 'Integ_test_base_%s' % time.strftime('%Y_%m_%d_%H%M')
		import_server_str = import_server(get_selenium(), ScalrConsts.Platforms.PLATFORM_EC2 ,\
										ScalrConsts.Behaviours.BEHAVIOUR_BASE , ip_address, role_name)
		
		channel = sshmanager.get_root_ssh_channel()
		
		#exec_command(channel,)
		# TODO: run <import_server_str> on instance, read log while bundle not complete, return ami id . 
		# Don't forget to run crons!
			
if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test_ ']
	unittest.main()