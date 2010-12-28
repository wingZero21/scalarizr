'''
Created on Dec 24, 2010

@author: spike
'''

import unittest
import time
import re
from szr_integtest_libs.providers import DataProvider 
from szr_integtest_libs.scalrctl import import_server, ScalrConsts
from szr_integtest_libs import exec_command
from szr_integtest import get_selenium
from szr_integtest_libs.szrdeploy import ScalarizrDeploy
import logging

class ImportEc2Test(unittest.TestCase):
	tags = ['ec2', 'import']
	
	def __init__(self, methodName='runTest'):
		super(ImportEc2Test, self).__init__(methodName)
		self._logger = logging.getLogger(__name__)
		
	def _init_server(self, root_device_type):
		# Init import with params
		dp = DataProvider(behaviour='raw', root_device_type=root_device_type)
		server = dp.server()
		
		deployer = ScalarizrDeploy(server.ssh_manager)
		deployer.add_repos('release')
		deployer.install_package()
		
		channel = server.ssh()
		self._install_software(channel, deployer.distr)
				
		try:
			platform = getattr(ScalrConsts.Platforms, 'PLATFORM_' + dp.platform.upper())
		except:
			raise Exception('Unknown platform: %s' % dp.platform)
		 
		role_name = 'Import-test-%s' % time.strftime('%Y-%m-%d-%H-%M')
		import_string = import_server(get_selenium(), platform, ScalrConsts.Behaviours.BEHAVIOUR_BASE, server.public_ip, role_name)
		
		exec_command(channel, 'screen -md %s' % import_string)
		
		return server
		
	def _install_software(self, channel, distr):
		if distr == 'debian':
			out = exec_command(channel, 'apt-get -y install screen', 240)
			error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
			if error:
				raise Exception("Can't install screen package: '%s'" % error.group('err_text'))		

		else:
			out = exec_command(channel, 'yum -y install screen', 240)
			if not re.search('Complete!|Nothing to do', out):
				raise Exception('Cannot install screen %s' % out)
			exec_command(channel, 'chmod 777 /var/run/screen')

	def _test_instance_store(self):
		server = self._init_server('instance_store')
		reader = server.log
		pass
	
	def test_ebs(self):
		print 'ololo'
		server = self._init_server('ebs')
		reader = server.log
		
		reader.expect( "Creating snapshot of root device image", 					240)
		self._logger.info("Creating snapshot of root device image")
		
		reader.expect( "Checking that snapshot (?P<snap_id>snap-\w+) is completed",240)
		self._logger.info("Checking that snapshot is completed")
		
		reader.expect( "Snapshot snap-\w+ completed", 								420)
		self._logger.info("Snapshot completed")
		
		reader.expect( "Registering image", 										120)
	
		self.ami = reader.expect("Checking that (?P<ami_id>ami-\w+) is available",  120).group('ami_id')

		self._logger.info("Checking for %s completed", self.ami)
		reader.expect( "Image (?P<ami>ami-\w+) available", 						    360)
	
		self._logger.info("Ami created: %s", self.ami)
	
		reader.expect( "Image registered and available for use", 					240)
			
		reader.expect( "Rebundle complete!", 											240)
		self._logger.info("Rebundle complete!")

		self.scalr_ctl.exec_cronjob('ScalarizrMessaging')
		self.scalr_ctl.exec_cronjob('BundleTasksManager')

			
if __name__ == "__main__":
	unittest.main()
