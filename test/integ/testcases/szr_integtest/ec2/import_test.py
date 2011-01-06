'''
Created on Dec 24, 2010

@author: spike
'''
from szr_integtest 		import get_selenium

from szr_integtest_libs.ssh_tool import execute
from szr_integtest_libs.datapvd	import DataProvider
from szr_integtest_libs.scalrctl	import ui_import_server, ScalrConsts, ScalrCtl
from szr_integtest_libs.szrdeploy	import ScalarizrDeploy

import logging
import os
import re
import time
import unittest

class ImportEc2Test(unittest.TestCase):
	tags = ['ec2', 'import']
	
	def __init__(self, methodName='runTest'):
		super(ImportEc2Test, self).__init__(methodName)
		self._logger = logging.getLogger(__name__)
		
	def _get_dp(self, root_device_type):
		return DataProvider(behaviour='raw', root_device_type=root_device_type)
	
	def _test_instance_store(self):
		dp = self._get_dp('instance-store')
		server = _init_server(dp, self._logger)
		reader = server.log.head()
		reader.expect( "Message 'Hello' delivered",				 					240)
		self._logger.info("Message 'Hello' delivered")
		
		scalrctl = ScalrCtl()
		scalrctl.exec_cronjob('ScalarizrMessaging')
		reader.expect( 'Bundling image...',								    	    240)
		reader.expect( 'Encrypting image',											240)
		reader.expect( 'Splitting image into chunks',								240)
		reader.expect( 'Encrypting keys',											240)
		reader.expect( 'Image bundle complete!',									240)
		
		self._logger.info("Image bundled!")
		
		reader.expect( 'Uploading bundle',											240)
		reader.expect( 'Enqueue files to upload',									240)
		reader.expect( 'Uploading files',											240)
		reader.expect( 'Registration complete!',									240)
		self.ami = reader.expect("Image (?P<ami_id>ami-\w+) available", 			360).group('ami_id')
		reader.expect( "Rebundle complete!", 										240)
		self._logger.info("Rebundle complete!")	
		scalrctl.exec_cronjob('ScalarizrMessaging')
		scalrctl.exec_cronjob('BundleTasksManager')
		
	def _test_ebs(self):
		dp = self._get_dp('ebs')
		server = _init_server(dp, self._logger)
		reader = server.log.head()
		reader.expect( "Message 'Hello' delivered",				 					240)
		self._logger.info("Message 'Hello' delivered")
		
		scalrctl = ScalrCtl()
		scalrctl.exec_cronjob('ScalarizrMessaging')		
				
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
			
		reader.expect( "Rebundle complete!", 										240)
		self._logger.info("Rebundle complete!")

		scalrctl.exec_cronjob('ScalarizrMessaging')
		scalrctl.exec_cronjob('BundleTasksManager')

def _init_server(dp, logger):
	# Init import with params

	logger.info('Starting new server.')
	server = dp.server()
	
	deployer = ScalarizrDeploy(server.ssh_manager)
	logger.debug('Adding repository on the instance.')
	try:
		repo_name = os.environ['SZR_REPO_TYPE']
		deployer.add_repos(repo_name)
	except:
		deployer.add_repos('stable')
		
	logger.info('Installing "scalarizr" package.')
	deployer.install_package()
	
	channel = server.ssh()
	# Enable debug logging
	execute(channel, 'cp -pr /etc/scalr/logging-debug.ini /etc/scalr/logging.ini')
	_install_software(channel, deployer.dist)
	
	try:
		platform = getattr(ScalrConsts.Platforms, 'PLATFORM_' + dp.platform.upper())
	except:
		raise Exception('Unknown platform: %s' % dp.platform)
	
	role_name = 'Import-test-%s' % time.strftime('%Y-%m-%d-%H-%M')
	logger.info("Importing instance as '%s' in scalr's interface"  % role_name)
	import_string = ui_import_server(get_selenium(), platform, ScalrConsts.Behaviours.BEHAVIOUR_BASE, server.public_ip, role_name)
	logger.info('Running import string on the instance.')
	execute(channel, 'screen -md %s' % import_string)
							
	return server
	
def _install_software(channel, dist):
	if dist == 'debian':
		out = execute(channel, 'apt-get -y install screen', 240)
		error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
		if error:
			raise Exception("Can't install screen package: '%s'" % error.group('err_text'))		
	else:
		out = execute(channel, 'yum -y install screen', 240)
		if not re.search('Complete!|Nothing to do', out):
			raise Exception('Cannot install screen %s' % out)
		execute(channel, 'chmod 777 /var/run/screen')
		
if __name__ == "__main__":
	unittest.main()