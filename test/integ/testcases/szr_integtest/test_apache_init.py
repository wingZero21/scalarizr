'''
Created on Oct 06, 2010

@author: shaitanich
'''
import unittest

from szr_integtest import get_selenium, config, Ec2TestAmis
from szr_integtest_libs import expect, SshManager, exec_command
from szr_integtest_libs.scalrctl import FarmUI, import_server, ScalrConsts
from test_import_ec2_server import ImportEc2Server
import logging
import re 

class ImportAppServer(ImportEc2Server):

	def _install_software(self, channel, distr):
		logger = logging.getLogger(__name__)
		#rhel , fedora, ubuntu
		ubuntu = distr is 'ubuntu'
		prefix, software  = ('apt-get -y ',['apache2']) if ubuntu else ('yum -y install ', ['httpd', 'mod_ssl'])
		install_cmd = prefix + ' '.join(software)
		out = exec_command(channel, install_cmd)
		
		if ubuntu:
			error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
			if error:
				raise Exception("Can't install %s: '%s'" % (software, error.group('err_text')))	
		
		else:
			if not re.search('Complete!|Nothing to do', out):
				raise Exception('Cannot install scalarizr')	
			
	
	def _import_server(self, role_name):
		return import_server(get_selenium(), ScalrConsts.Platforms.PLATFORM_EC2 ,\
			ScalrConsts.Behaviours.BEHAVIOUR_APP, self.ip_address, role_name)	

class TestImportAppServer(unittest.TestCase):
	
	def setUp(self):
		self.importer = ImportAppServer()
		self.importer.test_import()
		
	def tearDown(self):
		self.importer.cleanup()
	

if __name__ == "__main__":
	
	unittest.main()