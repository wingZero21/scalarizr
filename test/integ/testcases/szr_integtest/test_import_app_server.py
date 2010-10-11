'''
Created on Oct 06, 2010

@author: shaitanich
'''
import unittest
from szr_integtest import get_selenium
from szr_integtest_libs import exec_command
from szr_integtest_libs.scalrctl import import_server, ScalrConsts
from test_import_ec2_server import ImportEc2Server, _parse_args
import time
import sys
import re 

class ImportAppServer(ImportEc2Server):

	def _install_software(self, channel, distr):
		#rhel , fedora, ubuntu
		self._logger.debug("PLATFORM: %s" % distr)
		debian = distr is 'debian'

		prefix, software  = ('apt-get -y install ',['apache2']) if debian else ('yum -y install ', ['httpd', 'mod_ssl'])
		install_cmd = prefix + ' '.join(software)
		self._logger.debug("Installing software: %s" % install_cmd)
		out = exec_command(channel, install_cmd)
		
		if debian:
			error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
			if error:
				raise Exception("Can't install %s: '%s'" % (software, error.group('err_text')))	
		
		else:
			if not re.search('Complete!|Nothing to do', out):
				raise Exception('Cannot install scalarizr')	
			
	def _get_role_name(self):
		return 'Test_app_%s' % time.strftime('%Y_%m_%d_%H%M')
	
	def _import_server(self, role_name):
		return import_server(get_selenium(), ScalrConsts.Platforms.PLATFORM_EC2 ,\
			ScalrConsts.Behaviours.BEHAVIOUR_APP, self.ip_address, role_name)	

class TestImportAppServer(unittest.TestCase):
		
	importer = None
	
	def setUp(self):
		self.importer = ImportAppServer()

	def test_import(self):
		self.importer.test_import()
		
	def tearDown(self):
		#self.importer.cleanup()
		pass
	

if __name__ == "__main__":
	sysargs = _parse_args()
	del sys.argv[1:]
	unittest.main()