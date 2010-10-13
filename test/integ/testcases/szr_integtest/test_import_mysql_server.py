'''
Created on Oct 11, 2010

@author: spike
'''
import unittest
import sys
import time
import re

from szr_integtest.test_import_ec2_server import ImportEc2Server, _parse_args
from szr_integtest_libs.scalrctl import import_server, ScalrConsts
from szr_integtest import get_selenium
from szr_integtest_libs import exec_command

class ImportMysqlServer(ImportEc2Server):
		
	def _install_software(self, channel, distr):
		self._logger.info("Installing mysql server")
		if distr == 'debian':
			exec_command(channel, 'export DEBIAN_FRONTEND=noninteractive')
			out = exec_command(channel, 'apt-get -y install mysql-server', 240)
			error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
			if error:
				raise Exception("Can't install mysql package: '%s'" % error.group('err_text'))			
		else:
			out = exec_command(channel, 'yum -y install mysql-server mysql', 240)
			if not re.search('Complete!|Nothing to do', out):
				raise Exception('Cannot install mysql %s' % out)
			
	def _import_server(self, role_name):
		return import_server(get_selenium(), ScalrConsts.Platforms.PLATFORM_EC2 ,\
			ScalrConsts.Behaviours.BEHAVIOUR_MYSQL, self.ip_address, role_name)
		
	def _get_role_name(self):
		return 'Test_mysql_%s' % time.strftime('%Y_%m_%d_%H%M')

class TestImportMysqlServer(unittest.TestCase):
	
	importer = None
	
	def setUp(self):
		self.importer = ImportMysqlServer(sysargs)

	def test_import(self):
		self.importer.test_import()

	def tearDown(self):
		self.importer.cleanup()


if __name__ == "__main__":
	sysargs = _parse_args()
	del sys.argv[1:]
	unittest.main()