'''
Created on Oct 11, 2010

@author: spike
'''
import unittest
import sys
import time
from szr_integtest.test_import_ec2_server import ImportEc2Server, _parse_args, TestImportEc2Server
from szr_integtest_libs.scalrctl import import_server, ScalrConsts
from szr_integtest import get_selenium

class ImportMysqlServer(ImportEc2Server):
	def _install_software(self, channel, distr):
		if distr == 'debian':
			
			pass
		else:
			pass
			
	def _import_server(self, role_name):
		return import_server(get_selenium(), ScalrConsts.Platforms.PLATFORM_EC2 ,\
			ScalrConsts.Behaviours.BEHAVIOUR_MYSQL, self.ip_address, role_name)
		
	def _get_role_name(self):
		return 'Test_mysql_%s' % time.strftime('%Y_%m_%d_%H%M')

class TestImportMysqlServer(TestImportEc2Server):
	importer = None


if __name__ == "__main__":
	sysargs = _parse_args()
	del sys.argv[1:]
	unittest.main()