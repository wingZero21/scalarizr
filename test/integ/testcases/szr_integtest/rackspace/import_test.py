'''
Created on Jan 4, 2011

@author: spike
'''
from szr_integtest_libs.scalrctl	import ScalrCtl
from szr_integtest_libs.datapvd		import DataProvider

from szr_integtest.ec2.import_test	import _init_server

import unittest
import logging



class ImportRackspaceTest(unittest.TestCase):
	tags = ['rs', 'import']
	
	def __init__(self, methodName='runTest'):
		super(ImportRackspaceTest, self).__init__(methodName)
		self._logger = logging.getLogger(__name__)
		
	def _get_dp(self):
		return DataProvider(behaviour='raw')
	
	def test_import(self):
		dp = self._get_dp()
		server = _init_server(dp, self._logger)
		reader = server.log
		reader.expect( "Message 'Hello' delivered",				 					240)
		
		scalrctl = ScalrCtl()
		scalrctl.exec_cronjob('ScalarizrMessaging')
		
		reader.expect( "Searching our instance in server list.",					240)
		reader.expect( "Instance has been successfully found", 						240)
		reader.expect( "Creating instance's image",									240)
		img_id = ("Waiting for image completion. Image id - (?P<img_id>\d+)",		240)
		self._logger.info('Waiting for image id=%s creation.' % img_id)		
		reader.expect("Image has been successfully created.",						360)
		self._logger.info("Image has been successfully created")
		reader.expect("Updating message with os and software info.",				120)
		reader.expect("Message 'RebundleResult' delivered",							120)
		self._logger.info("Rebundle result delivered. Rebundle complete!")
		scalrctl.exec_cronjob('ScalarizrMessaging')
		scalrctl.exec_cronjob('BundleTasksManager')


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()