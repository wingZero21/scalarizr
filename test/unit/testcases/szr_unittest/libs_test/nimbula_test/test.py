'''
Created on Feb 14th, 2011

@author: Dmytro Korsakov
'''
import os
import unittest

from scalarizr.libs.nimbula import NimbulaConnection, NimbulaError, authenticate


nimbula_url = 'NIMBULA_URL'
nimbula_username = 'NIMBULA_USERNAME'
nimbula_password = 'NIMBULA_PASSWORD'

os.environ[nimbula_username] = '/scalr/administrator'
os.environ[nimbula_password] = 'vfJFort4'
os.environ[nimbula_url] = 'https://serverbeach.demo.nimbula.com:443'

class NimbulaConnectionTest(unittest.TestCase):

	conn = None

	def setUp(self):
		self.url = os.environ[nimbula_url]
		self.login = os.environ[nimbula_username]
		self.conn = NimbulaConnection(self.url,self.login)
		
	def _test_authenticate(self):
		cookie = authenticate()
		self.assertTrue(self.login in str(cookie))
		print "GOT COOKIES:", cookie[-1]

	def tearDown(self):
		pass
	
	def _test_get_URI(self):
		self.assertEquals(self.conn._get_object_URI('/scalr/administrator/imagename'), self.url+'/machineimage/scalr/administrator/imagename')
		self.assertEquals(self.conn._get_object_URI('imagename2'), '%s/machineimage%s/imagename2'%(self.url,self.login))
		
	def test_add_machine_image(self):
		image_path = '/root/pico_test.tar.gz'
		result = self.conn.add_machine_image('test_image', image_path)
		print result

	def _test_get_machine_image(self):
		image_name = '/nimbula/public/default'
		info = self.conn.get_machine_image(image_name)
		#print "GOT SERVER INFO:", info
		self.assertTrue(image_name in str(info))
		
		info2 = self.conn.get_machine_image(image_name)
		#print "GOT SERVER INFO2:", info2

	def _test_delete_machine_image(self):
		protected_image_name = '/nimbula/public/default'
		self.assertRaises(NimbulaError, self.conn.delete_machine_image, (protected_image_name))
		#info = self.conn.delete_machine_image('/scalr/administrator/test_cli_1')
		#print info

	def _test_discover_machine_image(self):
		container = '/nimbula/public/'
		info = self.conn.discover_machine_image(container)
		self.assertTrue(container in str(info))


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()