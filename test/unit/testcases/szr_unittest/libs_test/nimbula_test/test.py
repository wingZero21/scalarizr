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


class NimbulaConnectionTest(unittest.TestCase):

	conn = None

	def setUp(self):
		self.img_name = 'test_cli'
		self.image_path = '/root/test.tar.gz'
		self.url = os.environ[nimbula_url]
		self.login = os.environ[nimbula_username]
		self.conn = NimbulaConnection(self.url,self.login)

	def tearDown(self):
		pass
	
	
	def test_get_URI(self):
		self.assertEquals(self.conn._get_object_URI('/scalr/administrator/imagename'), self.url+'/machineimage/scalr/administrator/imagename')
		self.assertEquals(self.conn._get_object_URI('imagename2'), '%s/machineimage%s/imagename2'%(self.url,self.login))
		
	def test_authenticate(self):
		cookie = authenticate()
		self.assertTrue(self.login in str(cookie))
		#print "GOT COOKIES:", cookie[-1]
		
	def test_all(self):
		container = self.login+'/'
		list_images = self.conn.discover_machine_image(container)
		
		for img in list_images:
			if img.name.endswith(self.img_name):
				self.conn.delete_machine_image(img.name)
		
		result = self.conn.add_machine_image(self.img_name, self.image_path)
		
		image = self.conn.get_machine_image(self.img_name)
		self.assertTrue(image == result)
		
		new_image_list = self.conn.discover_machine_image(container)
		self.assertTrue(image in new_image_list)	
		
		self.conn.delete_machine_image(self.img_name)	
		
		cleaned_image_list = self.conn.discover_machine_image(container)
		self.assertTrue(image not in cleaned_image_list)
		
	def test_errors(self):
		protected_image_name = '/nimbula/public/default'
		
		self.assertRaises(NimbulaError, self.conn.delete_machine_image, (protected_image_name))
		
		self.assertRaises(NimbulaError, self.conn.add_machine_image, (protected_image_name, self.image_path))


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()