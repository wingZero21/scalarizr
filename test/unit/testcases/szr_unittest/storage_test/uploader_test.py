__author__ = 'shaitanich'
  
'''
Created on Nov 18, 2010

@author: shaitanich
'''
import unittest
from random import randint
from scalarizr.storage import uploader
import os 
import cloudfiles
from boto.s3.connection import S3Connection


class UploaderTest(unittest.TestCase):


	def setUp(self):
		self.fname = str(randint(1000000, 9999999))
		self.cont = str(randint(1000000, 9999999))
		self.prefix = str(randint(1000000, 9999999))
		self.content = str(randint(1000000, 9999999))
		self.target_file = str(randint(1000000, 9999999))
		file = open(self.fname, 'w')
		file.write(self.content)
		file.close()
		self.files = [self.fname]
		self.obj_name = '%s/%s'%(self.prefix, self.fname)


	def tearDown(self):
		#delete file and entry in cloud
		os.remove(self.fname)
		
		
	def test_S3(self):
		conn = S3Connection()
		if not self.cont in [b.name for b in conn.get_all_buckets()]:
			bucket = conn.create_bucket(self.cont)
		else:
			bucket = conn.get_bucket(self.cont)
		
		U = uploader.Uploader(pool=2)
		s3ud = uploader.S3UploadDest(bucket)
		U.upload(self.files, s3ud)
		
		#check container exists
		buckets = [b.name for b in conn.get_all_buckets()]
		self.assertTrue(self.cont in buckets)
		
		#check file uploaded and prefix is ok
		objects = [key.name for key in bucket.get_all_keys()]
		self.assertTrue(self.fname in objects)
		
		#check file contains appropriate data
		key = bucket.get_key(self.fname)
		key.get_contents_to_filename(self.target_file)
		t_content = open(self.target_file, 'r').read()
		self.assertEquals(self.content, t_content)
		
		#delete files
		for key in bucket.get_all_keys():
			key.delete()
		#delete bucket
		conn.delete_bucket(self.cont)
	

	def _test_cloud_files(self):		
		U = uploader.Uploader(pool=2)
		cfud = uploader.CloudFilesUploadDest(self.cont, self.prefix)
		U.upload(self.files, cfud)
		
		connection = cloudfiles.get_connection(username=os.environ["username"], api_key=os.environ["api_key"], serviceNet=True)
		#check container exists
		containers = [c.name for c in connection.get_all_containers()]
		self.assertTrue(self.cont in containers)
		
		#check file uploaded and prefix is ok
		container = connection.get_container(self.cont)
		objects = [c.name for c in container.get_objects()]
		self.assertTrue(self.obj_name in objects)
		
		#check file contains appropriate data
		obj = container.get_object(self.obj_name)
		obj.save_to_filename(self.target_file)
		t_content = open(self.target_file, 'r').read()
		self.assertEquals(self.content, t_content)
		
		container.delete_object(self.obj_name)
		#delete container
		connection.delete_container(self.cont)
		#remove target file
		os.remove(self.target_file) 
		
		
if __name__ == "__main__":
	unittest.main()