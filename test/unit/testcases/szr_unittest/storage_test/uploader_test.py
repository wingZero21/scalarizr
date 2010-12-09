__author__ = 'shaitanich'
  
'''
Created on Nov 18, 2010

@author: shaitanich
'''
import os 
import logging
import unittest
from random import randint

from scalarizr.storage import transfer
from scalarizr.platform.ec2.storage import S3TransferProvider
from scalarizr.platform.rackspace.storage import CFTransferProvider
from szr_unittest import main

import cloudfiles
from boto.s3.connection import S3Connection


'''
Transfer.explore_provider(S3TransferProvider)
trn = Transfer(pool=5, max_attempts=3)
trn.upload(files, 'cf://container/path/to/candy')
trn.download('s3://scalr-files/path/to/some-shit/', dst, recursive=True)
'''


class UploaderTest(unittest.TestCase):

	def setUp(self):
		
		os.environ["username"]='rackcloud05'
		os.environ["api_key"]='27630d6e96e72fa43233a185a0518f0e'
		
		self.fname = str(randint(1000000, 9999999)) + '.file'
		self.cont = 'container' + str(randint(1000000, 9999999))
		self.prefix = 'remotedir' + str(randint(1000000, 9999999))
		self.content = 'random_content' + str(randint(1000000, 9999999))
		self.target_file = str(randint(1000000, 9999999))
		file = open(self.fname, 'w')
		file.write(self.content)
		file.close()
		self.files = [self.fname]
		self.obj_name = '/%s/%s'%(self.prefix, self.fname)
		self._logger = logging.getLogger(__name__)
		self.new_dir = os.path.join(os.path.curdir, 'downloads')
		if not os.path.exists(self.new_dir):
			os.makedirs(self.new_dir)

	def tearDown(self):
		#cleaning downloaded junk
		for junk in os.listdir(self.new_dir):
			os.remove(os.path.join(self.new_dir,junk))
		if os.path.exists(self.new_dir):
			os.removedirs(self.new_dir)
		#delete file and entry in cloud
		os.remove(self.fname)
		
	def _test_S3(self):
		conn = S3Connection()
		if not self.cont in [b.name for b in conn.get_all_buckets()]:
			bucket = conn.create_bucket(self.cont)
		else:
			bucket = conn.get_bucket(self.cont)
		
		U = transfer.Transfer(pool=2, logger=self._logger)
		s3ud = S3TransferProvider(bucket, self.prefix)
		U.upload(self.files, s3ud)
		
		#check container exists
		buckets = [b.name for b in conn.get_all_buckets()]
		self.assertTrue(self.cont in buckets)
		
		#check file uploaded and prefix is ok
		objects = [key.name for key in bucket.get_all_keys()]
		self.assertTrue(self.obj_name in objects)
		
		#check file contains appropriate data
		U.download(self.new_dir, s3ud)
		t_content = open(os.path.join(self.new_dir, os.path.basename(self.fname)), 'r').read()
		self.assertEquals(self.content, t_content)
		
		#delete files
		for key in bucket.get_all_keys():
			key.delete()
		#delete bucket
		conn.delete_bucket(self.cont)
	
	def test_cloud_files(self):		
		transfer.Transfer.explore_provider(CFTransferProvider)
		U = transfer.Transfer(pool=2, logger=self._logger)
		#cfud = CFTransferProvider(self.cont, self.prefix)
		#U.upload(self.files, cfud)
		url = 'cf://%s/%s/' % (self.cont, self.prefix)
		print url
		
		U.upload(self.files, url)
		
		
		connection = cloudfiles.get_connection(username=os.environ["username"], api_key=os.environ["api_key"], serviceNet=True)
		#check container exists
		containers = [c.name for c in connection.get_all_containers()]
		
		self.assertTrue(self.cont in containers)
		
		#check file uploaded and prefix is ok
		container = connection.get_container(self.cont)
		objects = [c.name for c in container.get_objects()]
		print "obj_name", self.obj_name, 'objects', objects
		self.assertTrue(self.obj_name in objects)
		
		print "Downloading the file back"
		#check file contains appropriate data
		print url
		U.download(url, self.new_dir)
		t_content = open(os.path.join(self.new_dir, os.path.basename(self.fname)), 'r').read()
		self.assertEquals(self.content, t_content)
			
		container.delete_object(self.obj_name)
		#delete container
		connection.delete_container(self.cont)
		
		
if __name__ == "__main__":
	main()
	unittest.main()