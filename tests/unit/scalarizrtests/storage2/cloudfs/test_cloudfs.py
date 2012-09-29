'''
Created on Sep 19, 2012

@author: Dmytro Korsakov
'''
import Queue
import mock

from scalarizr.storage2 import cloudfs



class TestTransfer(object):
	def test_job_generator(self):
		pass

	def test_job_generator_with_retries(self):
		pass


	def test_worker_upload(self):
		pass

	def test_worker_upload_multipart(self):
		pass

	def test_worker_upload_error(self):
		pass

	def test_worker_assertion_error(self):
		pass

	def test_worker_download(self):
		pass

	

def firstn(n=2):
	num = 0
	while num <= n:
		yield num
		num += 1
		

def lastn(n=2):
	num = n
	while num >= 0:
		yield num
		num -= 1
  
   
def test_transfer():
	
	data = [
		
		('/mnt/backups/daily.tar.gz',               's3://backups/mysql/2012-09-05/',          'upload'),
		('s3://backups/mysql/2012-09-05/',          '/mnt/backups/daily.tar.gz',               'download'),
		
		(['/root/one', 'root/two'],                 's3://mybucket/',                          'upload'),
		(['s3://mybucket/1', 's3://mybucket/2'],    '/root/backup',                            'download'),
				
		(['/root/one', 'root/two'],                 ['s3://mybucket/one','s3://mybucket/two'], 'upload'),
		(['s3://mybucket/one','s3://mybucket/two'], ['/root/one', 'root/two'],                 'download'),		

		('/mnt/backups/daily.tar.gz',               's3://backups/mysql/2012-09-05/',          'upload'),
		('s3://backups/mysql/2012-09-05/',          '/mnt/backups/daily.tar.gz',               'download'),
				
		(firstn,                                    lastn,                                    'upload'),
		(lastn,                                     firstn,                                   'download'),
		
		(firstn,                                                  's3://mybucket/',                                        'upload'),
		(firstn,                                                  ['s3://mybucket/0','s3://mybucket/1','s3://mybucket/2'], 'upload'),
		(['s3://mybucket/0','s3://mybucket/1','s3://mybucket/2'], firstn,                                                  'download'),		
		
		]
	
	for src, dst, direction in data:
		print '_____________________________'
		transfer = cloudfs.Transfer(src, dst)
		print direction, type(transfer.src), type(transfer.dst)
		for source, dst in zip(transfer.src, transfer.dst):
			print source, dst
		
