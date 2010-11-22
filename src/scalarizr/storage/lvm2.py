'''
Created on Nov 11, 2010

@author: Dmytro Korsakov
'''

from scalarizr.util import system

class Lvm2:
	'''
	object-oriented interface to lvm2
	'''
	group = None
	
	def __init__(self, group='vg'):
		self.group = group
	
	def add_physical_volumes(self, *args):
		system(['pvcreate'] + list(args))	
	
	def remove_physical_volume(self, name, group=None):
		if not group: group = self.group
		system(['pvmove ', name])
		system(['vgreduce', group, name])
		system(['pvremove ', '-f', name])
		
	def get_pv_info(self):
		return [i.strip().split('|') for i in 
				system(['/sbin/pvs', '--separator', "|"], shell=False)[0].split('\n')[1:-1]]
		
	def get_physical_volumes(self):
		pvs = self.get_pv_info()
		if pvs:
			return [j[0] for j in pvs]
		
	
	def create_volume_group(self, group, block_size, *args):
		if not group: group = self.group
		if not block_size: block_size = '16M'
		print ['/sbin/vgcreate', '-s', block_size, group] + list(args)
		system(['/sbin/vgcreate', '-s', block_size, group] + list(args),  shell=False)
	
	def extend_group(self,group=None, *args):
		if not group: group = self.group
		system(['vgextend', group] + list(args))	
		
	def remove_volume_group(self, group=None):
		if not group: group = self.group
		system(['vgremove', group])
	
	def get_volume_groups_info(self):
		#TODO: parse output
		return [i.split() for i in system(['vgs'])[0].split('\n')[1:-1]]
	
	def get_logic_volume_size(self, lv_name):
		lv_info = self.get_logic_volumes_info()
		if lv_info:
			for lv in lv_info:
				if lv[0] == lv_name:
					return lv[3]
		return 0
	
	def get_volume_groups(self):
		vgs = self.get_volume_groups_info()
		if vgs:
			return [j[0] for j in vgs]
			
	
	def get_logic_volumes_info(self):
		return [i.strip().split('|') for i in 
				system(['/sbin/lvs', '--separator', "|"], shell=False)[0].split('\n')[1:-1]]
		
	def get_volume_group(self, device):
		lvs_info = self.get_logic_volumes_info()
		if lvs_info:
			for volume in lvs_info:
				if device.endswith(volume[0]):
					return volume[1]
		return None
	
	def get_vg_free_space(self, group_name):
		for group in self.get_volume_group_list():
			if group[0]==group_name:
				return group[-1]
		return 0
	
	def create_logic_volume(self, volume_name, size, group=None):
		if not group: group = self.group
		system(['lvcreate', '-n', volume_name, '-L', size, group])
		
	def create_snapshot_volume(self, volume_name, buf_size, l_volume, group=None):	
		if not group: group = self.group
		system(['lvcreate', '-s', '-n', volume_name, '-L', buf_size, '/dev/%s/%s'%(group,l_volume)])	
	
	def remove_logic_volume(self, group, volume_name):
		if not group: group = self.group
		system(['lvremove', '-f', '%/%' % (group, volume_name)])
			
	def repair_group(self, group):
		if not group: group = self.group
		system(['vgreduce', '--removemissing', group])
		system('vgchange', '-a', 'y', group)
		
	def dm_mod(self):
		'''
		Ubuntu 8 needs add dm_mod to kernel manually
		'''
		#modprobe dm_mod 
		system('modprobe', 'dm_mod')
		
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
		conn = S3Connection('AKIAJO6DOVEREBMYUERQ','LBEvgTXt+o7X3NsUr0c5paD4Uf9EWZsyrWMOixeD')
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
		
		
		os.environ["username"]='rackcloud05'
		os.environ["api_key"]='27630d6e96e72fa43233a185a0518f0e'
		
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