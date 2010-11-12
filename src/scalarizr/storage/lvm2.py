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
		#
		# #pvcreate /dev/sda
		#
		
		#if adding to existed group, also need to do: 
		#
		# #vgextend vg /dev/sdc
		#
		pass
	
	def remove_physical_volume(self, name):
		#
		# #pvmove /dev/sda
		# #vgreduce vg /dev/sd
		# #pvremove /dev/sda
		#
		pass
	
	def create_volume_group(self, device, group=None, block_size='32M'):
		#
		# #vgcreate -s 32M vg /dev/sda /dev/sdb
		#
		if not group:
			group = self.group
	
	def get_volume_group_list(self):
		#
		# #vgs
		#
		pass
	
	def create_logic_volume(self, volume_name, size, group=None, mirrors=None, journal_size=None):
		#
		# #lvcreate -n first -L 20G vg
		#
		#or if mirrored:
		#lvcreate -n data -m 1 -l 10 vg
		if not group:
			group = self.group
		
	def get_logic_volume_list(self):
		#
		# #lvs
		#
		pass
	
	def remove_logic_volume(self, group, volume_name):
		#
		# #lvremove vg/first
		#
		if not group:
			group = self.group
			
	def resize_volume(self, volume_name, new_size):
		#for raiserFS resize down:
		#
		# #resize_reiserfs -s 19G /dev/vg/second
		# #lvresize -L 20G vg/second
		# #resize_reiserfs /dev/vg/second
		#
		
		#for reiserFS resize up:
		#
		# lvresize -L 40G vg/second
		# resize_reiserfs /dev/vg/second
		#

		#also may need to resize ext2,ext3, xfs
		pass
		
	def format_logic_volume(self, name, group=None, fs='ext3', label=None):
		#mke2fs -L first -t ext4 /dev/vg/first
		if not group:
			group = self.group
			
	def repair_group(self, group):
		#vgreduce --removemissing vg
		#vgchange -a y vg
		if not group:
			group = self.group
			

class FileSystem(object):
	fs = None
	size = None
	used = None
	available = None
	use_percentage = None
	mountpoint = None
	
def get_fs_info(fs):
	#parse df -h
	# return FileSystem
	pass