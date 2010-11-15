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
	
	def add_physical_volumes(self, add_to_group=False, group=None, *args):
		system(['pvcreate'] + args)
		if add_to_group:
			if not group: group = self.group
			system(['vgextend', group] + args)
	
	def remove_physical_volume(self, name, group=None):
		if not group: group = self.group
		system(['pvmove ', name])
		system(['vgreduce', group, name])
		system(['pvremove ', '-f', name])
	
	def create_volume_group(self, group=None, block_size=None, *args):
		if not group: group = self.group
		system(['vgcreate', '-s', block_size, group] + args)
	
	def get_volume_group_list(self):
		#TODO: parse output
		return system(['vgs'])[0]
	
	def create_logic_volume(self, volume_name, size, group=None):
		if not group: group = self.group
		system(['lvcreate', '-n', volume_name, '-L', size, group])
		
	def create_snapshot_volume(self, volume_name, buf_size, group=None, l_volume):	
		if not group: group = self.group
		system(['lvcreate', '-s', '-n', volume_name, '-L', buf_size, '/dev/%s/%s'%(group,l_volume)])	
		
	def get_logic_volume_list(self):
		#TODO: parse output
		return system(['lvs'])[0]
	
	def remove_logic_volume(self, group, volume_name):
		if not group: group = self.group
		system(['lvremove', '-f', '%/%' % (group, volume_name)])
			
	def repair_group(self, group):
		if not group: group = self.group
		system(['vgreduce', '--removemissing', group])
		system('vgchange', '-a', 'y', group)
		
			