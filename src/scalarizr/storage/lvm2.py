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
		system(['pvremove ', name])
	
		
	'''
	def get_vg_size(self, group):
		return None
	
	def get_optimal_block_size(self, group):
		max_chunks = 64000
		size = self.get_optimal_block_size(group)
		return size / max_chunks
	'''
	
	def create_volume_group(self, group=None, block_size=None, *args):
		if not group: group = self.group
		system(['vgcreate', '-s', block_size, group] + args)
	
	def get_volume_group_list(self):
		#TODO: parse output
		return system(['vgs'])[0]
	
	def create_logic_volume(self, volume_name, size, group=None):
		if not group: group = self.group
		system(['lvcreate', '-n', volume_name, '-L', size, group])		
		
	def get_logic_volume_list(self):
		#TODO: parse output
		return system(['lvs'])[0]
	
	def remove_logic_volume(self, group, volume_name):
		if not group: group = self.group
		system(['lvremove','%/%' % (group, volume_name)])
			
	def repair_group(self, group):
		if not group: group = self.group
		system(['vgreduce', '--removemissing', group])
		system('vgchange', '-a', 'y', group)
		
			