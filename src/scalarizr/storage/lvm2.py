'''
Created on Nov 11, 2010

@author: Dmytro Korsakov
'''

import os
import re
from scalarizr.util import system, disttool

def err_handle(_system):
	def handle(*args):
		out = _system(*args)
		if out and type(out)==tuple and len(out)>1:
			e = out[1] 
			if e:
				raise Lvm2Error(e)
	return handle

class Lvm2Error(BaseException):
	pass

class Lvm2:
	'''
	object-oriented interface to lvm2
	'''
	group = None
	
	def __init__(self, group='vg'):
		self.group = group
		
	def _parse_table(self, cmd='lvs'):
		full_path = '/sbin/%s' % cmd
		if not os.path.exists(full_path) or cmd not in ('lvs','vgs', 'pvs'):
			raise Lvm2Error('Cannot parse output')
		
		out = system([full_path, '--separator', "|"], shell=False)[0]
		if not out:
			raise Lvm2Error('%s returned empty output') % full_path
		return [i.strip().split('|') for i in out.split('\n')[1:-1]]		
		
		
		
	def get_pv_info(self):
		return self._parse_table('pvs')
		
	def get_vg_info(self):
		return self._parse_table('vgs')				
	
	def get_lv_info(self):
		return self._parse_table('lvs')		
	
	
	
	def get_pv_list(self):
		return [j[0] for j in self.get_pv_info()]
		
	def get_vg_list(self):
		return [j[0] for j in self.get_vg_info()]
		
	def get_lv_list(self):
		return [j[0] for j in self.get_lv_info()]	
		

	@err_handle
	def create_pv(self, *args):
		return system(['/sbin/pvcreate'] + list(args), shell=False)
		
	@err_handle
	def create_vg(self, group, block_size, *args):
		if not group: group = self.group
		if not block_size: block_size = '4M'
		return system(['/sbin/vgcreate', '-s', block_size, group] + list(args), shell=False)
	
	@err_handle	
	def create_lv(self, volume_name, size, group=None):
		if not group: group = self.group
		return system(['/sbin/lvcreate', '-n', volume_name, '-L', size, group], shell=False)
	
	@err_handle	
	def create_lv_snapshot(self, volume_name, buf_size, l_volume, group=None):	
		if not group: group = self.group
		return system(['/sbin/lvcreate', '-s', '-n', volume_name, '-L', buf_size, '/dev/%s/%s'%(group,l_volume)], shell=False)
	
	
	@err_handle
	def remove_pv(self, name, group=None):
		if not group: group = self.group
		#system(['/sbin/pvmove', name], shell=False)
		#system(['/sbin/vgreduce', group, name], shell=False)
		return  system(['/sbin/pvremove', '-ff', name], shell=False)
	
	@err_handle	
	def remove_vg(self, group=None):
		if not group: group = self.group
		return system(['/sbin/vgremove', group], shell=False)		
	
	@err_handle
	def remove_lv(self, group, volume_name):
		if not group: group = self.group
		return system(['/sbin/lvremove', '-f', '%/%' % (group, volume_name)], shell=False)	
		
	def remove_lv_snapshot(self):
		pass	
	
	

	def get_lv_size(self, lv_name):
		lv_info = self.get_logic_volumes_info()
		if lv_info:
			for lv in lv_info:
				if lv[0] == lv_name:
					return lv[3]
		return 0
	
	def get_pv_size(self, pv_name):
		pass
	
	def get_vg_size(self, vg_name):
		pass
	
	
	
	def get_vg_free_space(self, group_name=None):
		'''
		@return tuple('amount','suffix')
		'''
		if not group_name: group_name = self.group
		for group in self.get_vg_info():
			if group[0]==group_name:
				raw = re.search('(\d+\.*\d*)(\D*)',group[-1])
				if raw:
					return (raw.group(1), raw.group(2))
				raise Lvm2Error('Cannot determine available free space in group %s' % group_name)
		raise Lvm2Error('Group %s not found' % group_name)		

	def get_vg_name(self, device):
		lvs_info = self.get_logic_volumes_info()
		if lvs_info:
			for volume in lvs_info:
				if device.endswith(volume[0]):
					return volume[1]
		return None
	
	@err_handle		
	def extend_vg(self,group=None, *args):
		if not group: group = self.group
		return system(['/sbin/vgextend', group] + list(args), shell=False)
	
	@err_handle	
	def repair_vg(self, group):
		if not group: group = self.group
		e = system(['/sbin/vgreduce', '--removemissing', group], shell=False)[1]
		if e:
			raise Lvm2Error

		return system(['/sbin/vgchange', '-a', 'y', group], shell=False)
		
		
	@err_handle		
	def dm_mod(self):
		'''
		Ubuntu 8 needs add dm_mod to kernel manually
		'''
		if disttool.is_ubuntu():
			#modprobe dm_mod 
			return system('/sbin/modprobe', 'dm_mod', shell=False)
	