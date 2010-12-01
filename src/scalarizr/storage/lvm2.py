'''
Created on Nov 11, 2010

@author: Dmytro Korsakov
'''

from scalarizr.util import system2, disttool, firstmatched, PopenError

import re
from collections import namedtuple
import logging
import os

logger = logging.getLogger(__name__)

def system(*args, **kwargs):
	kwargs['logger'] = logger
	kwargs['close_fds'] = True
	'''
	To prevent this garbage in stderr (Fedora/CentOS):
	File descriptor 6 (/tmp/ffik4yjng (deleted)) leaked on lv* invocation. 
	Parent PID 29542: /usr/bin/python
	'''
	kwargs['exc_class'] = Lvm2Error
	return system2(*args, **kwargs)

class Lvm2Error(PopenError):
	pass

class PVInfo(namedtuple('PVInfo', 'pv vg format attr size free')):
	pass

class VGInfo(namedtuple('VGInfo', 'vg num_pv num_lv num_sn attr size free')):
	@property
	def path(self):
		return '/dev/%s' % (self[0],)
	
class LVInfo(namedtuple('LVInfo', 'lv vg attr size origin snap_pc move log copy_pc convert')):
	@property
	def path(self):
		return lvpath(self[1], self[0])

def lvpath(group, lvol):
	return '/dev/mapper/%s-%s' % (group.replace('-', '--'), lvol.replace('-', '--'))

def extract_vg_lvol(lvolume):
	''' 
	Return (vg, lvol) from logical device name 
	Example:
		/dev/mapper/vg0-vol0 -> ('vg0', 'vol0')
		/dev/mapper/my--volume--group-data -> ('my-volume-group' ,'data')
	'''
	vg_lvol = os.path.basename(lvolume).split('-')
	if len(vg_lvol) > 2:
		ret = []
		for s in vg_lvol:
			if len(ret) and not ret[-1][-1] or not s:
				ret[-1].append(s)
			else:
				ret.append([s])
		vg_lvol = map(lambda x: '-'.join(filter(None, x)), ret)
	return tuple(vg_lvol)	

def normalize_lvname(lvolume):
	if '/dev/mapper' in lvolume:
		return '/dev/%s/%s' % extract_vg_lvol(lvolume)
	else:
		return lvolume



class Lvm2:
	'''
	Object-oriented interface to lvm2
	'''
	
	def __init__(self):
		if disttool.is_debian_based():
			system(['sbin/modprobe', 'dm_mod', 'dm_snapshot'], 
					error_text='Cannot load device mapper kernel module')
		
	def _parse_status_table(self, name, ResultClass):
		if not name in ('lvs','vgs', 'pvs'):
			raise ValueError('Unexpected value: %s' % name)
		if isinstance(ResultClass, tuple):
			raise ValueError('ResultClass should be a namedtuple subclass. %s taken' % type(ResultClass))
		
		out = system(['/sbin/%s' % name, '--separator', '|'])[0].strip()
		if out:
			return tuple(ResultClass(*line.strip().split('|')) for line in out.split('\n')[1:])
		return ()

	def _status(self, name, ResultClass, column=None):
		rows = self._parse_status_table(name, ResultClass)
		if column:
			return tuple(getattr(o, column) for o in rows)
		return rows
		
	def pv_status(self, column=None):
		return self._status('pvs', PVInfo, column)
	
	def vg_status(self, column=None):
		return self._status('vgs', VGInfo, column)				
	
	def lv_status(self, column=None):
		return self._status('lvs', LVInfo, column)		

	def pv_info(self, ph_volume):
		info = firstmatched(lambda inf: inf.pv == ph_volume, self.pv_status())
		if info:
			return info
		raise LookupError('Physical volume %s not found' % ph_volume)

	def vg_info(self, group):
		info = firstmatched(lambda inf: inf.vg == group, self.vg_status())
		if info:
			return info
		raise LookupError('Volume group %s not found' % group)
	
	def lv_info(self, lvolume=None, group=None, name=None):
		lvolume = lvolume if lvolume else '/dev/%s/%s' % (group, name)
		info = firstmatched(lambda inf: inf.path == lvolume, self.lv_status())
		if info:
			return info
		raise LookupError('Logical volume %s not found' % lvolume)
	
	def create_pv(self, *devices):
		system(['/sbin/pvcreate'] + list(devices), 
				error_text='Cannot initiate a disk for use by LVM')
		
	def create_vg(self, group, ph_volumes, ph_extent_size=4):
		system(['/sbin/vgcreate', '-s', ph_extent_size, group] + list(ph_volumes), 
				error_text='Cannot create a volume group %s' % group)
		return '/dev/%s' % group
	
	def create_lv(self, group=None, name=None, extents=None, size=None, segment_type=None, ph_volumes=None):
		args = ['/sbin/lvcreate']
		if name:
			args += ('-n', name)
		if extents:
			args += ('-l', extents)
		elif size:
			args += ('-L', size)
		if segment_type:
			args += ('--type=' + segment_type,)
		if group and segment_type != 'snapshot':
			args.append(group)
		if ph_volumes:
			args += ph_volumes
		
		out = system(args, error_text='Cannot create logical volume')[0].strip()
		vol = re.match(r'Logical volume "([^\"]+)" created', out.split('\n')[-1].strip()).group(1)
		return lvpath(os.path.basename(group), vol)
	
	def create_lv_snapshot(self, lvolume, name=None, extents=None, size=None):
		vg = extract_vg_lvol(lvolume)[0]
		return self.create_lv(vg, name, extents, size, segment_type='snapshot', ph_volumes=(normalize_lvname(lvolume),))
	
	def change_lv(self, lvolume, available=None):
		cmd = ['/sbin/lvchange']
		if available is not None:
			cmd.append('-ay' if available else '-an')
		cmd.append(normalize_lvname(lvolume))
		system(cmd, error_text='Cannot change logical volume attributes')
	
	def remove_pv(self, ph_volume):
		if self.pv_info(ph_volume).vg:
			system(('/sbin/vgreduce', '-f', ph_volume), error_text='Cannot reduce volume group')
		system(('/sbin/pvremove', '-ff', ph_volume), error_text='Cannot remove a physical volume')
	
	def remove_vg(self, group):
		system(('/sbin/vgremove', '-ff', group), error_text='Cannot remove volume group')
	
	def remove_lv(self, lvolume):
		lvi = self.lv_info(lvolume)
		if 'a' in lvi.attr:
			self.change_lv(lvolume, available=False)
		system(('/sbin/lvremove', '-f', normalize_lvname(lvolume)), error_text='Cannot remove logical volume')	

	def extend_vg(self, group, *ph_volumes):
		system(['/sbin/vgextend', group] + list(ph_volumes), error_text='Cannot extend volume group')
	
	def repair_vg(self, group):
		system(('/sbin/vgreduce', '--removemissing', group))
		system(('/sbin/vgchange', '-a', 'y', group))


	# Untested ---> 

	def get_lv_size(self, lv_name):
		lv_info = self.get_logic_volumes_info()
		if lv_info:
			for lv in lv_info:
				if lv[0] == lv_name:
					return lv[3]
		return 0
	
	def get_vg_free_space(self, group=None):
		'''
		@return tuple('amount','suffix')
		'''
		if not group: group = self.group
		for group_name in self.get_vg_info():
			if group_name[0]==group:
				raw = re.search('(\d+\.*\d*)(\D*)',group_name[-1])
				if raw:
					return (raw.group(1), raw.group(2))
				raise Lvm2Error('Cannot determine available free space in group %s' % group)
		raise Lvm2Error('Group %s not found' % group)		
	