'''
Created on Nov 11, 2010

@author: marat
'''
import os
import re
from scalarizr.util import firstmatched, system
from scalarizr.storage import _system
from scalarizr.util import filetool
MDADM_PATH='/sbin/mdadm'


class Mdadm:
	
	def __init__(self):
		if not os.path.exists(MDADM_PATH):
			raise Exception("Make sure you have mdadm package installed.")
		
	def create(self, devices, level=1):
		if not int(level) in (0,1,5):
			raise Exception('Unknown RAID level: %s' % level)
		
		devname = '/dev/md%s' % firstmatched(lambda x: not os.path.exists('/dev/md%s' % x), range(100))
		
		cmd = '%s --create %s --level=%s -n %s %s' % (MDADM_PATH, devname, level, len(devices), ' '.join(devices))
		error = 'Error occured during raid device creation.'
		_system(cmd, error)

		
	def delete(self, array):
		devname = os.path.basename(array)
		
		_system('%s -S %s' % (MDADM_PATH, array), 'Error occured during array stopping')
		_system('%s --remove %s' % (MDADM_PATH, array), 'Error occured during array deletion')
		
		out = filetool.read_file('/proc/mdstat')
		if not out:
			raise Exception("Can't get array info from /proc/mdstat.")
		
		for line in out.splitlines():
			if not line.startswith(devname):
				continue
			devices = re.findall('([^\s]+)\[\d+\]', line)
			break
		
		for device in devices:
			_system('%s --zero-superblock /dev/%s' % (MDADM_PATH, device), 'Error occured during zeroing superblock of %s' % device)
		
	def remove_disk(self, device):
		devname = os.path.basename(device)
		out = filetool.read_file('/proc/mdstat')
		if not out:
			raise Exception("Can't get array info from /proc/mdstat.")
		
		for line in out.splitlines():
			if devname in line:
				array = line.split()[0]
				break
		else:
			raise Exception("Device %s isn't part of any array.")
		
		_system('%s %s --fail %s --remove %s' %(MDADM_PATH, array, device, device), 'Error occured during device removal')
		
	def add_disk(self, array, device):
		_system('%s --add %s %s' % (MDADM_PATH, array, device), 'Error occured during device addition')
		raid_devs, total_devs = self._get_array_disk_info(array)
		if total_devs > raid_devs:
			_system('%s --grow %s --raid-devices=%s' % (MDADM_PATH, array, total_devs),\
				    'Error occured during array "%s" growth')		
		
	def _get_array_disk_info(self, array):
		out = _system('%s -D %s' % (MDADM_PATH, array), 'Error occured while array "%s" info obtaining' % array)
		raid_devices = re.search('Raid\s+Devices\s+:\s+(?P<count>\d+)', out).group('count')
		total_devices = re.search('Total\s+Devices\s+:\s+(?P<count>\d+)', out).group('count')
		return raid_devices, total_devices