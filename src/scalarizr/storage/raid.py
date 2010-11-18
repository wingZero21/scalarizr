'''
Created on Nov 11, 2010

@author: marat
'''
import os
import re
from scalarizr.util import firstmatched, system, filetool, wait_until
from scalarizr.storage import _system


MDADM_PATH='/sbin/mdadm'


class Mdadm:

	def __init__(self):
		if not os.path.exists(MDADM_PATH):
			raise Exception("Make sure you have mdadm package installed.")
		self.__raid_devices_re  = re.compile('Raid\s+Devices\s+:\s+(?P<count>\d+)')
		self.__total_devices_re = re.compile('Total\s+Devices\s+:\s+(?P<count>\d+)')
		self.__state_re         = re.compile('State\s+:\s+(?P<state>.+)')
		self.__rebuild_re       = re.compile('Rebuild\s+Status\s+:\s+(?P<percent>\d+)%')
		self.__level_re			= re.compile('Raid Level : (?P<level>.+)')
		
	def create(self, devices, level=1):
		if not int(level) in (0,1,5):
			raise Exception('Unknown RAID level: %s' % level)
		
		devname = '/dev/md%s' % firstmatched(lambda x: not os.path.exists('/dev/md%s' % x), range(100))
		
		for device in devices:
			try:
				self._zero_superblock(device)
			except:
				pass
			
		cmd = '%s --create %s --level=%s -f -e default -n %s %s' % (MDADM_PATH, devname, level, len(devices), ' '.join(devices))
		error = 'Error occured during raid device creation.'
		_system(cmd, error)
		return devname

		
	def delete(self, array):
		if not os.path.exists(array):
			raise Exception('Device %s does not exist' % array)
		
		devices = self.get_array_devices(array)
		wait_until(lambda: not self.get_array_info(array)['rebuild_status'])
		_system('%s -S -f %s' % (MDADM_PATH, array), 'Error occured during array stopping')
		try:
			_system('%s --remove -f %s' % (MDADM_PATH, array), 'Error occured during array deletion')
		except Exception, e:
			if not 'No such file or directory' in str(e):
				raise
			
		for device in devices:
			self._zero_superblock(device)

	def remove_disk(self, device):
		array = self._get_array_by_device(device)
		wait_until(lambda: not self.get_array_info(array)['rebuild_status'])
		_system('%s %s -f --fail %s' %(MDADM_PATH, array, device), 'Error occured while markin device as failed')
		_system('%s %s -f --remove %s' %(MDADM_PATH, array, device), 'Error occured during device removal')

	def add_disk(self, array, device, grow=True):
		info = self.get_array_info(array)
		if info['level'] == 'raid0':
			raise Exception("Can't add devices to raid level 0.")
		wait_until(lambda: not self.get_array_info(array)['rebuild_status'])
		_system('%s --add %s %s' % (MDADM_PATH, array, device), 'Error occured during device addition')
		
		if grow:
			array_info = self.get_array_info(array)
			raid_devs = array_info['raid_devices']
			total_devs = array_info['total_devices']
		
			if total_devs > raid_devs:
				_system('%s --grow %s --raid-devices=%s' % (MDADM_PATH, array, total_devs),\
					    'Error occured during array "%s" growth')		

	def get_array_info(self, array):
		ret = {}
		out = _system('%s -D %s' % (MDADM_PATH, array), 'Error occured while array "%s" info obtaining' % array)
		ret['raid_devices']   = int(re.search(self.__raid_devices_re, out).group('count'))
		ret['total_devices']  = int(re.search(self.__total_devices_re, out).group('count'))
		ret['state']		  = re.search(self.__state_re, out).group('state')
		ret['level']		  = re.search(self.__level_re, out).group('level')
		rebuild_res    		  = re.search(self.__rebuild_re, out)
		ret['rebuild_status'] = rebuild_res.group('percent') if rebuild_res else None
		return ret

	def _get_array_by_device(self, device):
		devname = os.path.basename(device)
		out = filetool.read_file('/proc/mdstat')
		if not out:
			raise Exception("Can't get array info from /proc/mdstat.")
		
		for line in out.splitlines():
			if devname in line:
				array = line.split()[0]
				break
		else:
			raise Exception("Device %s isn't part of any array." % device)
		
		return '/dev/%s' % array

	def _zero_superblock(self, device):
		devname = os.path.basename(device)
		_system('%s --zero-superblock -f /dev/%s' % (MDADM_PATH, devname), 'Error occured during zeroing superblock of %s' % device)

	def replace(self, old, new):
		array = self._get_array_by_device(old)
		if self.get_array_info(array)['level'] == 'raid0':
			raise Exception("Can't replace disk in raid level 0.")
		self.add_disk(array, new, False)
		self.remove_disk(old)


	def get_array_devices(self, array):
		devname = os.path.basename(array)
		
		out = filetool.read_file('/proc/mdstat')
		if not out:
			raise Exception("Can't get array info from /proc/mdstat.")
		
		for line in out.splitlines():
			if not line.startswith(devname):
				continue
			devices = re.findall('([^\s]+)\[\d+\]', line)
			break
		else:
			devices = []
		return devices	