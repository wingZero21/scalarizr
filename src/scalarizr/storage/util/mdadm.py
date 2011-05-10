'''
Created on Nov 11, 2010

@author: spike
@author: marat
'''

from scalarizr.util import system2, wait_until, firstmatched, filetool, PopenError
from scalarizr.util.filetool import read_file, write_file

import logging
import os
import re
import time

MDADM_EXEC='/sbin/mdadm'
logger = logging.getLogger(__name__)

class MdadmError(PopenError):
	pass

def system(*popenargs, **kwargs):
	kwargs['logger'] = logger
	kwargs['exc_class'] = MdadmError
	return system2(*popenargs, **kwargs)

class Mdadm:

	def __init__(self):
		if not os.path.exists(MDADM_EXEC):
			raise MdadmError("Make sure you have mdadm package installed.")
		for location in ['/etc ', '/lib']:
			path = os.path.join(location, 'udev/rules.d/85-mdadm.rules')
			if os.path.exists(path):
				
				rule = read_file(path)
				if rule:
					rule = re.sub(re.compile('^([^#])', re.M), '#\\1', rule)
					write_file(path, rule)
					
		self._raid_devices_re  	= re.compile('Raid\s+Devices\s+:\s+(?P<count>\d+)')
		self._total_devices_re 	= re.compile('Total\s+Devices\s+:\s+(?P<count>\d+)')
		self._state_re         	= re.compile('State\s+:\s+(?P<state>.+)')
		self._rebuild_re       	= re.compile('Rebuild\s+Status\s+:\s+(?P<percent>\d+)%')
		self._level_re			= re.compile('Raid Level : (?P<level>.+)')
		
	def create(self, devices, level=1):
		# Validate RAID level
		if not int(level) in (0,1,5):
			raise MdadmError('Unknown RAID level: %s' % level)
		
		# Select RAID device name 
		devname = self._get_free_md_devname()
		
		for device in devices:
			try:
				self._zero_superblock(device)
			except:
				pass
			
		# Create RAID device
		cmd = [MDADM_EXEC, '--create', devname, '--level=%d' % level, '-f', '-e', 'default', '-n', len(devices)]
		cmd.extend(devices)
		system(cmd, error_text='Error occured during raid device creation')
		system2((MDADM_EXEC, '-W', devname), raise_error=False)
		
		return devname

		
	def delete(self, array, zero_superblock=True):
		if not os.path.exists(array):
			raise MdadmError('Device %s does not exist' % array)
		
		# Stop raid
		devices = self.get_array_devices(array)
		system2((MDADM_EXEC, '-W', array), raise_error=False)
		#wait_until(lambda: not self.get_array_info(array)['rebuild_status'])
		cmd = (MDADM_EXEC, '-S', '-f', array)
		try:
			system(cmd, error_text='Error occured during array stopping')
		except (Exception, BaseException), e:
			if not 'Device or resource busy' in str(e):
				raise 
			time.sleep(5)
			system(cmd, error_text='Error occured during array stopping')

		# Delete raid
		try:
			cmd = (MDADM_EXEC, '--remove', '-f', array)
			system(cmd, error_text='Error occured during array deletion')
		except (Exception, BaseException), e:
			if not 'No such file or directory' in str(e):
				raise

		system(('rm', '-f', array))
		
		if zero_superblock:
			for device in devices: 
				self._zero_superblock(device)
			
	def assemble(self, devices):
		md_devname = self._get_free_md_devname()
		cmd = (MDADM_EXEC, '--assemble', md_devname) + tuple(devices)
		system(cmd, error_text="Error occured during array assembling")
		system2((MDADM_EXEC, '-W', md_devname), raise_error=False)
		return md_devname

	def add_disk(self, array, device, grow=True):
		info = self.get_array_info(array)
		if info['level'] == 'raid0':
			raise MdadmError("Can't add devices to raid level 0.")
		
		wait_until(lambda: not self.get_array_info(array)['rebuild_status'], timeout=60)
		cmd = (MDADM_EXEC, '--add', array, device)
		system(cmd, error_text='Error occured during device addition')
		
		if grow:
			array_info = self.get_array_info(array)
			raid_devs = array_info['raid_devices']
			total_devs = array_info['total_devices']
		
			if total_devs > raid_devs:
				cmd = (MDADM_EXEC, '--grow', array, '--raid-devices=%d' % total_devs)
				system(cmd, error_text='Error occured during array "%s" growth')

		system2((MDADM_EXEC, '-W', array), raise_error=False)

	def remove_disk(self, device):
		array = self._get_array_by_device(device)
		wait_until(lambda: not self.get_array_info(array)['rebuild_status'], timeout=60)

		cmd = (MDADM_EXEC, array, '-f', '--fail', device)	
		system(cmd, error_text='Error occured while markin device as failed')
		
		cmd = (MDADM_EXEC, array, '-f', '--remove', device)
		system(cmd, error_text='Error occured during device removal')

	def replace_disk(self, old, new):
		array = self._get_array_by_device(old)
		if self.get_array_info(array)['level'] == 'raid0':
			raise MdadmError("Can't replace disk in raid level 0.")
		self.add_disk(array, new, False)
		self.remove_disk(old)
		system2((MDADM_EXEC, '-W', array), raise_error=False)


	def get_array_info(self, array):
		ret = {}
		cmd = (MDADM_EXEC, '-D', array)
		error_text = 'Error occured while obtaining array %s info' % array
		out = system(cmd, error_text=error_text)[0]
		ret['raid_devices']   = int(re.search(self._raid_devices_re, out).group('count'))
		ret['total_devices']  = int(re.search(self._total_devices_re, out).group('count'))
		ret['state']		  = re.search(self._state_re, out).group('state')
		ret['level']		  = re.search(self._level_re, out).group('level')
		rebuild_res    		  = re.search(self._rebuild_re, out)
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
		cmd = (MDADM_EXEC, '--zero-superblock', '-f', '/dev/%s' % devname)
		system(cmd, error_text='Error occured during zeroing superblock on %s' % device)
		
	def _get_free_md_devname(self):
		return '/dev/md%s' % firstmatched(lambda x: not os.path.exists('/dev/md%s' % x), range(100))

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
	