'''
Created on Nov 25, 2011

@author: marat

Pluggable API to get system information similar to SNMP, Facter(puppet), Ohai(chef)
'''

import os
import logging
import sys
import re

from scalarizr import rpc
from scalarizr.util import system2

LOG = logging.getLogger(__name__)

class SysInfoAPI(object):
	
	def __init__(self, diskstats=None, cpuinfo=None):
		self.__diskstats = diskstats if diskstats else None
		self.__cpuinfo = cpuinfo if cpuinfo else None

	def add_extension(self, extension):
		'''
		@param extension: obj with some public attribute for extension the self API object. 
		@note: add each callable public attribute into extension to self SysInfoAPI object.'''
		for name in dir(extension):
			attr = getattr(extension, name)
			try:
				getattr(self, name)
				LOG.warn('scalarizr.api.sysinfo.SysInfoAPI.add_extension: Duplicate\
					API attribute %s. The old attribute replacing with new.' % name)
			except:	pass
			if not name.startswith('_') and callable(attr):
				setattr(self, name, attr)


	@rpc.service_method
	def fqdn(self, fqdn=None):
		'''	@rtype: str
			@note: get or set system hostname'''
		if fqdn:
			with open('/etc/hostname', 'w+') as fp:
				hostname = fp.readline().strip()
				if hostname != fqdn:
					fp.write(fqdn)
					return fqdn
				return hostname
		else:
			with open('/etc/hostname') as fp:
				return fp.readline().strip()


	@rpc.service_method
	def block_devices(self):
		'''	@rtype: list
			@return: ['sda1', 'ram1']
			@note: return list of all block devices'''
		if not self.__diskstats:
			with open('/proc/diskstats') as fp:
				self.__diskstats = fp.readlines()
		devicelist = []
		for value in self.__diskstats:
			devicelist.append(value.split()[2])
		LOG.debug('%s', devicelist)
		return devicelist


	@rpc.service_method
	def uname(self):
		'''
		@rtype: dict
		@return: {	'kernel_name': 'Linux',
					'kernel_release': '2.6.41.10-3.fc15.x86_64',
					'kernel_version': '#1 SMP Mon Jan 23 15:46:37 UTC 2012',
					'nodename': 'marat.office.webta',			
					'machine': 'x86_64',
					'processor': 'x86_64',
					'hardware_platform': 'x86_64'}'''
		#/usr/bin/pyversions
		#TODO: better read this from file
		#/etc/issue
		return {
			'kernel_name': system2(('uname', '-s'))[0].strip(),
			'kernel_release': system2(('uname', '-r'))[0].strip(),
			'kernel_version': system2(('uname', '-v'))[0].strip(),
			'nodename': system2(('uname', '-n'))[0].strip(),
			'machine': system2(('uname', '-m'))[0].strip(),
			'processor': system2(('uname', '-p'))[0].strip(),
			'hardware_platform': system2(('uname', '-i'))[0].strip()}


	@rpc.service_method
	def dist(self):
		'''
		@rtype: dict
		@return: {	'id': 'Fedora',
					'release': '15',
					'codename': 'Lovelock',
					'description': 'Fedora release 15 (Lovelock)'}
		#TODO: now return some as: 
			{	'Codename': 'oneiric',
				'Description': 'Ubuntu 11.10',
				'Distributor ID': 'Ubuntu',
				'Release': '11.10'}'''
		out, err, rcode = system2(('lsb_release', '-a'))
		if rcode == 0:
			self.__dist = out.split('\n')
		else:
			raise Exception, err
		res = {}
		for ln in self.__dist:
			if ':' in ln:
				(key,value) = map(lambda x: x.strip(), ln.split(':'))
				res.update({key: value})
		return res


	@rpc.service_method
	def pythons(self):
		''
		def _check_path(path):
			py_name = os.path.basename(path)
			if py_name.startswith('python') and len(py_name) >= 6:
				#filtering name as `python2.7-config` and other analogy
				if not re.search('[^ \-.+0-9]', py_name[6:]):
					return True

		#add python path to paths if we want to find python in it
		paths = ['/usr/bin/', '/usr/local/bin/']
		res = []
		for path in paths:
			(out, err, rc) = system2(('find', path, '-name', 'python*'))
			if rc == 0:
				if '\n' in out:
					out = out.split('\n')
					for lp in out:
						if _check_path(lp):
							res.append(lp)
				elif out:
					if _check_path(out.strip()):
							res.append(out.strip())
			else:
				LOG.debug('scalarizr.api.sysinfo.SysInfoAPI.pythons: error find python\
					at path %s, details: `%s`', path, err)
		#check full correct version
		LOG.debug('Variant of python paths: `%s`. They checking now.', res)
		result = []
		for pypath in res:
			(out, err, rc) = system2((pypath, '-V'))
			if rc == 0:
				result.append((out or err).strip())
			else: 
				LOG.debug('Can`t explore `%s -V`, details: %s', pypath, err or out)
		return map(lambda x: x[6:].strip(), list(set(result)))


	@rpc.service_method
	def cpu_info(self):
		''' @rtype: list
			@return: [{processor:0, vendor_id:GenuineIntel,...}, ...]
			@note: return list with cpu cores information 
		'''
		# @see /proc/cpuinfo
		if not self.__cpuinfo:
			with open('/proc/cpuinfo') as fp:
				self.__cpuinfo = fp.readlines()
		res = []
		index = 0
		while index < len(self.__cpuinfo):
			core = {}
			while index < len(self.__cpuinfo):
				if ':' in self.__cpuinfo[index]:
					tmp = map(lambda x: x.strip(), self.__cpuinfo[index].split(':'))
					(key, value) = list(tmp) if len(list(tmp)) == 2 else (tmp, None)
					if key not in core.keys():
						core.update({key:value})
					else:
						break
				index += 1
			res.append(core)
		return res


	@rpc.service_method
	def load_average(self):
		return os.getloadavg()


	@rpc.service_method
	def disk_stats(self):
		'''
		Disks I/O statistics
		@rtype: [{
			<device>: Linux device name
			<read>: {
				<num>: total number of reads completed successfully
				<sectors>: total number of sectors read successfully
				<bytes>: total number of bytes read successfully
			}
			<write>: {
				<num>: total number of writes completed successfully
				<sectors>: total number of sectors written successfully
				<bytes>: total number of bytes written successfully
			}
		}, ...]
		http://www.kernel.org/doc/Documentation/iostats.txt
		'''
		if not self.__diskstats:
			with open('/proc/diskstats') as fp:
				self.__diskstats = fp.readlines()
		devicelist = []
		for value in self.__diskstats:
			params = value.split()[2:]
			device = params[0]
			for i in range(1, len(params)-1):
				params[i] = int(params[i])
			if len(params) == 12:
				read = {'num': params[1], 'sectors': params[3], 'bytes': params[3]*512}
				write = {'num': params[5], 'sectors': params[7], 'bytes': params[7]*512}
			elif len(params) == 5:
				read = {'num': params[1], 'sectors': params[2], 'bytes': params[2]*512}
				write = {'num': params[3], 'sectors': params[4], 'bytes': params[4]*512}
			else:
				raise Exception, 'scalarizr.api.sysinfo.disk_stats: number of column in\
					/proc/diskstats is unexpected. Count of column = %s' % (len(params)+2)
			devicelist.append({'device': device, 'write': write, 'read': read})
		return devicelist


	@rpc.service_method
	def net_stats(self):
		'''
		Network I/O statistics
		@rtype: [{
			<iface>: Network interface name
			<receive>: {
				<bytes>: total received bytes
				<packets>: total received packets
				<errors>: total receive errors
			}
			<transmit>: {
				<bytes>: total transmitted bytes
				<packets>: total transmitted packets
				<errors>: total transmit errors
			}
		}, ...]
		'''
		with open('/proc/net/dev', "r") as fp:
			list = fp.readlines()
		res = []
		for row in list:
			row = row.split(':')
			iface = row.pop(0).strip()
			columns = map(lambda x: x.strip(), row.split())
			res.append({'iface': iface,
						'receive': {'bytes': columns[0], 'packets': columns[1], 'errors': columns[2]},
						'transmit': {'bytes': columns[8], 'packets': columns[9], 'errors': columns[10]},
						})
		return res
		'''	
		# __IF-MIB.py
		
		return [{
			'iface': 'lo', 
			'receive': {'bytes': 14914843, 'packets': 116750, 'errors': 0}, 
			'transmit': {'bytes': 14914843, 'packets': 116750, 'errors': 0}
		}, {
			'iface': 'p1p1', 
			'receive': {'bytes': 6191422351, 'packets': 23714651, 'errors': 0}, 
			'transmit': {'bytes': 14914843, 'packets': 116750, 'errors': 0}
		}]'''
