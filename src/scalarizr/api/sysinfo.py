'''
Created on Nov 25, 2011

@author: marat

Pluggable API to get system information similar to SNMP, Facter(puppet), Ohai(chef)
'''

import os
import logging

from scalarizr import rpc

LOG = logging.getLogger(__name__)

class SysInfoAPI(object):
	
	def __init__(self, diskstats=None):
		self.__diskstats = diskstats if diskstats else None

	
	def add_extension(self, extension):
		# @todo: export each callable public attribute into self. 
		# raise on duplicate 
		raise NotImplemented()

	@rpc.service_method
	def fqdn(self, fqdn=None):
		# get or set hostname
		# @see: man hostname
		raise NotImplemented()

	@rpc.service_method
	def block_devices(self):
		#TODO: __UCD-DISKIO-MIB.py
		if not self.__diskstats:
			with open('/proc/diskstats') as fp:
				self.__diskstats = fp.readlines()

		devicelist = []
		for index in range(len(self.__diskstats)):
			values = self.__diskstats[index].split()
			is_partition = len(values) == 7
			devicelist.append(values[2])
		LOG.debug('%s', devicelist)
		
		raise NotImplemented()
	
		return ['sda1', 'loop0']
	
	@rpc.service_method
	def uname(self):
		raise NotImplemented()
		return {
			'kernel_name': 'Linux',
			'kernel_release': '2.6.41.10-3.fc15.x86_64',
			'kernel_version': '#1 SMP Mon Jan 23 15:46:37 UTC 2012',
			'nodename': 'marat.office.webta',			
			'machine': 'x86_64',
			'processor': 'x86_64',
			'hardware_platform': 'x86_64'
		}
	
	@rpc.service_method
	def dist(self):
		raise NotImplemented()
		return {
			'id': 'Fedora', 
			'release': '15', 
			'codename': 'Lovelock', 
			'description': 'Fedora release 15 (Lovelock)'
		}
	
	@rpc.service_method
	def pythons(self):
		raise NotImplemented()
		return ('2.6', '2.7', '3.2')

	@rpc.service_method
	def cpu_info(self):
		raise NotImplemented()
		# @see /proc/cpuinfo


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
		'''
		# __UCD-DISKIO-MIB.py
		# http://www.kernel.org/doc/Documentation/iostats.txt
		raise NotImplemented()
		return [{
			'device': 'sda1',
			'read': {'num': 24913, 'sectors': 501074, 'bytes': 256549888},
			'write': {'num': 2009937, 'sectors': 54012056, 'bytes': 27654172672}
		}, {
			'device': 'sdb',
			# ...
		}]
	
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
		# __IF-MIB.py
		raise NotImplemented()
		return [{
			'iface': 'lo', 
			'receive': {'bytes': 14914843, 'packets': 116750, 'errors': 0}, 
			'transmit': {'bytes': 14914843, 'packets': 116750, 'errors': 0}
		}, {
			'iface': 'p1p1', 
			'receive': {'bytes': 6191422351, 'packets': 23714651, 'errors': 0}, 
			'transmit': {'bytes': 14914843, 'packets': 116750, 'errors': 0}
		}]
