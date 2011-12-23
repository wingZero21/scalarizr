'''
Created on Nov 25, 2011

@author: marat

Pluggable API to get system information similar to SNMP, Facter(puppet), Ohai(chef)
'''

import os

from scalarizr import rpc


class SysInfoAPI(object):

	def add_extension(self, extension):
		# @todo: export each callable public attribute into self 
		raise NotImplemented()
	
	@rpc.service_method
	def fqdn(self, fqdn=None):
		# get or set hostname
		raise NotImplemented()

	@rpc.service_method
	def block_devices(self):
		# __UCD-DISKIO-MIB.py
		raise NotImplemented()
		return ['sda1', 'loop0']
	
	@rpc.service_method
	def uname(self):
		raise NotImplemented()
	
	@rpc.service_method
	def lsb_release(self):
		raise NotImplemented()
	
	@rpc.service_method
	def pythons(self):
		raise NotImplemented()

	@rpc.service_method
	def cpu_info(self):
		raise NotImplemented()
	
	@rpc.service_method
	def load_average(self):
		return os.getloadavg()

	@rpc.service_method
	def disk_stats(self):
		# later
		pass
	
	@rpc.service_method
	def bandwidth(self):
		# __IF-MIB.py
		pass
	