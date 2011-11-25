'''
Created on Nov 25, 2011

@author: marat

Pluggable API to get system information similar to SNMP, Facter(puppet), Ohai(chef)
'''

class SysInfoAPI(object):

	def query(self, expr):
		pass