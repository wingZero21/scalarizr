'''
Created on Sep 10, 2010

@author: marat
'''

__all__ = ('all_installed', 'software_info', 'explore', 'whereis')

def all_installed():
	pass

def software_info(name):
	pass

def explore(name, lookup_fn):
	pass

def whereis(name):
	'''
	Search executable in /bin /sbin /usr/bin /usr/sbin /usr/libexec /usr/local/bin /usr/local/sbin
	@rtype: tuple
	'''
	pass

class SoftwareInfo:
	name = None
	
	version = None
	'''
	@param version: tuple(major, minor, bugfix) 
	'''
	
	version_string = None