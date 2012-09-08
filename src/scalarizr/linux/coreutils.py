'''
Created on Aug 28, 2012

@author: marat
'''

from scalarizr import linux


def sync():
	return linux.system(('/bin/sync', ))


def df():
	raise NotImplementedError()


def dd(**kwds):
	raise NotImplementedError()


def sfdisk():
	raise NotImplementedError()


def modprobe(module_name, **long_kwds):
	return linux.system(linux.build_cmd_args(
				executable='/sbin/modprobe', 
				long=long_kwds,
				params=[module_name]), 
			error_text='Kernel module %s is not available' % module_name)
	

def dmsetup(action, *params, **long_kwds):
	return linux.system(linux.build_cmd_args(
			executable='/sbin/dmsetup', 
			short=[action], 
			long=long_kwds, 
			params=params))	
	
	
def losetup(*args, **long_kwds):
	return linux.system(linux.build_cmd_args(
				executable='/sbin/losetup', 
				long=long_kwds, 
				params=args))

	
def losetup_all():
	'''
	Alias to losetup --all with parsing output into python dict.
	Example:
		{'/dev/loop0': '/mnt/loop0',
 		'/dev/loop1': '/mnt/loop1',
 		'/dev/loop2': '/mnt/loop2'}
	'''
	ret = dict()
	out = losetup(all=True)[0].strip()
	for line in out.splitlines():
		cols = line.split()
		ret[cols[0][:-1]] = cols[-1][1:-1]
	return ret

	
def truncate(filename, size):
	# truncate with dd or /usr/bin/truncate
	raise NotImplementedError()
	
