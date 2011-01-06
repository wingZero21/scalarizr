'''
Created on Jan 6, 2011

@author: marat
'''

from .. import system

LOSETUP_EXEC = '/sbin/losetup'

def mkloop(filename, size=None, quick=False):
	''' Create loop device '''
	if size:
		cmd = ['dd', 'if=/dev/zero', 'of=%s' % filename, 'bs=1M']
		if quick:
			cmd.extend(['seek=%d' % (size - 1,), 'count=1'])
		else:
			cmd.extend(['count=%d' % size])
		system(cmd)
	devname = system(('/sbin/losetup', '-f'))[0].strip()
	system((LOSETUP_EXEC, devname, filename))
	return devname

def listloop():
	ret = {}
	loop_lines = system((LOSETUP_EXEC, '-a'))[0].strip().splitlines()
	for loop_line in loop_lines:
		words = loop_line.split()
		ret[words[0][:-1]] = words[-1][1:-1]
	return ret
	

def rmloop(device):
	system((LOSETUP_EXEC, '-d', device))