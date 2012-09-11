

def mdadm(*params, **long_kwds):
	'''
	Example:
	mdadm.mdadm('/dev/md0', wait=True)
	'''
	raise NotImplementedError()


def mode(mode, md_device, *devices, **long_kwds):
	'''
	Example:
	mdadm.mode('create', '/dev/md0', '/dev/loop0', '/dev/loop1', 
				level=0, metadata='default', 
				assume_clean=True, raid_devices=2)
	'''
	raise NotImplementedError()


def mdfind(*devices):
	''' Return md name that contains passed devices '''
	raise NotImplementedError()


def findname():
	''' Return unused md device name '''
	raise NotImplementedError()

	
def detail(md_device):
	'''
	Example:
	>> mdadm.detail('/dev/md0')
	>> {
		'version': '1.2',
		'creation_time': 'Tue Sep 11 23:20:21 2012',
		'raid_level':
		...
		'devices_detail': [{
			'raiddevice': 0,
			'state': 'active sync',
			'device': '/dev/loop0'
		}, ...]
	}
	'''
	raise NotImplementedError()


