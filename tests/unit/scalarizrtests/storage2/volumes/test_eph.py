

volume = {
	'snap_backend': {
		'path': 's3://scalr-3414-ap-northeast-1/data-bundles/5071/percona'
	},
	'vg': 'percona',
	'fs_created': True,
	'fstype': 'xfs',
	'lvm_group_cfg': None,
	'mpoint': '/mnt/dbstorage',
	'device': '/dev/mapper/percona-data',
	'disk': {
		'fs_created': None,
		'fstype': None,
		'mpoint': None,
		'device': '/dev/sda2',
		'type': 'base',
		'id': 'base-vol-1fdb2bf0'
	},
	'type': 'eph',
	'id': 'eph-vol-2a3bd1c8',
	'size': '80%'
}

snapshot = {
	'snap_backend>' : {
		'path': 's3://scalr-3414-ap-northeast-1/data-bundles/5071/percona'
	},
	'vg': 'percona',
	'description': 'MySQL data bundle (farm: 5071 role: percona64-centos6)',
	'snap_strategy': 'data',
	'fs_created': True,
	'fstype': 'xfs',
	'lvm_group_cfg': None,
	'mpoint': '/mnt/dbstorage',
	'device': '/dev/mapper/percona-data',
	'path': 's3://scalr-3414-ap-northeast-1/data-bundles/5071/percona/eph-snap-fe9ac9ce.manifest.ini',
	'disk': {
		'fs_created': None,
		'fstype': None,
		'mpoint': None,
		'device': '/dev/sda2',
		'type': 'base',
		'id': 'base-vol-1fdb2bf0'
	},
	'type': 'eph',
	'id': 'eph-snap-fe9ac9ce'			
}
