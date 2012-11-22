__author__ = 'Nick Demyanchuk'

import os
import logging
import tempfile

from scalarizr import storage2
from scalarizr import linux
from scalarizr.linux import coreutils


from lettuce import world, step, after


def parse_config(raw_cfg):
	cfg = {}
	cfg_pairs = raw_cfg.split(',')
	for pair in cfg_pairs:
		k,v = pair.strip().split('=')
		cfg[k] = v
	return cfg


def get_device_size(device):
	o, e, code = linux.system(('df', device))
	return int(o.split('\n')[1].split()[1])


def get_file_md5sum(file_path):
	o, e, code = linux.system(('md5sum', file_path))
	return o.strip().split()[0]


@step('I have (\w+) volume with "([^"])" settings')
def prepare_volume(type, raw_cfg):
	# Prepare config
	cfg = parse_config(raw_cfg)

	world.tmp_mount_dir = tempfile.mkdtemp()
	cfg['mpoint'] = world.tmp_mount_dir

	vol = world.volume = storage2.volume(**cfg)
	vol.ensure(mount=True, mkfs=True)

	world.size_before = get_device_size(vol.device)



@step('I create some file on it')
def create_some_file():
	world.fpath = os.path.join(world.tmp_mount_dir, 'myfile')

	dd_kwargs = {'if': '/dev/urandom', 'of': world.fpath, 'bs': '1M', 'count': 10}
	coreutils.dd(**dd_kwargs)

	world.file_md5 = get_file_md5sum(world.fpath)


@step('I grow volume with "([^"])"')
def grow_volume(raw_cfg):
	cfg = parse_config(raw_cfg)
	bigger_vol = world.bigger_vol = world.volume.grow(**cfg)


@step('I see that volume size increased properly')
def check_space_increased():
	new_size = get_device_size(world.bigger_vol.device)
	assert world.size_before < new_size, "New size is not bigger than old"


@step('I still se my precious file')
def check_integrity():
	new_md5 = get_file_md5sum(world.fpath)
	assert new_md5 == world.file_md5, 'Integrity check failed'


@after.each_scenario
def teardown_scenario():
	for vol in ('volume', 'bigger_vol'):
		if world.hasattr(vol):
			try:
				v = getattr(world, vol)
				v(force=True, remove_disks=True)
			except:
				pass

			world.spew(vol)



