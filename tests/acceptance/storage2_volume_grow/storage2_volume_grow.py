__author__ = 'Nick Demyanchuk'

import os
import logging
import tempfile

from scalarizr import storage2
from scalarizr import linux
from scalarizr.linux import coreutils, mdadm


from lettuce import world, step, after

logging.basicConfig()
logging.getLogger().setLevel(logging.INFO)
LOG = logging.getLogger(__name__)


def parse_config(raw_cfg, dot_notation=False):
	cfg = {}
	cfg_pairs = raw_cfg.split(',')
	for pair in cfg_pairs:
		k,v = pair.strip().split('=')
		if dot_notation and '.' in k:
			k, sub_k = k.split('.', 1)
			v = {sub_k: v}
		if k not in cfg:
			cfg[k] = v
		else:
			cfg[k].update(v)
	return cfg


def get_device_size(device):
	o, e, code = linux.system(['df', device])
	return int(o.split('\n')[1].split()[1])


def get_file_md5sum(file_path):
	o, e, code = linux.system(['md5sum', file_path])
	return o.strip().split()[0]


@step('I have (.+?) volume with (.+?) settings')
def prepare_volume(step, kind, raw_cfg):
	# Prepare config
	cfg = parse_config(raw_cfg)

	world.tmp_mount_dir = tempfile.mkdtemp()
	cfg['mpoint'] = world.tmp_mount_dir
	cfg['type'] = kind

	vol = world.volume = storage2.volume(**cfg)
	vol.ensure(mount=True, mkfs=True)

	world.size_before = get_device_size(vol.device)


@step('I create some file on it')
def create_some_file(step):
	world.fpath = os.path.join(world.tmp_mount_dir, 'myfile')

	dd_kwargs = {'if': '/dev/urandom', 'of': world.fpath, 'bs': '1M', 'count': 1}
	coreutils.dd(**dd_kwargs)

	world.file_md5 = get_file_md5sum(world.fpath)


@step('I grow volume with (.+)')
def grow_volume(step, raw_cfg):
	cfg = parse_config(raw_cfg)
	world.bigger_vol = world.volume.grow(**cfg)


@step('I see that volume size increased properly')
def check_space_increased(step):
	new_size = get_device_size(world.bigger_vol.device)
	assert world.size_before < new_size, "New size is not bigger than old"


@step('I still see my precious file')
def check_integrity(step):
	new_md5 = get_file_md5sum(world.fpath)
	assert new_md5 == world.file_md5, 'Integrity check failed'



@step('I have raid (.+)')
def prepare_raid(step, raw_config):
	config = parse_config(raw_config, dot_notation=True)

	disk_count = config.pop('disks')
	disk_config = config.pop('disk')
	config['disks'] = [disk_config] * int(disk_count)

	world.tmp_mount_dir = tempfile.mkdtemp()
	config['mpoint'] = world.tmp_mount_dir
	config['type'] = 'raid'
	world.initial_raid_cfg = config

	vol = world.volume = storage2.volume(**config)
	vol.ensure(mount=True, mkfs=True)

	world.size_before = get_device_size(vol.device)

@step('I (.+?) raid volume')
def grow_raid_vol(step, raw_cfg):
	world.grow_cfg = cfg = parse_config(raw_cfg, dot_notation=True)
	world.bigger_vol = world.volume.grow(**cfg)


@step('I see that raid grew properly')
def raid_grew_properly(step):
	new_size = get_device_size(world.bigger_vol.device)

	each_volume_grew = world.grow_cfg.get('foreach')
	if int(world.bigger_vol.level) == 5 or each_volume_grew:
		assert world.size_before < new_size, "New size is not bigger than old"
		LOG.info('Size grew on %s' % (new_size - world.size_before))

	mdinfo = mdadm.detail(world.bigger_vol.raid_pv)
	disk_count_should_be = int(world.grow_cfg.get('len') or len(world.initial_raid_cfg['disks']))
	assert mdinfo['raid_devices'] == disk_count_should_be, "Disk count doesn't match"


@after.outline
def teardown_scenario(scenario, order, outline, reasons_to_fail):
	for vol in ('volume', 'bigger_vol'):
		if hasattr(world, vol):
			LOG.info('Removing %s' % vol)
			try:
				v = getattr(world, vol)
				v.destroy(force=True, remove_disks=True)
			except:
				pass

			world.spew(vol)

	if hasattr(world, 'tmp_mount_dir'):
		try:
			os.rmdir(world.tmp_mount_dir)
		except:
			pass
