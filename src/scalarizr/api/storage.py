'''
Created on Nov 25, 2011

@author: marat
'''

import threading
from uuid import uuid4

from scalarizr.handlers import operation
from scalarizr.rpc import service_method
from scalarizr.storage import Storage


class StorageAPI(object):

	@service_method
	def create(self, storage_cfg=None, snapshot_cfg=None):
		if storage_cfg and snapshot_cfg:
			raise Exception('Both storage and snapshot configurations'
			'were passed. Only one configuration expected.')

		if not storage_cfg and not snapshot_cfg:
			raise Exception('No configuration were passed')

		kw = storage_cfg or {'snapshot': snapshot_cfg}
		vol = Storage.create(**kw)
		return vol.config()


	@service_method
	def snapshot(self, storage_cfg):
		vol = Storage.create(storage_cfg)
		snap = vol.snapshot()
		return snap.config()


	@service_method
	def detach(self, storage_cfg):
		# TODO: if no id in storage_cfg - do nothing
		vol = Storage.create(storage_cfg)
		vol.detach()


	@service_method
	def destroy(self, storage_cfg, remove_disks=False):
		# TODO: if no id in storage_cfg - do nothing
		vol = Storage.create(storage_cfg)
		vol.destroy(remove_disks=remove_disks)


	@service_method
	def replace_raid_disk(self, storage_cfg, target_disk_device, replacement_disk_cfg):
		assert storage_cfg.get('type') == 'raid', 'Configuration type is not raid'
		raid = Storage.create(**storage_cfg)

		failed = filter(lambda x: x.device == target_disk_device, raid.disks)
		if not failed:
			raise Exception("Can't find failed disk in array")
		failed = failed[0]
		new_drive = Storage.create(**replacement_disk_cfg)
		try:
			raid.replace_disk(failed, new_drive)
		except:
			pass
		else:
			failed.destroy()
			return raid.config()


	@service_method
	def status(self, storage_cfg):
		vol_id = storage_cfg.get('id')
		vol = Storage.get(vol_id)
		return vol.status()


