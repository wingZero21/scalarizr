'''
Created on Nov 25, 2011

@author: marat
'''

import threading

from scalarizr.handlers import operation
from scalarizr.rpc import service_method
from scalarizr.storage import Storage


class StorageAPI(object):

	@service_method
	def create(self, storage_config=None, snapshot_config=None, async=False):
		if storage_config and snapshot_config:
			raise AssertionError('Both storage and snapshot configurations'
			'were passed. Only one configuration expected.')

		if not storage_config and not snapshot_config:
			raise AssertionError('No configuration were passed')

		kw = storage_config or {'snapshot': snapshot_config}
		if async:
			txt = 'Create storage'
			op = operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						vol = Storage.create(**kw)
				op.ok(data=vol.config())
			threading.Thread(target=block).start()
			return op.id
		
		else:
			vol = Storage.create(**kw)
			return vol.config()


	@service_method
	def snapshot(self, storage_config, async=False):
		vol = Storage.create(storage_config)
		if async:
			txt = 'Create storage snapshot'
			op = operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						snap = vol.snapshot()
				op.ok(data=snap.config())
			threading.Thread(target=block).start()
			return op.id
			
		else:
			snap = vol.snapshot()
			return snap.config()


	@service_method
	def detach(self, storage_config, async=False):
		assert storage_config.get('id'), 'storage_config[id] is empty'
		vol = Storage.create(storage_config)
		if async:
			txt = 'Detach storage'
			op = operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						vol.detach()
				op.ok()
			threading.Thread(target=block).start()
			return op.id
			
		else:
			vol.detach()


	@service_method
	def destroy(self, storage_config, remove_disks=False, async=False):
		assert storage_config.get('id'), 'storage_config[id] is empty'
		
		vol = Storage.create(storage_config)
		if async:
			txt = 'Destroy storage'
			op = operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						vol.destroy(remove_disks=remove_disks)
				op.ok()
			threading.Thread(target=block).start()
			return op.id
		
		else:
			vol.destroy(remove_disks=remove_disks)


	@service_method
	def replace_raid_disk(self, storage_config, target_disk_device, replacement_disk_config, async=False):
		assert storage_config.get('type') == 'raid', 'Configuration type is not raid'
		raid = Storage.create(**storage_config)

		def replace_disk_block():
			target = filter(lambda x: x.device == target_disk_device, raid.disks)
			if not target:
				raise Exception("Can't find failed disk in array")
			target = target[0]
			new_drive = Storage.create(**replacement_disk_config)
	
			try:
				raid.replace_disk(target, new_drive)
			except:
				if not replacement_disk_config.get('id'):
					# Disk was created during replacement. Deleting
					new_drive.destroy()
			else:
				target.destroy()
				return raid.config()

		if async:
			txt = 'Replace RAID disk'
			op = operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						raid_config = replace_disk_block()
				op.ok(data=raid_config)
			threading.Thread(target=block).start()
			return op.id

		else:
			return replace_disk_block()

	@service_method
	def status(self, storage_config):
		vol_id = storage_config.get('id')
		vol = Storage.get(vol_id)
		return vol.status()


