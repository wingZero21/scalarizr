'''
Created on Nov 25, 2011

@author: marat
'''
from __future__ import with_statement

import threading

from scalarizr import handlers, rpc
from scalarizr import storage as storage_lib


class StorageAPI(object):

	@rpc.service_method
	def create(self, volume_config=None, snapshot_config=None, async=False):
		if volume_config and snapshot_config:
			raise AssertionError('Both volume and snapshot configurations'
			'were passed. Only one configuration expected.')

		if not volume_config and not snapshot_config:
			raise AssertionError('No configuration were passed')

		kw = volume_config or {'snapshot': snapshot_config}
		if async:
			txt = 'Create volume'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						vol = storage_lib.Storage.create(**kw)
				op.ok(data=vol.config())
			threading.Thread(target=block).start()
			return op.id
		
		else:
			vol = storage_lib.Storage.create(**kw)
			return vol.config()


	@rpc.service_method
	def snapshot(self, volume_config, description=None, async=False):
		vol = storage_lib.Storage.create(volume_config)
		if async:
			txt = 'Create snapshot'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						snap = vol.snapshot(description)
				op.ok(data=snap.config())
			threading.Thread(target=block).start()
			return op.id
			
		else:
			snap = vol.snapshot(description)
			return snap.config()


	@rpc.service_method
	def detach(self, volume_config, async=False):
		assert volume_config.get('id'), 'volume_config[id] is empty'
		vol = storage_lib.Storage.create(volume_config)
		if async:
			txt = 'Detach volume'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						vol.detach()
				op.ok(data=vol.config())
			threading.Thread(target=block).start()
			return op.id
			
		else:
			vol.detach()
			return vol.config()


	@rpc.service_method
	def destroy(self, volume_config, destroy_disks=False, async=False):
		assert volume_config.get('id'), 'volume_config[id] is empty'
		
		vol = storage_lib.Storage.create(volume_config)
		if async:
			txt = 'Destroy volume'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						vol.destroy(remove_disks=destroy_disks)
				op.ok()
			threading.Thread(target=block).start()
			return op.id
		
		else:
			vol.destroy(remove_disks=destroy_disks)


	@rpc.service_method
	def replace_raid_disk(self, volume_config, target_disk_device, replacement_disk_config, async=False):
		assert volume_config.get('type') == 'raid', 'Configuration type is not raid'
		raid = storage_lib.Storage.create(**volume_config)

		def replace_disk_block():
			target = filter(lambda x: x.device == target_disk_device, raid.disks)
			if not target:
				raise Exception("Can't find failed disk in array")
			target = target[0]
			new_drive = storage_lib.Storage.create(**replacement_disk_config)
	
			try:
				raid.replace_disk(target, new_drive)
			except:
				if not replacement_disk_config.get('id'):
					# Disk was created during replacement. Deleting
					new_drive.destroy()
			else:
				try:
					target.destroy()
				except:
					pass
				return raid.config()

		if async:
			txt = 'Replace RAID disk'
			op = handlers.operation(name=txt)
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

	@rpc.service_method
	def status(self, volume_config):
		vol = storage_lib.Storage.create(volume_config)
		return vol.status()


