'''
Created on Aug 1, 2012

@author: dmitry
'''

'''
Created on Nov 25, 2011

@author: marat
'''


from __future__ import with_statement

import time
import logging
import threading
from scalarizr.bus import bus
from scalarizr import handlers, rpc
from scalarizr.services import redis as redis_service
from scalarizr.handlers import redis as redis_handler
from scalarizr import storage as storage_lib

BEHAVIOUR = CNF_SECTION = redis_handler.CNF_SECTION
OPT_REPLICATION_MASTER = redis_handler.OPT_REPLICATION_MASTER
OPT_PERSISTENCE_TYPE = redis_handler.OPT_PERSISTENCE_TYPE
STORAGE_PATH = redis_handler.STORAGE_PATH


LOG = logging.getLogger(__name__)


class RedisAPI(object):

	_cnf = None
	_queryenv = None
	
	def __init__(self):
		self._cnf = bus.cnf
		self._queryenv = bus.queryenv_service


	@property
	def is_replication_master(self):
		value = 0
		if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_REPLICATION_MASTER):
			value = self._cnf.rawini.get(CNF_SECTION, OPT_REPLICATION_MASTER)
		return True if int(value) else False
	
	
	@property
	def persistence_type(self):
		value = 'snapshotting'
		if self._cnf.rawini.has_section(CNF_SECTION) and self._cnf.rawini.has_option(CNF_SECTION, OPT_PERSISTENCE_TYPE):
			value = self._cnf.rawini.get(CNF_SECTION, OPT_PERSISTENCE_TYPE)
		return value


	def get_primary_ip(self):
		master_host = None
		LOG.info("Requesting master server")
		while not master_host:
			try:
				master_host = list(host 
					for host in self._queryenv.list_roles(self._role_name)[0].hosts 
					if host.replication_master)[0]
			except IndexError:
				LOG.debug("QueryEnv respond with no %s master. " % BEHAVIOUR + 
						"Waiting %d seconds before the next attempt" % 5)
				time.sleep(5)
		host = master_host.internal_ip or master_host.external_ip
		return host


	def _start_processes(self, ports=[], passwords=[]):
		redis_instances = redis_service.RedisInstances(self.is_replication_master, self.persistence_type)
		redis_instances.init_processes(ports, passwords)
		if self.is_replication_master:
			res = redis_instances.init_as_masters(mpoint=STORAGE_PATH)
		else:
			primary_ip = self.get_primary_ip()
			assert primary_ip is not None
			res = redis_instances.init_as_slaves(mpoint=STORAGE_PATH, primary_ip=primary_ip)
		return res
	
	
	@rpc.service_method
	def launch_processes(self, num=None, ports=None, passwords=None, async=False):	
		if ports and passwords and len(ports) != len(passwords):
			raise AssertionError('Number of ports must be equal to number of passwords')
		if num and ports and num != len(ports):
				raise AssertionError('When ports range is passed its length must be equal to num parameter')
		if not self.is_replication_master:
			if not passwords or not ports:
				raise AssertionError('ports and passwords are compulsory to launch processes on redis slave')
		
		if async:
			txt = 'Start redis processes'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						result = self._start_processes(ports, passwords)
				op.ok(data=dict(ports=result[0], passwords=result[1]))
			threading.Thread(target=block).start()
			return op.id
		else:
			result = self._start_processes(ports, passwords)
			return dict(ports=result[0], passwords=result[1])

		
	@rpc.service_method
	def shutdown_processes(self, ports, remove_data=False, async=False):
		redis_instances = redis_service.RedisInstances()
		redis_instances.init_processes(ports)
		if async:
			txt = 'Shutting down redis processes'
			op = handlers.operation(name=txt)
			def block():
				op.define()
				with op.phase(txt):
					with op.step(txt):
						redis_instances.kill_processes(ports, remove_data)
				op.ok(data={})
			threading.Thread(target=block).start()
			return op.id
		else:
			redis_instances.kill_processes(ports, remove_data)
			return {}
		
				
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
				raise
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
