__author__ = 'Nick Demyanchuk'

import os
import sys
import uuid
import datetime

from scalarizr import storage2
from scalarizr.node import __node__
from scalarizr.storage2.volumes import base
from scalarizr.storage2.util import gce as gce_util


class GcePersistentVolume(base.Volume):
	'''
	def _get_device_name(self):
		return 'google-%s' % (self.alias or self.name)
	'''


	def __init__(self, name=None, link=None, size=None, zone=None, **kwargs):
		name = name or 'scalr-disk-%s' % uuid.uuid4().hex[:8]
		super(GcePersistentVolume, self).__init__(name=name, link=link,
												  size=size, zone=zone,
												  **kwargs)


	def _ensure(self):

		garbage_can = []
		zone = os.path.basename(__node__['gce']['zone'])
		connection = __node__['gce']['compute_connection']
		project_id = __node__['gce']['project_id']
		server_name = __node__['server_id']

		try:
			create = False
			if not self.link:
				# Disk does not exist, create it first
				create_request_body = dict(name=self.name, sizeGb=self.size)
				if self.snap:
					self.snap = storage2.snapshot(self.snap)
					create_request_body['sourceSnapshot'] = self.snap.link
				create = True
			else:
				self._check_attr('zone')
				# TODO: update zone from disk resource of compute engine
				if self.zone != zone:
					# Volume is in different zone, snapshot it,
					# create new volume from this snapshot, then attach
					temp_snap = self.snapshot('volume')
					garbage_can.append(temp_snap)
					# TODO: generate new name
					create_request_body = dict(name=self.name,
											   sizeGb=self.size,
											   sourceSnapshot=temp_snap.link)
					create = True

			attach = False
			if create:
				op = connection.disks().insert(project=project_id,
											   zone=zone,
											   body=create_request_body).execute()
				gce_util.wait_for_operation_to_complete(connection, project_id, op['name'], zone)
				disk_name = create_request_body['name']
				disk_dict = connection.disks().get(disk=disk_name,
												   project=project_id,
												   zone=zone).execute()
				self.id = disk_dict['id']
				self.link = disk_dict['selfLink']
				self.zone = zone
				attach = True

			else:
				attachment_inf = self._attachment_info(connection)
				if attachment_inf:
					disk_devicename = attachment_inf['deviceName']
				else:
					attach = True

			if attach:
				op = connection.instances().attachDisk(
							instance=server_name,
							project=project_id,
							zone=zone,
							body=dict(
									deviceName=self.name,
									source=self.link,
									mode="READ_WRITE",
									type="PERSISTENT"
							)).execute()
				gce_util.wait_for_operation_to_complete(connection, project_id, op['name'], zone=zone)
				disk_devicename = self.name

			device = gce_util.devicename_to_device(disk_devicename)
			if not device:
				raise storage2.StorageError("Disk should be attached, but corresponding"
											" device not found in system")
			self.device = device

		finally:
			# Perform cleanup
			for garbage in garbage_can:
				try:
					garbage.destroy(force=True)
				except:
					pass


	def _attachment_info(self, con):
		zone = __node__['gce']['zone']
		project_id = __node__['gce']['project_id']
		server_name = __node__['server_id']

		this_instance = con.instances().get(zone=zone,
										   project=project_id,
										   instance=server_name).execute()
		attached = filter(lambda x: x['source'] == self.link, this_instance.disks)
		if attached:
			return attached[0]


	def _detach(self, force, **kwds):
		connection = __node__['gce']['compute_connection']
		attachment_inf = self._attachment_info(connection)
		if attachment_inf:
			zone = __node__['gce']['zone']
			project_id = __node__['gce']['project_id']
			server_name = __node__['server_id']
			op = connection.instances().detachDisk(instance=server_name,
										project=project_id,
										zone=zone,
										deviceName=attachment_inf['deviceName']).execute()

			gce_util.wait_for_operation_to_complete(connection, project_id, op['name'], zone=zone)


	def _destroy(self, force, **kwds):
		pass


	def _snapshot(self, description, tags, **kwds):
		connection = __node__['gce']['compute_connection']
		project_id = __node__['gce']['project_id']

		now_raw = datetime.datetime.utcnow()
		now_str = now_raw.strftime('%d-%b-%Y-%H-%M-%S-%f')
		snap_name = ('%s-snap-%s' % (self.name, now_str)).lower()

		operation = connection.snapshots().insert(project=project_id,
						body=dict(
							name=snap_name,
							# Doesnt work without kind (3.14.2013)
							kind="compute#snapshot",
							description=description,
							sourceDisk=self.link,
						)).execute()

		try:
			gce_util.wait_for_operation_to_complete(connection, project_id, operation['name'])
		except:
			e = sys.exc_info()[1]
			raise storage2.StorageError('Google disk snapshot creation '
					'failed. Error: %s' % e)

		snapshot = connection.snapshots().get(project=project_id, snapshot=snap_name,
							fields='id,name,diskSizeGb,selfLink').execute()

		return GcePersistentSnapshot(id=snapshot['id'], name=snapshot['name'],
						size=snapshot['diskSizeGb'], link=snapshot['selfLink'])


class GcePersistentSnapshot(base.Snapshot):

	def __init__(self, name, **kwds):
		super(GcePersistentSnapshot, self).__init__(name=name, **kwds)


	def _destroy(self):
		try:
			connection = __node__['gce']['compute_connection']
			project_id = __node__['gce']['project_id']

			op = connection.snapshots().delete(project=project_id,
											snapshot=self.name).execute()

			gce_util.wait_for_operation_to_complete(connection, project_id,
										   op['name'])
		except:
			e = sys.exc_info()[1]
			raise storage2.StorageError('Failed to delete google disk snapshot.'
										' Error: %s' % e)


storage2.volume_types['gce_persistent'] = GcePersistentVolume
storage2.snapshot_types['gce_persistent'] = GcePersistentSnapshot