__author__ = 'Nick Demyanchuk'

import sys
import datetime

from scalarizr import storage2

from scalarizr.platform.gce.storage import wait_for_operation_to_complete
from scalarizr.storage2.volumes import base
from scalarizr.storage2.volumes import gce_ephemeral
from scalarizr.node import __node__


class GcePersistentVolume(gce_ephemeral.GceEphemeralVolume):
	'''
	def _get_device_name(self):
		return 'google-%s' % (self.alias or self.name)
	'''

	@property
	def link(self):
		compute = __node__['gce']['compute_connection']
		project_id = __node__['gce']['project_id']
		return '%s%s/disks/%s' % (compute._baseUrl, project_id, self.name)


	def _snapshot(self, description, tags, **kwds):
		connection = __node__['gce']['compute_connection']
		project_id = __node__['gce']['project_id']

		now_raw = datetime.datetime.utcnow()
		now_str = now_raw.strftime('%d-%b-%Y-%H-%M-%S-%f')
		snap_name = '%s-snap-%s' % (self.id, now_str)

		operation = connection.snapshots().insert(project=project_id,
						body=dict(
							name=snap_name,
							description=description,
							sourceDisk=self.link,
							sourceDiskId=self.id
						)).execute()

		try:
			wait_for_operation_to_complete(connection, project_id, operation['name'])
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


	def destroy(self):
		try:
			connection = __node__['gce']['compute_connection']
			project_id = __node__['gce']['project_id']

			op = connection.snapshots().delete(project=project_id,
											snapshot=self.name).execute()

			wait_for_operation_to_complete(connection, project_id,
										   op['name'])
		except:
			e = sys.exc_info()[1]
			raise storage2.StorageError('Failed to delete google disk snapshot.'
										' Error: %s' % e)


storage2.volume_types['gce_persistent'] = GcePersistentVolume
storage2.snapshot_types['gce_persistent'] = GcePersistentSnapshot