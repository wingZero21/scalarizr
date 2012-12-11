__author__ = 'Nick Demyanchuk'

import sys
import datetime

from scalarizr import storage2, util
from scalarizr.storage2.volumes import base
from scalarizr.storage2.volumes import gce_ephemeral
from scalarizr.node import __node__


def wait_for_operation_to_complete(connection, project_id, operation_name, timeout=3600):
	def op_complete():
		status = connection.operations().get(project=project_id,
					operation=operation_name, fields='status, error').execute()
		if 'DONE' == status['status']:
			error = status.get('error')
			if error:
				err_msg = '\n'.join([err['message'] for err in error['errors']])
				raise Exception(err_msg)
			return True
		return False

	util.wait_until(op_complete, timeout=timeout)


class GcePersistentVolume(gce_ephemeral.GceEphemeralVolume):
	'''
	def _get_device_name(self):
		return 'google-%s' % (self.alias or self.name)
	'''


	def _snapshot(self, description, tags, **kwds):
		connection = __node__['gce']['compute_connection']
		project_id = __node__['gce']['project_id']

		now_raw = datetime.datetime.utcnow()
		now_str = now_raw.strftime('%d-%b-%Y-%H-%M-%S-%f')
		snap_name = '%s_snap_%s' % (self.id, now_str)

		operation = connection.snapshots().insert(project=project_id,
						body=dict(
							name=snap_name,
							description=description,
							sourceDisk=self.name,
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
		connection = __node__['gce']['compute_connection']
		project_id = __node__['gce']['project_id']

		op = connection.snapshots().delete(project=project_id,
											snapshot=self.name).execute()
		try:
			wait_for_operation_to_complete(connection, project_id,
										   op['name'])
		except:
			e = sys.exc_info()[1]
			raise storage2.StorageError('Failed to delete google disk snapshot.'
										' Error: %s' % e)


storage2.volume_types['gce_persistent'] = GcePersistentVolume
storage2.snapshot_types['gce_persistent'] = GcePersistentSnapshot