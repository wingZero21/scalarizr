__author__ = 'Nick Demyanchuk'

import os
import sys
import datetime

from scalarizr import storage2, util
from scalarizr.storage2.volumes import base
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



class GceEphemeralVolume(base.Volume):
	location = '/dev/disk/by-id/'

	def __init__(self, ephemeral_id, *args, **kwargs):
		super(GceEphemeralVolume, self).__init__(ephemeral_id=ephemeral_id, *args, **kwargs)


	def _ensure(self):
		device_path = self._get_device_path()
		if not os.path.exists(device_path):
			raise storage2.StorageError('Device %s not found.' % device_path)

		self.device = device_path


	def _get_device_path(self):
		return os.path.join(self.location, self._get_device_name())


	def _get_device_name(self):
		return 'google-ephemeral-disk-%s' % self.ephemeral_id



class GcePersistentVolume(GceEphemeralVolume):

	def __init__(self, name, alias=None, *args, **kwargs):
		super(GcePersistentVolume, self).__init__(name=name, alias=alias, *args, **kwargs)


	def _get_device_name(self):
		return 'google-%s' % (self.alias or self.name)


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




