import os

from scalarizr import util


def devicename_to_device(device_name):
	path = '/dev/disk/by-id/google-{0}'.format(device_name)
	if os.path.exists(path):
		rel_path = os.readlink(path)
		abs_path = os.path.abspath(os.path.join(
			os.path.dirname(path), rel_path
		))
		return abs_path


def get_op_status(conn, proj_id, op_name, zone=None, fields=None):
	fields = fields if isinstance(fields, str) else ', '.join(fields) if fields else None
	kwargs = dict(project=proj_id, operation=op_name, fields=fields)
	if zone:
		kwargs['zone'] = zone
		return conn.zoneOperations().get(**kwargs).execute()
	else:
		return conn.globalOperations().get(**kwargs).execute()


def wait_for_operation(connection, project_id, operation_name,
								   zone=None, status_to_wait=("DONE",), timeout=3600):
	def op_reached_status():
		status = get_op_status(connection,
							   project_id,
							   operation_name,
							   zone,
							   ('status', 'error'))
		if status['status'] in status_to_wait:
			error = status.get('error')
			if error:
				err_msg = '\n'.join([err['message'] for err in error['errors']])
				raise Exception(err_msg)
			return True
		return False

	util.wait_until(op_reached_status, timeout=timeout)
