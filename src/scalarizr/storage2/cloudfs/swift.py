__author__ = 'vladimir'

import urlparse
import os
import sys
import logging

from scalarizr.storage2.cloudfs.base import DriverError
from scalarizr.storage2 import cloudfs
from scalarizr.node import __node__

import swiftclient
# TODO: make progress reports possible


LOG = logging.getLogger(__name__)


class SwiftFileSystem(object):

	schema = "swift"
	urlparse.uses_netloc.append(schema)


	def _get_connection(self):
		return __node__['openstack']['new_swift_connection']


	def ls(self, remote_path):
		container, prefix = self._parse_path(remote_path)
		conn = self._get_connection()
		objects = conn.get_container(container)[1]

		objects = (obj["name"] for obj in objects)

		if prefix:
			prefix = prefix.rstrip("/") + "/"
			objects = (obj for obj in objects if obj.startswith(prefix))

		return tuple((self._format_path(container, obj) for obj in objects))


	def _format_path(self, container, obj):
		#? shouldn't it be more specific, e.g. rackspace-swift
		return '%s://%s/%s' % (self.schema, container, obj)


	def _parse_path(self, path):
		o = urlparse.urlparse(path)
		if o.scheme != self.schema:
			# TODO: in all drivers
			raise DriverError('Wrong schema: %s' % o.scheme)
		return o.netloc, o.path[1:]  # netloc instead of hostname, because
									 # letter case matters on rackspace (and others?)


	def put(self, local_path, remote_path, report_to=None):
		LOG.info("Uploading '%s' to Swift under '%s'", local_path, remote_path)
		container, object_ = self._parse_path(remote_path)
		if object_.endswith("/"):
			object_ = os.path.join(object_, os.path.basename(local_path))

		fd = open(local_path, 'rb')
		try:
			conn = self._get_connection()
			try:
				conn.put_object(container, object_, fd)
			except swiftclient.client.ClientException, e:
				if e.http_reason == "Not Found":
					# stand closer, shoot again
					conn.put_container(container)
					conn.put_object(container, object_, fd)
				else:
					raise
		except:  # TODO: catch specific exceptions
			exc = sys.exc_info()
			raise DriverError, exc[1], exc[2]
		finally:
			fd.close()

		return self._format_path(container, object_)


	def get(self, remote_path, local_path, report_to=None):
		LOG.info('Downloading %s from Swift to %s', remote_path, local_path)
		container, object_ = self._parse_path(remote_path)
		#? join only if local_path.endswith("/")
		dest_path = os.path.join(local_path, os.path.basename(remote_path))

		fd = open(dest_path, 'w')
		try:
			conn = self._get_connection()
			res = conn.get_object(container, object_)
			fd.write(res[1])
		except:  #? see todo in put
			exc = sys.exc_info()
			raise DriverError, exc[1], exc[2]
		finally:
			fd.close()
		return dest_path


	def delete(self, remote_path):
		LOG.info('Deleting %s from Swift', remote_path)
		container, object_ = self._parse_path(remote_path)

		try:
			conn = self._get_connection()
			conn.delete_object(container, object_)
		except Exception, e:  #? see todo in put
			if isinstance(e, swiftclient.client.ClientException) and \
					e.http_reason == "Not Found":
				return False
			exc = sys.exc_info()
			raise DriverError, exc[1], exc[2]


cloudfs.cloudfs_types["swift"] = SwiftFileSystem
