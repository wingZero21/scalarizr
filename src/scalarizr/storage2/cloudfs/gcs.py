__author__ = 'vladimir'

import logging
import urlparse
import os
import sys
import json

from apiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
from apiclient.errors import HttpError

from scalarizr.storage2 import cloudfs
from scalarizr.bus import bus
from scalarizr.node import __node__


# TODO: get connection from node


LOG = logging.getLogger(__name__)


class GCSFileSystem (object):
	# TODO: add amazon-style 0% and 100% callbacks ?

	schema = 'gcs'
	urlparse.uses_netloc.append(schema)
	chunk_size = 2*1024*1024
	report_interval = 10  # percent; every <value> percent at most


	def ls(self, remote_path):
		bucket, path = self._parse_path(remote_path)

		path = path.rstrip('/') + '/' if path else ''

		req = self.cloudstorage.objects().list(
			bucket=bucket, prefix=path)
		resp = req.execute()

		items = (self._format_path(bucket, x["name"])
				 for x in resp.setdefault("items", []))
		return tuple(items)


	def _format_path(self, bucket, key):
		return "%s://%s/%s" % (self.schema, bucket, key)


	def get(self, remote_path, local_path, report_to=None):
		LOG.debug('Downloading %s from cloud storage (local path: %s)', remote_path, local_path)
		bucket, name = self._parse_path(remote_path)
		local_path = os.path.join(local_path, os.path.basename(remote_path))

		request = self.cloudstorage.objects().get_media(
			bucket=bucket, object=name)

		f = open(local_path, 'w')
		try:
			media = MediaIoBaseDownload(f, request, chunksize=self.chunk_size)

			last_progress = 0
			done = False
			while not done:
				status, done = media.next_chunk()
				if status:
					percentage = int(status.progress() * 100)
					if percentage - last_progress >= self.report_interval:
						if report_to:
							report_to(status.resumable_progress, status.total_size)
						last_progress = percentage
		finally:
			f.close()

		LOG.debug("Finished downloading %s" % os.path.basename(local_path))
		return local_path


	def put(self, local_path, remote_path, report_to=None):
		LOG.debug('Uploading %s to cloud storage (remote path: %s)', local_path, remote_path)
		filename = os.path.basename(local_path)
		bucket, name = self._parse_path(remote_path)
		if name.endswith("/"):
			name = os.path.join(name, filename)

		buckets = self._list_buckets()
		if bucket not in buckets:
			self._create_bucket(bucket)

		fd = open(local_path, 'rb')
		try:
			media = MediaIoBaseUpload(fd,
				'application/octet-stream',
				resumable=True)
			req = self.cloudstorage.objects().insert(
				bucket=bucket, name=name, media_body=media
			)
			last_progress = 0
			response = None
			while response is None:
				status, response = req.next_chunk()
				if status:
					percentage = int(status.progress() * 100)
					if percentage - last_progress >= self.report_interval:
						if report_to:
							report_to(status.resumable_progress, status.total_size)
						last_progress = percentage
		finally:
			fd.close()
		LOG.debug("Finished uploading %s" % os.path.basename(local_path))
		return 'gcs://%s' % os.path.join(bucket, name)


	def delete(self, remote_path):
		bucket, obj = self._parse_path(remote_path)

		req = self.cloudstorage.objects().delete(bucket=bucket, object=obj)
		try:
			return req.execute()
		except HttpError, e:
			if "Not Found" in json.loads(e.content)["error"]["message"]:
				return False
			else:
				raise


	def _parse_path(self, path):
		o = urlparse.urlparse(path)
		if o.scheme != self.schema:
			raise cloudfs.DriverError('Wrong schema')
		return o.hostname, o.path[1:]


	def _list_buckets(self):
		pl = bus.platform
		proj_id = pl.get_numeric_project_id()

		req = self.cloudstorage.buckets().list(projectId=proj_id)
		resp = req.execute()
		if 'items' not in resp:
			return []
		buckets = [b['id'] for b in resp['items']]
		return buckets


	def _create_bucket(self, bucket_name):
		pl = bus.platform
		proj_id = pl.get_numeric_project_id()

		req_body = dict(id=bucket_name, projectId=proj_id)
		req = self.cloudstorage.buckets().insert(body=req_body)
		try:
			req.execute()
		except:
			e = sys.exc_info()[1]
			if not 'You already own this bucket' in str(e):
				raise


	@property
	def cloudstorage(self):
		pl = bus.platform
		return pl.new_storage_client()


cloudfs.cloudfs_types["gcs"] = GCSFileSystem
