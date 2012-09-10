__author__ = 'Nick Demyanchuk'

import os
import urlparse
import logging

from apiclient.http import MediaFileUpload, MediaIoBaseDownload

from scalarizr.bus import bus
from scalarizr.storage import transfer


LOG = logging.getLogger(__name__)
CHUNK_SIZE = 2*1024*1024


class GoogleCSTransferProvider(transfer.TransferProvider):

	schema = 'gs'
	urlparse.uses_netloc.append(schema)


	def list(self, remote_path):
		bucket, path = self._parse_path(remote_path)

		req = self.cloudstorage.objects().list(
			bucket=bucket, prefix=path, delimiter='/'
		)
		resp = req.execute()
		return tuple('gs://' % o['name'] for o in resp['items'])


	def get(self, remote_path, local_path):
		LOG.debug('Downloading %s from cloud storage (local path: %s)', remote_path, local_path)
		bucket, name = self._parse_path(remote_path)
		f = open(local_path, 'w')
		request = self.cloudstorage.objects().get_media(
			bucket=bucket, object=name)
		media = MediaIoBaseDownload(f, request, chunksize=CHUNK_SIZE)
		done = False

		while not done:
			status, done = media.next_chunk()
			if status:
				LOG.debug('Downloaded %d%%' % int(status.progress() * 100))

		LOG.debug('Download complete.')


	def put(self, local_path, remote_path):
		LOG.debug('Uploading %s to cloud storage (remote path: %s)', local_path, remote_path)

		filename = os.path.basename(local_path)
		bucket, name = self._parse_path(remote_path)
		name = os.path.join(name, filename)

		buckets = self._list_buckets()
		if bucket not in buckets:
			self._create_bucket(bucket)

		media = MediaFileUpload(local_path,
			'application/octet-stream',
			resumable=True)

		response = None
		req = self.cloudstorage.objects().insert(
			bucket=bucket, name=name, media_body=media
		)

		while response is None:
			status, response = req.next_chunk()
			if status:
				LOG.debug("Uploaded %d%%." % int(status.progress() * 100))

		LOG.debug('Upload completed.')


	def _parse_path(self, path):
		o = urlparse.urlparse(path)
		if o.scheme != self.schema:
			raise transfer.TransferError('Wrong schema')
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
		req.execute()


	@property
	def cloudstorage(self):
		if not hasattr(self, '_cloudstorage'):
			pl = bus.platform
			self._cloudstorage = pl.new_storage_client()
		return self._cloudstorage
