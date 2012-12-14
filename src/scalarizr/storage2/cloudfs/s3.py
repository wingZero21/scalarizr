__author__ = 'vladimir'

import urlparse
import logging
import os
import sys

from scalarizr.node import __node__
from scalarizr.storage2 import cloudfs

from boto.s3.key import Key
from boto.exception import S3ResponseError

from scalarizr.storage2 import cloudfs

LOG = logging.getLogger(__name__)


class S3FileSystem(object):
	schema = 's3'
	urlparse.uses_netloc.append(schema)

	acl = None

	_bucket = None

	def __init__(self, acl='aws-exec-read'):
		self.acl = acl

	def parse_url(self, url):
		o = urlparse.urlparse(url)
		if o.scheme != self.schema:
			raise cloudfs.DriverError('Wrong schema')
		return o.hostname, o.path[1:]

	def ls(self, remote_path):
		bucket_name, key_name = self.parse_url(remote_path)
		connection = self._get_connection()
		bkt = connection.get_bucket(bucket_name, validate=False)
		files = [self._format_path(self._bucket.name, key.name) for key in bkt.list(prefix=key_name)]
		return tuple(files)

	def stat(self, path):
		raise NotImplementedError()

	def put(self, local_path, remote_path):
		LOG.info("Uploading '%s' to S3 under '%s'", local_path, remote_path)
		bucket_name, key_name = self.parse_url(remote_path)
		key_name = os.path.join(key_name, os.path.basename(local_path))

		try:
			connection = self._get_connection()

			if not self._bucket_check_cache(bucket_name):
				try:
					bck = connection.get_bucket(bucket_name)
				except S3ResponseError, e:
					if e.code == 'NoSuchBucket':
						bck = connection.create_bucket(
							bucket_name,
							location=self._bucket_location(),
							policy=self.acl
						)
					else:
						raise
				# Cache bucket
				self._bucket = bck

			file = None
			try:
				key = Key(self._bucket)
				key.name = key_name
				file = open(local_path, "rb")
				LOG.debug("Actually uploading %s" % file)
				# TODO: sometimes takes a long time
				#(finished within 22 minutes) - on 20 mb file
				key.set_contents_from_file(file, policy=self.acl)
				LOG.debug("Finished uploading %s" % file)
				return self._format_path(bucket_name, key_name)
			finally:
				if file:
					file.close()

		except:
			exc = sys.exc_info()
			LOG.debug('Caught error', exc_info=exc)
			raise cloudfs.DriverError, exc[1], exc[2]

	def get(self, remote_path, local_path):
		LOG.info('Downloading %s from S3 to %s' % (remote_path, local_path))
		bucket_name, key_name = self.parse_url(remote_path)
		dest_path = os.path.join(local_path, os.path.basename(remote_path))

		try:
			connection = self._get_connection()
			try:
				if not self._bucket_check_cache(bucket_name):
					self._bucket = connection.get_bucket(bucket_name, validate=False)
				key = self._bucket.get_key(key_name)
				if key is None:
					raise cloudfs.DriverError("Key is None. No such key?")   ###
			except S3ResponseError, e:
				if e.code in ('NoSuchBucket', 'NoSuchKey'):
					raise cloudfs.DriverError("S3 path '%s' not found" % remote_path)
				raise

			LOG.debug("Actually downloading %s" % file)
			key.get_contents_to_filename(dest_path)
			LOG.debug("Finished downloading %s" % file)
			return dest_path

		except:
			exc = sys.exc_info()
			raise cloudfs.DriverError, exc[1], exc[2]

	def delete(self, path):
		raise NotImplementedError()

	def _bucket_check_cache(self, bucket):
		if self._bucket and self._bucket.name != bucket:
			self._bucket = None
		return self._bucket

	def _get_connection(self):
		return __node__['ec2']['connect_s3']()

	def _bucket_location(self):
		region = __node__['ec2']['region']
		if region == 'us-east-1' or not region:
			return ''
		elif region == 'eu-west-1':
			return 'EU'
		else:
			return region

	def _format_path(self, bucket, key):
		return '%s://%s/%s' % (self.schema, bucket, key)


cloudfs.cloudfs_types["s3"] = S3FileSystem
