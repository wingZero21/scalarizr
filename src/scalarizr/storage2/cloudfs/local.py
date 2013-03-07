import logging
import urlparse
import os
import sys
import shutil

from scalarizr.storage2 import cloudfs


LOG = logging.getLogger(__name__)


def reraise_DriverError(f):
	def wrapper(*args, **kwargs):
		try:
			return f(*args, **kwargs)
		except:
			exc = sys.exc_info()
			raise cloudfs.DriverError, exc[1], exc[2]
	return wrapper

#
class LocalFileSystem(object):
	"""
	local://abs/one/two -> /one/two
	local://rel/one/two -> one/two

	ls() and delete() don't validate input, so there's a potential danger
	when using them: ls() tries to list all files recursively - that can take
	a long time if called with some top level dirs; delete() is dangerous
	since tests mostly run as root.
	"""

	schema = "local"
	buckets = ("abs", "rel")

	def _parse_url(self, url):
		o = urlparse.urlparse(url)
		if o.scheme != self.schema:
			raise cloudfs.DriverError('Wrong schema: %s' % o.scheme)
		if o.netloc == "abs":
			path = o.path
		elif o.netloc == "rel":
			path = o.path[1:]
		else:
			raise cloudfs.DriverError('Invalid bucket: %s not in %s' % (
				o.netloc, self.buckets))
		return path

	def _format_path(self, path):
		bucket, path = ("abs", path[1:]) if path.startswith("/") else ("rel", path)
		return '%s://%s/%s' % (self.schema, bucket, path)

	@reraise_DriverError
	def ls(self, url):
		path = self._parse_url(url)

		LOG.debug("Trying to list %s", path)
		res = []
		for dirpath, dirnames, filenames in os.walk(path):
			res += map(lambda x: self._format_path(os.path.join(dirpath, x)),
			 		   filenames)
		return res

	@reraise_DriverError
	def put(self, src, url, report_to=None):
		path = self._parse_url(url)

		LOG.debug("Uploading '%s' to '%s'", src, path)
		try:
			os.makedirs(os.path.dirname(path))
		except OSError, e:
			if e.errno != 17:  # 17: already exists
				raise

		shutil.copy(src, path)

		res = path
		if res.endswith("/"):
			res = os.path.join(res, os.path.basename(src))
		return self._format_path(res)

	@reraise_DriverError
	def get(self, url, dst, report_to=None):
		path = self._parse_url(url)
		dst = os.path.join(dst, os.path.basename(path))

		LOG.debug("Downloading from '%s' to '%s'", path, dst)
		shutil.copy(path, dst)
		return dst

	@reraise_DriverError
	def delete(self, url):
		path = self._parse_url(url)

		LOG.debug("Deleting %s", path)
		return os.remove(path)


cloudfs.cloudfs_types["local"] = LocalFileSystem
