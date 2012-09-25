
from scalarizr.storage2 import cloudfs
from scalarizr.node import __node__

class GlacierFilesystem(cloudfs.CloudFileSystem):

	def _conn(self):
		return __node__['ec2']['connect_glacier']()


	def multipart_init(self, path):
		'''
		Returns upload_id
		'''
		raise NotImplementedError()

	def multipart_put(self, upload_id, src):
		raise NotImplementedError()

	def multipart_complete(self, upload_id):
		'''
		Returns glacier://Vault_1/?avail_zone=us-east-1&archive_id=NkbByEejwEggmBz2fTHgJrg0XBoDfjP4q6iu87-TjhqG6eGoOY9Z8i1_AUyUsuhPAdTqLHy8pTl5nfCFJmDl2yEZONi5L26Omw12vcs01MNGntHEQL8MBfGlqrEXAMPLEArchiveId
		'''
		raise NotImplementedError()

	def multipart_abort(self, upload_id):
		raise NotImplementedError()
