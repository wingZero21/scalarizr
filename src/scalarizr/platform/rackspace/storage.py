'''
Created on Nov 24, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.storage import uploader

import logging
import os

import cloudfiles

class CloudFilesUploadDest(uploader.UploadDest):
	
	def __init__(self, container_name, prefix, logger=None):
		self.container_name = container_name
		self.prefix = prefix
		self._logger = logger or logging.getLogger(__name__)
		
	def put(self, filename):
		self._logger.info('Uploading %s in CloudFiles container %s' % (file, self.container_name))
		base_name = os.path.basename(filename)
		obj_path = self.prefix + '/' + base_name
		try:		
			connection = cloudfiles.get_connection(username=os.environ["username"], api_key=os.environ["api_key"], serviceNet=True)
			
			try:
				container = connection.get_container(self.container_name)
			except cloudfiles.errors.NoSuchContainer:
				container = connection.create_container(self.container_name)
				
			o = container.create_object(obj_path)
			o.load_from_filename(filename)
			
		except (cloudfiles.errors.ResponseError, OSError, Exception), e:
			raise uploader.UploadError, e
		
		return os.path.join(self.container_name, obj_path)