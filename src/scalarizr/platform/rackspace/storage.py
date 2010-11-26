'''
Created on Nov 24, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.storage import uploader
import logging
import socket
import os
import cloudfiles
from scalarizr.storage.uploader import TransferError

class CloudFilesUploadDest(uploader.UploadDest):
	
	def __init__(self, container_name, prefix, logger=None):
		self.container_name = container_name
		self.prefix = prefix
		self._logger = logger or logging.getLogger(__name__)
		
	def put(self, filename):
		self._logger.info('Uploading %s in CloudFiles container %s' % (filename, self.container_name))
		base_name = os.path.basename(filename)
		obj_path = self.prefix + '/' + base_name
		try:		
			
			connection = self._get_connection()
			
			try:
				container = connection.get_container(self.container_name)
			except cloudfiles.errors.NoSuchContainer:
				self._logger.debug('Container %s not found. Trying to create.' % self.container_name)
				container = connection.create_container(self.container_name)
				
			o = container.create_object(obj_path)
			o.load_from_filename(filename)
			
		except (cloudfiles.errors.ResponseError, OSError, Exception, socket.timeout), e:
			raise uploader.TransferError, e
		
		return os.path.join(self.container_name, obj_path)
	
	def get(self, filename, dest):
		self._logger.info('Getting %s from CloudFiles container %s' % (filename, self.container_name))
		dest_path = os.path.join(dest, os.path.basename(filename))
		try:		
			obj = None
			container = None
			connection = self._get_connection()
			
			try:
				container = connection.get_container(self.container_name)
			except cloudfiles.errors.NoSuchContainer:
				raise TransferError('Container %s not found.' % self.container_name)
			
			try:
				obj = container.get_object(filename)
			except cloudfiles.errors.NoSuchObject, e:
				raise TransferError('Object %s not found in %s container.' 
						% (filename, self.container_name))
				
			obj.save_to_filename(dest_path)
			
		except (cloudfiles.errors.ResponseError, OSError, Exception), e:
			raise uploader.TransferError, e
		return os.path.join(self.container_name, dest_path)
		
	def get_list_files(self):
		connection = self._get_connection()
		container = connection.get_container(self.container_name)
		objects = container.get_objects(path=self.prefix)
		self._logger.info([obj.name for obj in objects])
		return [obj.name for obj in objects] if objects else []	

	def _get_connection(self):
		return cloudfiles.get_connection(username=os.environ["username"], api_key=os.environ["api_key"], serviceNet=True)
	