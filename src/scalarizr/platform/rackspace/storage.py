'''
Created on Nov 24, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.storage.transfer import Transfer, TransferProvider, TransferError

from urlparse import urlparse
import logging
import socket
import os
import sys

import cloudfiles


class CFTransferProvider(TransferProvider):
	
	schema = 'cf'
	username = None
	api_key = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)		

	def put(self, local_path, remote_path):
		self._logger.info('Uploading %s in CloudFiles container %s' % (local_path, self.container_name))
		base_name = os.path.basename(local_path)
		obj_path = os.path.join(self.prefix, base_name)
		try:		
			
			connection = self._get_connection(remote_path)
			
			try:
				container = connection.get_container(self.container_name)
			except cloudfiles.errors.NoSuchContainer:
				self._logger.debug('Container %s not found. Trying to create.' % self.container_name)
				container = connection.create_container(self.container_name)
				
			o = container.create_object(obj_path)
			o.load_from_filename(local_path)
			
		except (cloudfiles.errors.ResponseError, OSError, Exception, socket.timeout), e:
			raise TransferError, e

		return os.path.join(self.container_name, obj_path)
	
	def get(self, remote_path, local_path):
		self._logger.info('Getting %s from CloudFiles container %s' % (remote_path, self.container_name))
		dest_path = os.path.join(local_path, os.path.basename(remote_path))
		try:		
			obj = None
			container = None
			connection = self._get_connection(remote_path)
			
			try:
				container = connection.get_container(self.container_name)
			except cloudfiles.errors.NoSuchContainer:
				raise TransferError('Container %s not found.' % self.container_name)
			
			try:
				o = urlparse(remote_path)
				basename = o.path
				obj = container.get_object(basename)
			except cloudfiles.errors.NoSuchObject, e:
				raise TransferError('Object %s not found in %s container.' 
						% (remote_path, self.container_name))
				
			obj.save_to_filename(dest_path)
			
		except (cloudfiles.errors.ResponseError, OSError, Exception), e:
			raise TransferError, e
		return os.path.join(self.container_name, dest_path)
	
	def configure(self, remote_path, username=None, api_key=None, force=False):
		o = urlparse(remote_path)
		if o.scheme != self.schema:
			raise TransferError('Wrong schema')
		self.container_name = o.hostname
		self.prefix = o.path
		if not self.username or force:
			self.username = username if username else os.environ["username"]
		if not self.api_key or force:
			self.api_key = api_key if api_key else os.environ["api_key"]
		
	def list(self, remote_path):
		connection = self._get_connection(remote_path)
		container = connection.get_container(self.container_name)
		objects = container.get_objects(path=self.prefix)
		return [self.schema+'://'+self.container_name+obj.name for obj in objects] if objects else []	

	def _get_connection(self, remote_path):
		self.configure(remote_path)
		if not self.username or not self.api_key:
			raise TransferError('Couldn`t initialize connection to Cloud Files: Credentials not found.')
		return cloudfiles.get_connection(username=self.username, api_key=self.api_key, serviceNet=True)
	
Transfer.explore_provider(CFTransferProvider)