from scalarizr.handlers import Handler
from scalarizr.messaging import Messages
from scalarizr.util.filetool import read_file, write_file

import os
import logging

class UpdateSshAuthorizedKeysError(BaseException):
	pass

def get_handlers ():
	return [SSHKeys()]

class SSHKeys(Handler):
	
	PATH = '/root/.ssh/authorized_keys'
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		
	def on_UpdateSshAuthorizedKeys(self, message):
	
		if not message.add and not message.remove:
			self._logger.debug('Empty key lists in message. Nothing to do.')
			return
		
		content = None

		if message.add:
			content = self._read_ssh_keys_file()	
			
			for key in message.add:
				content = self._add_key(content, key)
			
		if message.remove:
			if not content: 
				content = self._read_ssh_keys_file()
				
			for key in message.remove:
				content = self._remove_key(content, key)
				
		if content:
			content = self._check(content)
			self._write_ssh_keys_file(content)
	
	def _read_ssh_keys_file(self):
		if os.path.exists(self.PATH):
			content = read_file(self.PATH, msg='Reading autorized keys from %s'%self.PATH, logger=self._logger)
			if content == None:
				raise UpdateSshAuthorizedKeysError('Unable to read ssh keys from %s' % self.PATH)
			return content
		return ''
	
	def _write_ssh_keys_file(self, content):
		ssh_dir = os.path.basename(self.PATH)
		if not os.path.exists(ssh_dir):
			os.makedirs(ssh_dir)
		ret = write_file(self.PATH, content, msg='Writing authorized keys', logger=self._logger)
		if not ret:
			raise UpdateSshAuthorizedKeysError('Unable to write ssh keys to %s' % self.PATH)
	
	def _add_key(self, content, key):
		if not key in content:
			return content + '\n%s\n' % key
		else:
			self._logger.debug('Key already exists in %s' % self.PATH)
			return content
	
	def _remove_key(self, content, key):
		if content:
			return content.replace(key, '')
		else: 
			self._logger.debug('No keys found. Keys file %s is probably empty' % self.PATH)
			return content
		
	def _check(self, content):
		while '\n\n' in content:
			content = content.replace('\n\n', '\n')
		if not content.endswith('\n'):
			content += '\n'
		return content
		
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return (message.name == Messages.UPDATE_SSH_AUTHORIZED_KEYS)
	