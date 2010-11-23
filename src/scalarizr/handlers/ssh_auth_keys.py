from scalarizr.handlers import Handler
import logging
from scalarizr.messaging import Messages
from scalarizr.util.filetool import read_file, write_file

class UpdateSshAuthorizedKeysError(BaseException):
	pass

class SSHKeys(Handler):
	
	path = '/root/.ssh/authorized_keys'
	content = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		
	def on_UpdateSshAuthorizedKeys(self, message):
		self.content = None

		if not message.add and not message.remove:
			self._logger.debug('Empty key lists in message. Nothing to do.')
			return
		
		if message.add:
			self._read_ssh_keys_file()	
			
			for key in message.add:
				self._add_key(key)
			
		if message.remove:
			if not self.content: 
				self._read_ssh_keys_file()
				
			for key in message.remove:
				self._remove_key(key)
				
		if self.content:
			self._write_ssh_keys_file()
	
	def _read_ssh_keys_file(self):
		self.content = read_file(self.path, msg='Reading autorized keys from %s'%self.path, logger=self._logger)
		if self.content == None:
			raise UpdateSshAuthorizedKeysError('Unable to read ssh keys from %s' % self.path)
	
	def _write_ssh_keys_file(self):
		ret = write_file(self.path, self.content, msg='Writing authorized keys', logger=self._logger)
		if not ret:
			raise UpdateSshAuthorizedKeysError('Unable to write ssh keys to %s' % self.path)
	
	def _add_key(self, key):
		if not key in self.content:
			self.content += '\n%s\n' % key
		else:
			self._logger.debug('Key already exists in %s' % self.path)
	
	def _remove_key(self, key):
		if self.content:
			self.content = self.content.replace(key, '')
		else: 
			self._logger.debug('No keys found. Keys file %s is probably empty' % self.path)
		
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return (message.name == Messages.UPDATE_SSH_AUTHORIZED_KEYS)
	