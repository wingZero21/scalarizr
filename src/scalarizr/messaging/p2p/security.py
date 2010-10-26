'''
Created on Aug 10, 2010

@author: marat
'''

import logging, binascii
from scalarizr.util import cryptotool, configtool
from scalarizr.messaging import MessagingError


class P2pMessageSecurity(object):
	server_id = None
	crypto_key_path = None
	_logger = None
	def __init__(self, server_id=None, crypto_key_path=None):
		self._logger = logging.getLogger(__name__)
		self.server_id = server_id
		self.crypto_key_path = crypto_key_path
	
	def in_protocol_filter(self, consumer, queue, message):
		try:
			self._logger.debug('Decrypting message')
			crypto_key = configtool.read_key(self.crypto_key_path)
			self._logger.debug('Key: %s', crypto_key)
			crypto_key = binascii.a2b_base64(configtool.read_key(self.crypto_key_path))
			xml = cryptotool.decrypt(message, crypto_key)
			# Remove special chars
			return xml.strip(''.join(chr(i) for i in range(0, 31)))
				
		except (BaseException, Exception), e:
			raise MessagingError('Cannot decrypt message. error: %s; raw message: %s' % (e, message))
	
	def out_protocol_filter(self, producer, queue, message, headers):
		try:
			self._logger.debug('Encrypting message')
			
			# Crypt
			crypto_key = configtool.read_key(self.crypto_key_path)
			self._logger.debug('Key: %s', crypto_key)
			crypto_key = binascii.a2b_base64(configtool.read_key(self.crypto_key_path))
			
			data = cryptotool.encrypt(message, crypto_key)
			
			# Sign
			signature, timestamp = cryptotool.sign_http_request(data, crypto_key)
			
			# Modify headers
			headers['Date'] = timestamp
			headers['X-Signature'] = signature
			headers['X-Server-Id'] = self.server_id
			
			return data
		except (BaseException, Exception), e:
			raise MessagingError('Cannot encrypt message. error: %s' % (e))
