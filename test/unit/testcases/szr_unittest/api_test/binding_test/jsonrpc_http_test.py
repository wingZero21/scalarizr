'''
Created on May 15, 2012

@author: marat
'''

import binascii
import os
import tempfile
import shutil

from scalarizr import rpc
from scalarizr.api.binding import jsonrpc_http
from scalarizr.util import cryptotool

from nose.tools import raises

class MyService(object):
	
	@rpc.service_method
	def foo(self):
		return 'bar'
	

class TestWsgiApplication(object):
	
	def setup(self):
		self.tmp = tempfile.mkdtemp()

		self.key_path = os.path.join(self.tmp, 'crypto_key')
		with open(self.key_path, 'w') as fp:
			fp.write(cryptotool.keygen())
		
		self.app = jsonrpc_http.WsgiApplication(
						rpc.RequestHandler({'myservice': MyService()}), 
						self.key_path)
	
	
	def teardown(self):
		shutil.rmtree(self.tmp)


	def _read_crypto_key(self):
		return binascii.a2b_base64(open(self.key_path).read().strip())

	
	def test_check_signature(self):
		data = 'ABCDEF'
		signature, timestamp = cryptotool.sign_http_request(data, self._read_crypto_key())
		self.app.check_signature(signature, data, timestamp)
	
	
	@raises(AssertionError)
	def test_check_signature_invalid(self):
		self.app.check_signature('4ycrRqph560YsgK/HTT5zKeYrQ8=', 'ABC', 'Tue 15 May 2012 16:05:32 EET')
	
	
	def test_decrypt_data(self):
		s = 'ABC'
		encrypted = cryptotool.encrypt(s, self._read_crypto_key())
		assert self.app.decrypt_data(encrypted) == s
	
	
	@raises(rpc.InvalidRequestError)
	def test_decrypt_data_invalid(self):
		self.app.decrypt_data('invalid')
	
	
	def test_call(self):
		
		
		environ = 
	
	
	def test_error_in_request_handler(self):
		pass
	
	
	def test_error_in_check_signature(self):
		pass
	
	
	def test_error_in_decrypt_data(self):
		pass
	
	
