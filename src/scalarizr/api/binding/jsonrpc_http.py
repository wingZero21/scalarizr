'''
JSON-RPC over HTTP. 

Public Scalarizr API

- Simple to Learn
- Simple to Use
'''

import binascii
import logging
import sys


from scalarizr import rpc
from scalarizr.util import cryptotool


LOG_CATEGORY = 'scalarizr.api'
LOG = logging.getLogger(LOG_CATEGORY)



class WsgiApplication(object):

	def __init__(self, req_handler, crypto_key_path):
		self.req_handler = req_handler		
		self.crypto_key_path = crypto_key_path
		
	
	def __call__(self, environ, start_response):
		try:
			length = int(environ['CONTENT_LENGTH'])
			data = environ['wsgi.input'].read(length)
		except:
			data = ''
		
		try:
			self.check_signature(data, environ['HTTP_X_SIGNATURE'], data, environ['HTTP_DATE'])
			data = self.decrypt_data(data)
		except:
			start_response('400 Bad request', [], sys.exc_info())
			return str(sys.exc_info()[1])
		
		LOG.debug('request: %s', data)
		result = self.req_handler.handle_request(data)
		
		headers = [('Content-type', 'application/json'), 
				('Content-length', str(len(result)))]
		
		start_response('200 OK', headers)
		LOG.debug('response: %s', result)
		return result


	def check_signature(self, signature, data, timestamp):
		calc_signature = cryptotool.sign_http_request(data, self._read_crypto_key(), timestamp)
		assert signature == calc_signature, "Signature doesn't match"

	
	def decrypt_data(self, data):
		try:
			return cryptotool.decrypt(data, self._read_crypto_key())
		except:
			raise rpc.InvalidRequestError('Failed to decode request data')


	def _read_crypto_key(self):
		return binascii.a2b_base64(open(self.crypto_key_path).read().strip())

'''
class HttpServer(rpc.Server):
	
	def __init__(self, endpoint, app):
		pr = urlparse.urlparse(endpoint)
		self.wsgi_server = gevent.pywsgi.WSGIServer((pr.hostname, pr.port), app)
	
	def serve_forever(self):
		self.wsgi_server.serve_forever()
		
	def stop(self):
		self.wsgi_server.stop()
'''	