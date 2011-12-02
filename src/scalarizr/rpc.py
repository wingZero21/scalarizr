'''
Created on Nov 21, 2011

@author: marat

JSON-RPC implementation and utilities for 0MQ and HTTP transport 
'''

import sys
import time
try:
	import json
except ImportError:
	import simplejson as json
try:
	from gevent.local import local
except ImportError:
	from threading import local


def service_method(fn):
	fn.jsonrpc = True
	return fn


class ServiceError(Exception):
	PARSE = -32700
	INVALID_REQUEST = -32600
	METHOD_NOT_FOUND = -32601
	INVALID_PARAMS = -32602
	INTERVAL = -32603
	
	def __init__(self, *args):
		self.code, self.message = args[0:2]
		try:
			self.data = args[2]
		except IndexError:
			self.data = None
		Exception.__init__(self, *args)


class ServiceHandler(object):
	def __init__(self, service):
		if not service:
			import __main__ as service
		self.service = service
		
	def handle_request(self, data):
		id, result, error = '', None, None
		try:
			req = self._parse_request(data)
			id, method, params = self._translate_request(req)
			fn = self._find_method(method)
			result = self._invoke_method(fn, params)
		except ServiceError, e:
			error = {'code': e.code, 
					'message': e.message, 
					'data': e.data}
		except:
			error = {'code': ServiceError.INTERVAL, 
					'message': 'Internal error', 
					'data': str(sys.exc_value)}
		finally:
			if result:
				resp = {'result': result}
			else:
				resp = {'error': error}
			resp['id'] = id
		return resp

	
	def _parse_request(self, data):
		try:
			return json.loads(data)
		except:
			raise ServiceError(ServiceError.PARSE, 
							'Parse error', 
							str(sys.exc_value))
	
	def _translate_request(self, req):
		try:
			return req['id'], req['method'], req['params']
		except:
			raise ServiceError(ServiceError.INVALID_REQUEST, 
							'Invalid Request',
							 str(sys.exc_value))

	def _find_method(self, name):
		meth = getattr(self.service, name, None)
		if meth and getattr(meth, 'jsonrpc', None):
			return meth
		else:
			raise ServiceError(ServiceError.METHOD_NOT_FOUND, 
							'Method not found', 
							name)			
		
	def _invoke_method(self, method, params):
		return method(**params)


class ServiceProxy(object):

	def __init__(self, endpoint):
		self.endpoint = endpoint
		self.local = local()

	def __getattr__(self, name):
		try:
			self.__dict__['local'].method.append(name)
		except AttributeError:
			self.__dict__['local'].method = [name]
		return self
	
	def __call__(self, **kwds):
		try:
			req = json.dumps({'method': self.local.method[-1], 'params': kwds, 'id': time.time()})
			resp = json.loads(self.exchange(req))
			if 'error' in resp:
				error = resp['error']
				raise ServiceError(error.get('code'), error.get('message'), error.get('data'))
			return resp['result']
		finally:
			self.local.method = []
		
	def exchange(self, request):
		raise NotImplemented()
	
	
class Server(object):

	def __init__(self, endpoint, handler):
		self.endpoint = endpoint
		self.handler = handler

	def serve_forever(self):
		raise NotImplemented()
	
