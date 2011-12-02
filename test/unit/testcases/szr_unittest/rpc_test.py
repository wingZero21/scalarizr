'''
Created on Dec 2, 2011

@author: marat
'''
import unittest
import json
from scalarizr import rpc


FOO = {
	'str': 'str',
	'int': 1,
	'bool': False
}


class MyService(object):
	
	@rpc.service_method
	def foo(self, bar=None):
		return FOO
	
	@rpc.service_method
	def with_non_keyworded_args(self, a, b, c):
		return FOO
	
	@rpc.service_method
	def raise_exception(self):
		raise 
	
	def unregistered_method(self):
		pass


class MyServiceClient(rpc.ServiceProxy):
	
	def exchange(self, request):
		return json.dumps(self.endpoint.handle_request(request))


class TestServiceHandler(unittest.TestCase):


	def setUp(self):
		self.handler = rpc.ServiceHandler(MyService())
		self.client = MyServiceClient(self.handler)

	def test_get_foo(self):
		assert self.client.foo() == FOO
	
	def test_unregistered_method(self):
		self.assert_method_not_found('unregistered_method')

	def test_non_existed_method(self):
		self.assert_method_not_found('non_existed')
	
	def assert_method_not_found(self, method):
		try:
			getattr(self.client, method)()
			self.fail()
		except rpc.ServiceError, e:
			assert e.code == rpc.ServiceError.METHOD_NOT_FOUND
	
	def test_parse_error(self):
		resp = self.handler.handle_request('not a json string')
		assert resp['error']['code'] == rpc.ServiceError.PARSE

	def test_invalid_request(self):
		resp = self.handler.handle_request(json.dumps({'method': 'klunk'}))
		assert resp['error']['code'] == rpc.ServiceError.INVALID_REQUEST

	def test_internal_error(self):
		try:
			self.client.foo(unknown='omm')
			self.fail()
		except rpc.ServiceError, e:
			assert e.data == "foo() got an unexpected keyword argument 'unknown'"

if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()