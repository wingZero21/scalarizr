'''
Created on Dec 22, 2011

@author: marat
'''
import unittest
import inspect

from scalarizr.libs import validate

_rule_protocol = validate.rule(choises=['tcp', 'http'])
_rule_backend = validate.rule(re=r'^role:\d+$')
_rule_hc_target = validate.rule(re='^[tcp|http]:\d+$')

def foo(protocol=None, port=None, ipv4=None, ipv6=None, backend=None):
	return 'bar'

class Bar(object):
	@validate.param('only_int', type=int)
	@validate.param('backend', optional=_rule_backend)
	@validate.param('something', type=int)
	def foo(self, only_int=789, backend='ololo', something=None):
		pass

class TestObjectBounded(unittest.TestCase):
	def test(self):
		bar = Bar()
		#print bar.foo
		#print inspect.getargspec(bar.foo)
		
		try:
			bar.foo(only_int=123, backend='role:123', something='dasd')
			#self.fail()
		except ValueError, e:
				assert 'Type error(int expected)' in str(e)

class _TestRequired(unittest.TestCase):

	def setUp(self):
		self.foo = validate.param('protocol', required=True)(foo)
	
	def test_ok(self):
		self.foo(protocol='tcp')
	
	def test_fail(self):
		try:
			self.foo()
			self.fail()
		except ValueError, e:
			assert 'Empty: protocol' in str(e)


class _TestChoises(unittest.TestCase):
	
	def setUp(self):
		self.foo = validate.param('protocol', choises=['tcp', 'http'])(foo)

	def test_ok(self):
		self.foo(protocol='tcp')
	
	def test_fail(self):
		try:
			self.foo(protocol='https')
			self.fail()
		except ValueError, e:
			assert 'Allowed values are' in str(e)


class _TestRe(unittest.TestCase):
	
	def setUp(self):
		self.foo = validate.param('backend', optional=validate.rule(re=r'^role:\d+$'))(foo)

	def test_ok(self):
		self.foo(backend='role:1675')
	
	def test_optional(self):
		self.foo()
	
	def test_fail(self):
		try:
			self.foo(backend='incorrect')
			self.fail()
		except ValueError, e:
			assert "Doesn't match expression" in str(e)
			assert "^role:\d+$" in str(e)


class _TestType(unittest.TestCase):
	
	def setUp(self):
		self.foo = validate.param('port', type=int)(foo)

	def test_ok(self):
		self.foo(port=1234)
	
	def test_optional_fail(self):
		try:
			self.foo()
			self.fail()
		except ValueError, e:
			assert 'Empty: port' in str(e)
	
	def test_fail(self):
		try:
			self.foo(port='seven')
			self.fail()
		except ValueError, e:
			assert 'Type error(int expected)' in str(e)
	

class _TestUserType(unittest.TestCase):
	
	def setUp(self):
		self.foo = validate.param('ipv6', type='ipv6', optional=True)(
					validate.param('ipv4', type='ipv4', optional=True)(
					foo))
		
	def test_ipv4(self):
		self.foo(ipv4='50.17.202.206')
		
	def test_ipv6(self):
		self.foo(ipv6='2001:0db8:11a3:09d7:1f34:8a2e:07a0:765d')
					
	def test_ipv4_fail(self):
		try:
			self.foo(ipv4='256.0.0.0')
			self.fail()
		except ValueError, e:
			assert 'Type error(ipv4 expected)' in str(e)


if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.test']
	unittest.main()