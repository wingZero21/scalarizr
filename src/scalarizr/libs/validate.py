'''
Simple validation decorators 

@author: marat

Sample usage:
>> @validate.param('port', type=int)
>> @validate.param('ipaddr', type='ipv4')
>> @validate.param('backend', re=r'^role:\d+$')
>> def foo(port=None, ipaddr=None, backend=None):
...    pass
>> 

IMPORTANT!
The current limitation: all function args should have default values 
'''

import re
import inspect


MESSAGES = {
	'type': 'Type error(%s expected): %s',
	'empty': 'Empty: %s',
	're': "Doesn't match expression %s: %s",
	'choises': 'Allowed values are %s: %s',
	'unknown_user_type': 'Unknown user type: %s'
}

USER_TYPES = {
	'ipv4': re.compile(r'^(25[0-5]|2[0-4]\d|[0-1]?\d?\d)(\.(25[0-5]|2[0-4]\d|[0-1]?\d?\d)){3}$'),
	'ipv6': re.compile(r'^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$')
}

class rule(object):
	re = None
	choises = None
	type = None
	user_type = None
	
	def __init__(self, type=None, re=None, choises=None):
		if isinstance(type, basestring):
			if type in USER_TYPES:
				re = USER_TYPES[type]
				self.user_type = type
				type = None
			else:
				raise ValueError(MESSAGES['unknown_user_type'] % (type, ))
		if isinstance(re, basestring):
			re = globals()['re'].compile(re)
		self.re = re 
		self.type = type
		self.choises = choises


class param(object):
	names = None
	rule = None
	required = True
	
	def __init__(self, *names, **kwds):
		'''
		@keyword optional: set names as optional when True.
		@type optional: bool|rule
		
		@keyword required: set names as required when True.
		@type required: bool|rule
		
		@keyword re: regular expression to match over
		@type re: str|re
		
		@keyword choises: value choises list
		@type choies: list
		
		@keyword type: value type
		@keyword type: type|str
		'''
		self.names = names
		for key in ('optional', 'required'):
			if key in kwds:
				if isinstance(kwds[key], rule):
					self.rule = kwds[key]
				del kwds[key]
				if key == 'optional':
					self.required = False
		if not self.rule:
			self.rule = rule(**kwds)
	
	
	def __call__(self, fn):
		if not isinstance(fn, _func_wrapper):
			fn = _func_wrapper(fn)
		fn.params.append(self)
		return fn
		

class _func_wrapper(object):
	params = None
	def __init__(self, fn):
		self.fn = fn
		self.params = []
		
	def __call__(self, *args, **kwds):
		asp = inspect.getargspec(self.fn)
		values = dict(zip(asp.args, asp.defaults))
		values.update(kwds)
		validate(values, self.params)
		return self.fn(**kwds)
	
	
def validate(values, params):
	for name, value in values.items():
		for param in params:
			if name in param.names:
				if not param.required and not value:
					break
				
				rule = param.rule				
				if param.required and not value:
					raise ValueError(MESSAGES['empty'] % (name, ))
				if rule.re and not rule.re.search(value):
					if rule.user_type:
						raise ValueError(MESSAGES['type'] % (rule.user_type, name))
					else:
						raise ValueError(MESSAGES['re'] % (rule.re.pattern, name))
				if rule.choises and value not in rule.choises:
					raise ValueError(MESSAGES['choises'] % (str(rule.choises), name))
				if rule.type and not isinstance(value, rule.type):
					raise ValueError(MESSAGES['type'] % (rule.type.__name__, name))
