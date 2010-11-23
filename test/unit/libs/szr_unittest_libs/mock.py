'''
Created on Nov 4, 2010

@author: marat
'''

def _wrapper(reference): return (lambda *args: reference)

class Mock:
	
	def __init__(self, **kwargs):
		for method, reference in kwargs.items():
			setattr(self, method, _wrapper(reference))
				
class QueryEnvService(Mock):
	'''
	Usage:
	qe = QueryEnvService(
		list_roles=list(
			Role('www', 'ln-nginx-centos55', list(RoleHost('10.38.197.1'), RoleHost('10.38.197.2'))),
		),
		get_https_certificate = ('----- BEGIN CERTIFICATE...', '----- BEGIN PRIVATE KEY...')
	)
	bus.queryenv_service = qe
	'''
	pass

class MessageService(Mock):
	'''
	Send nothing
	'''
	pass

class MessageProducer(Mock):
	pass

class MessageConsumer(Mock):
	pass