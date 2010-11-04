'''
Created on Nov 4, 2010

@author: marat
'''

class QueryEnvService:
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
	
	def __init__(self, **kwargs):
		pass


class MessageService:
	'''
	Send nothing
	'''
	pass