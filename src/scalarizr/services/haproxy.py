'''
Created on Nov 25, 2011

@author: marat
'''

from scalarizr.util import initdv2
from scalarizr.util import disttool as dist
from scalarizr.util import filetool
from scalarizr.util import which


class HAProxyCfg(filetool.ConfigurationFile):
	'''
	haproxy.cfg configuration object.
	- access configuration in a dict-like way 
	- wrap all setting in standart python types
	
	Create object:
	>> cfg = HaProxyCfg()
	

	Access settings:
	>> cfg['global']['stats']['socket']
	/var/run/haproxy-stats.sock
	
	>> cfg['backend']['scalr:backend:role:456:port:8080']['server']
	[{'name': 'srv0', 'address': '10.156.7.18', 'port': 1234, 'check': True},
	{'name': 'srv1', 'address': '10.156.26.59', 'port': 1234, 'check': True}]
	
	>> cfg['listen']['scalr:listener:tcp:12345']['option']
	{'forwardfor': True, 'httpchk': True}
	
	
	Modify settings:
	>> cfg['listen']['scalr:listener:tcp:12345'] = {
	'bind': '*:12345', 
	'mode': 'tcp', 
	'balance': 'roundrobin', 
	'option': {'tcplog': True}, 
	'default_backend': 'scalr:backend:port:1234'
	}
	
	
	Find sections:
	>> cfg.sections('scalr:backend')
	['scalr:backend:port:1234', 'scalr:backend:role:456:port:8080']
	'''
	
	DEFAULT = '/etc/haproxy/haproxy.cfg'
	
	def __init__(self, path=None):
		super(HAProxyCfg, self).__init__(path or self.DEFAULT)
	
	def sections(self, filter=None):
		raise NotImplemented()


class StatSocket(object):
	'''
	haproxy unix socket API
	- one-to-one naming
	- connect -> request -> disconnect  
	
	Create object:
	>> ss = StatSocket('/var/run/haproxy-stats.sock')
	
	Show stat:
	>> ss.show_stat()
	[{'status': 'UP', 'lastchg': '68', 'weight': '1', 'slim': '', 'pid': '1', 'rate_lim': '', 
	'check_duration': '0', 'rate': '0', 'req_rate': '', 'check_status': 'L4OK', 'econ': '0', 
	'wredis': '0', 'dresp': '0', 'ereq': '', None: [''], 'tracked': '', 'pxname': 'scalr:backend:port:1234', 
	'dreq': '', 'hrsp_5xx': '', 'check_code': '', 'sid': '1', 'bout': '0', 'hrsp_1xx': '', 
	'qlimit': '', 'hrsp_other': '', 'bin': '0', 'smax': '0', 'req_tot': '', 'lbtot': '0', 
	'stot': '0', 'wretr': '0', 'req_rate_max': '', 'iid': '1', 'hrsp_4xx': '', 'chkfail': '0', 
	'hanafail': '0', 'downtime': '0', 'qcur': '0', 'eresp': '0', 'cli_abrt': '0', 'srv_abrt': '0', 
	'throttle': '', 'scur': '0', 'type': '2', 'bck': '0', 'qmax': '0', 'rate_max': '0', 'hrsp_2xx': '', 
	'act': '1', 'chkdown': '0', 'svname': 'srv0', 'hrsp_3xx': ''}]
	'''
	
	def __init__(self, address):
		pass
	
	def show_stat(self):
		'''
		@rtype: list[dict] 
		'''
		raise NotImplemented()


def naming(type_, *args, **kwds):
	#ln = 'scalr:listener:%s:%s' % (protocol, port)
	#bnd = 'scalr:backend:%s%s:%s' % (backend and backend + ':' or '', protocol, port)
	raise NotImplemented()

	
class HAProxyInitScript(initdv2.InitScript):
	'''
	haproxy init script
	- start
	- stop
	- restart
	- reload
	- status
	
	see MySQL, Apache init scripts
	'''
	
	def __init__(self):
		pass
