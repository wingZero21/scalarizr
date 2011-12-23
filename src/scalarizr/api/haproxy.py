'''
Created on Nov 25, 2011

@author: marat
'''

from scalarizr import rpc, exceptions
from scalarizr.libs import validate
from scalarizr.services import haproxy


import logging


LOG = logging.getLogger(__name__)
HEALTHCHECK_DEFAULTS = {
	'timeout': '3s',  
	'interval': '30s', 
	'fall_threshold': 2, 
	'rise_threshold': 10
}

__rule_protocol = validate.rule(choises=['tcp', 'http'])
__rule_backend = validate.rule(re=r'^role:\d+$')
__rule_hc_target = validate.rule(re='^[tcp|http]:\d+$')


class HAProxyAPI(object):
	
	def __init__(self):
		self.cfg = haproxy.HAProxyCfg()
		self.svs = haproxy.HAProxyInitScript()


	@rpc.service_method
	@validate.param('port', 'server_port', type=int)
	@validate.param('protocol', required=__rule_protocol)
	@validate.param('server_port', optional=__rule_protocol)
	@validate.param('backend', optional=__rule_backend)
	def create_listener(self, port=None, protocol=None, server_port=None, 
					server_protocol=None, backend=None):

		ln = haproxy.naming('listener', protocol, port)
		bnd = haproxy.naming('backend', protocol, port, backend=backend)
		listener = backend = None

		if ln in self.cfg.listeners:
			raise exceptions.Duplicate('Listener %s:%s already exists' % (protocol, port))
		
		if protocol == 'tcp':
			listener = {'balance': 'roundrobin'}
		elif protocol == 'http':
			listener = {'option': {'forwardfor': True}}
		else:
			raise ValueError('Unexpected protocol: %s' % (protocol, ))
		# apply defaults
		listener.update({
			'bind': '*:%s' % port,
			'mode': protocol, 
			'default_backend': bnd
		})
			
		if protocol == 'tcp':
			backend = {}
		elif protocol == 'http':
			backend = {'option': {'httpchk': True}}
		else:
			raise ValueError('Unexpected protocol: %s' % (protocol, ))
		# apply defaults 
		backend.update({
			'mode': protocol,
			'timeout': HEALTHCHECK_DEFAULTS['timeout'],
			'default-server': {
				'fall': HEALTHCHECK_DEFAULTS['fall_threshold'],
				'rise': HEALTHCHECK_DEFAULTS['rise_threshold'],
				'inter': HEALTHCHECK_DEFAULTS['interval']
			}
		})
		
		# Apply changes
		with self.svs.trans(exit='running'):
			with self.cfg.trans(enter='reload', exit='working'):
				self.cfg['listen'][ln] = listener
				if not bnd in self.cfg.backend:
					self.cfg['backend'][bnd] = backend
				self.svs.reload()
	
	
	@rpc.service_method
	@validate.param('target', required=__rule_hc_target)
	@validate.param('interval', 'timeout', re=r'^\d+[sm]$')
	@validate.param('fall_threshold', 'rise_threshold', type=int)
	def configure_healthcheck(self, target=None, interval=None, timeout=None, 
							fall_threshold=None, rise_threshold=None):
		
		pass

	
	@rpc.service_method
	@validate.param('ipaddr', type='ipv4')
	@validate.param('backend', optional=__rule_backend)
	def add_server(self, ipaddr=None, backend=None):
		
		self.cfg.reload()
		bnds = self.cfg.sections(haproxy.naming('backend', backend=backend))
		if not bnds:
			if backend:
				raise exceptions.NotFound('Backend not found: %s' % (backend, ))
			else:
				raise exceptions.Empty('No listeners to add server to')
			
		with self.svs.trans(exit='running'):
			with self.cfg.trans(exit='working'):
				server = {
					'name': ipaddr.replace('.', '-'),
					'address': ipaddr,
					'check': True
				}
				for bnd in bnds:
					self.cfg.backend[bnd]['server'].add(server)
				self.svs.reload()
					
	
	@rpc.service_method
	@validate.param('ipaddr', type='ipv4', optional=True)
	def get_servers_health(self, ipaddr=None):
		pass
	
	
	@rpc.service_method
	@validate.param('port', type=int)
	@validate.param('protocol', required=__rule_protocol)
	def delete_listener(self, port=None, protocol=None):
		pass
	
	
	@rpc.service_method
	@validate.param('target', required=__rule_hc_target)
	def reset_healthcheck(self, target):
		pass
	
	
	@rpc.service_method
	@validate.param('ipaddr', type='ipv4')
	@validate.param('backend', optional=__rule_backend)
	def remove_server(self, ipaddr=None, backend=None):
		pass
	
	
	@rpc.service_method
	def list_listeners(self):
		self.cfg.reload()
		for ln in self.cfg.sections(haproxy.naming('listener')):
			try:
				listener = self.cfg.listener[ln]
				yield {
					'port': None,
					'protocol': listener['mode'],
					'server_port': None,
					'server_protocol': None,
					'backend': None
				}
			except:
				LOG.exception('Iteration failed')
		raise StopIteration()

	
	@rpc.service_method
	@validate.param('backend', optional=__rule_backend)
	def list_servers(self, backend=None):
		pass
	
