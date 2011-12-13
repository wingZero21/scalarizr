'''
Created on Nov 25, 2011

@author: marat
'''

from scalarizr.rpc import service_method
from scalarizr.services import haproxy
from scalarizr import exceptions

import logging


LOG = logging.getLogger(__name__)
HEALTHCHECK_DEFAULTS = {
	'timeout': '3s',  
	'interval': '30s', 
	'fall_threshold': 2, 
	'rise_threshold': 10
}

class HAProxyAPI(object):
	
	def __init__(self):
		self.cfg = haproxy.HAProxyCfg()
		self.svs = haproxy.HAProxyInitScript()

	@service_method
	def create_listener(self, port=None, protocol=None, server_port=None, 
					server_protocol=None, backend=None):
		assert port
		assert protocol
		assert server_port
		if not server_protocol:
			server_protocol = protocol
			
		# check data:
		# * protocol, server_protocol
		# * backend 
		
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
				self.cfg.listener[ln] = listener
				if not bnd in self.cfg.backend:
					self.cfg.backend[bnd] = backend
				self.svs.reload()
				
		
	
	@service_method
	def configure_healthcheck(self, target=None, interval=None, timeout=None, 
							fall_threshold=None, rise_threshold=None):
		assert target
		assert interval
		assert timeout
		assert fall_threshold
		assert rise_threshold

	
	@service_method
	def add_server(self, ipaddr=None, backend=None):
		assert ipaddr
		
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
					
	
	@service_method
	def get_servers_health(self, ipaddr=None):
		pass
	
	@service_method
	def delete_listener(self, port, protocol):
		pass
	
	@service_method
	def reset_healthcheck(self, target):
		pass
	
	@service_method
	def remove_server(self, ipaddr, backend=None):
		pass
	
	@service_method
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

	
	@service_method
	def list_servers(self, backend=None):
		pass