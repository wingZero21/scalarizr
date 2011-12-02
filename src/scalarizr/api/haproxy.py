'''
Created on Nov 25, 2011

@author: marat
'''

from scalarizr.rpc import service_method
from scalarizr.services import haproxy

class HAProxyAPI(object):
	
	def __init__(self):
		self.cfg = haproxy.HAProxyCfg()
		self.svs = haproxy.HAProxyInitScript()
		self.healthcheck_defaults = {
			'timeout': '3s',  
			'interval': '30s', 
			'fall_threshold': 2, 
			'rise_threshold': 10
		}

	@service_method
	def create_listener(self, port=None, protocol=None, server_port=None, 
					server_protocol=None, backend=None):
		assert port
		assert protocol
		assert server_port
		if not server_protocol:
			server_protocol = protocol
		
		self.cfg.add_listener()
		self.cfg.add_backend()

		with self.svs.running_on_exit():
			with self.cfg.save():
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
	def add_server(self, ipaddr, backend=None):
		pass
	
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
		pass
	
	@service_method
	def list_servers(self, backend=None):
		pass