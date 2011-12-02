

from scalarizr.bus import bus
from scalarizr.handlers import Handler, HandlerError
from scalarizr.api.haproxy import HAProxyAPI
from scalarizr.services.haproxy import HAProxyInitScript
from scalarizr.config import ScalarizrState


import sys
import logging



def get_handlers():
	pass


LOG = logging.getLogger(__name__)

class HAProxyHandler(Handler):
	def __init__(self):
		self.api = HAProxyAPI()
		self.on_reload()
	
	def on_init(self, *args, **kwds):
		bus.on(
			host_init_response=self.on_host_init_response,
			before_hostup=self.on_before_hostup
		)
		
	def on_reload(self, *args):
		self.cnf = bus.cnf
		self.svs = HAProxyInitScript()
		
	def on_start(self):
		if bus.cnf.state == ScalarizrState.INITIALIZING:
			# todo: Repair data from HIR
			# 
			pass
		
	def on_host_init_response(self, msg):
		if not 'haproxy' in msg.body:
			raise HandlerError("HostInitResponse message for HAProxy behaviour must have 'haproxy' property")
		data = msg.haproxy.copy()
		self._listeners = data.get('listeners', [])
		self._healthchecks = data.get('healthchecks', [])
	
	
	def on_before_hostup(self, msg):
		data = {'listeners': [], 'healthchecks': []}
		
		for ln in self._listeners:
			try:
				ln0 = self.api.create_listener(**ln)
				data['listeners'].append(ln0)
			except:
				LOG.error('Failed to add listener %s', str(ln))
				
		for hl in self._healthchecks:
			try:
				hl0 = self.api.configure_healthcheck(**hl)
				data['healthchecks'].append(hl0)
			except:
				LOG.error('Failed to configure healthcheck %s', str(hl))
				
		msg['haproxy'] = data
		self.svs.start()
		
	
	def on_HostUp(self, msg):
		# Add roles to backends
		pass
	
	
	def on_HostDown(self, msg):
		# Remove roles from backends
		
		pass
	
		
	@__result_message('HAProxy_AddServerResult')
	def on_HAProxy_AddServer(self, msg):
		return self.api.add_server(**msg.body)
		
	
	@__result_message('HAProxy_RemoveServerResult')
	def on_HAProxy_RemoveServer(self, msg):
		return self.api.remove_server(**msg.body)
		
	
	@__result_message('HAProxy_ConfigureHealthcheckResult')
	def on_HAProxy_ConfigureHealthcheck(self, msg):
		return self.api.configure_healthcheck(**msg.body)

	
	@__result_message('HAProxy_GetServersHealth')
	def on_HAProxy_GetServersHealth(self, msg):
		return {'health': self.api.get_servers_health()} 


	@__result_message('HAProxy_ResetHealthcheckResult')
	def on_HAProxy_ResetHealthcheck(self, msg):
		return self.api.reset_healthcheck(msg.target)

	
	@__result_message('HAProxy_ListListenersResult')
	def on_HAProxy_ListListeners(self, msg):
		return {'listeners': self.api.list_listeners()} 
	
	
	@__result_message('HAProxy_ListServersResult')
	def on_HAProxy_ListServers(self, msg):
		return {'servers': self.api.list_servers(msg.backend) }
	
	
def __result_message(name):
	def result_wrapper(fn):
		def fn_wrapper(self, *args, **kwds):
			result = self.new_message(name, body={'status': 'ok'})
			try:
				fn_return = fn(self, *args, **kwds)
				result.body.update(fn_return or {})
			except:
				result.body.update({'status': 'error', 'last_error': str(sys.exc_value)})
			self.send_message(result)
	return result_wrapper()

