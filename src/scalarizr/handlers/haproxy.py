

from scalarizr.bus import bus
from scalarizr.handlers import Handler, HandlerError
from scalarizr.api import haproxy as haproxy_api
from scalarizr.services import haproxy as haproxy_svs
from scalarizr.config import ScalarizrState
from scalarizr.messaging import Messages
from scalarizr.util import iptables 

import sys
import logging


def get_handlers():
	return [HAProxyHandler()]


LOG = logging.getLogger(__name__)


def _result_message(name):
	def result_wrapper(fn):
		LOG.debug('result_wrapper')
		def fn_wrapper(self, *args, **kwds):
			LOG.debug('fn_wrapper name = `%s`', name)
			result = self.new_message(name, body={'status': 'ok'})
			try:
				fn_return = fn(self, *args, **kwds)
				result.body.update(fn_return or {})
			except:
				result.body.update({'status': 'error', 'last_error': str(sys.exc_info)})
			self.send_message(result)
		return fn_wrapper
	return result_wrapper


class HAProxyMessages:
	HAPROXY_ADDSERVER = 'HAProxy_AddServer'
	HAPROXY_REMOVESERVER = 'HAProxy_RemoveServer'
	HAPROXY_CONFIGURE_HEALTHCHECK = 'HAProxy_ConfigureHealthcheck'
	HAPROXY_GETSERVERSHEALTH = 'HAProxy_GetServersHealth'
	HAPROXY_RESETHEALTHCHECK = 'HAProxy_ResetHealthcheck'
	HAPROXY_LISTLISTENERS = 'HAProxy_ListListeners',
	HAPROXY_LISTSERVERS = 'HAProxy_ListServers'

class HAProxyHandler(Handler):
	def __init__(self):
		self.api = haproxy_api.HAProxyAPI()
		self.on_reload()
		bus.on(init=self.on_init, reload=self.on_reload)

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		accept_res = haproxy_svs.BEHAVIOUR in behaviour and message.name in (
			#message.haproxy in message or
			#TODO: message.haproxy or haproxymessage?
			Messages.HOST_UP, Messages.HOST_DOWN, Messages.BEFORE_HOST_TERMINATE,
			HAProxyMessages.HAPROXY_ADDSERVER,
			HAProxyMessages.HAPROXY_CONFIGURE_HEALTHCHECK,
			HAProxyMessages.HAPROXY_GETSERVERSHEALTH,
			HAProxyMessages.HAPROXY_LISTLISTENERS,
			HAProxyMessages.HAPROXY_LISTSERVERS,
			HAProxyMessages.HAPROXY_REMOVESERVER,
			HAProxyMessages.HAPROXY_RESETHEALTHCHECK
			)
		LOG.debug('--------------------------------------------')
		LOG.debug('haproxy_svs.BEHAVIOUR = `%s`', haproxy_svs.BEHAVIOUR)
		LOG.debug('behaviour = `%s`', behaviour)
		LOG.debug('haproxy_svs.BEHAVIOUR in behaviour =`%s`', (haproxy_svs.BEHAVIOUR in behaviour))
		LOG.debug('message.name = `%s`', message.name)
		LOG.debug('`%s`', accept_res)
		LOG.debug('message = `%s`', message)
		LOG.debug('--------------------------------------------')
		return accept_res

	def on_init(self, *args, **kwds):
		bus.on(
			host_init_response=self.on_host_init_response,
			before_host_up=self.on_before_host_up,
		)

	def on_reload(self, *args):
		self.cnf = bus.cnf
		self.svs = haproxy_svs.HAProxyInitScript()

	def on_start(self):
		if bus.cnf.state == ScalarizrState.INITIALIZING:
			# todo: Repair data from HIR
			# 
			pass

	def on_host_init_response(self, msg):
		LOG.debug('HAProxyHandler.on_host_init_response')
		if not 'haproxy' in msg.body:
			raise HandlerError('HostInitResponse message for HAProxy behaviour must \
					have `haproxy` property')
		data = msg.haproxy.copy()
		
		self._listeners = data.get('listeners', [])
		self._healthchecks = data.get('healthchecks', [])
		LOG.debug('listeners = `%s`', self._listeners)
		LOG.debug('healthchecks = `%s`', self._healthchecks)

	def on_before_host_up(self, msg):
		LOG.debug('HAProxyHandler.on_before_host_up')

		try:
			if self.svs.status() != 0:
				self.svs.start()
		except:
			#TODO: if it not run and not starting, do we need raising exception or logging as error?
			LOG.error('Can`t start `haproxy`. Details: `%s`', sys.exc_info()[1])

		data = {'listeners': [], 'healthchecks': []}

		if isinstance(self._listeners, list):
			for ln in self._listeners:
				try:
					ln0 = self.api.create_listener(**ln)
					data['listeners'].append(ln0)
				except:
					LOG.error('HAProxyHandler.on_before_host_up. Failed to add listener'\
							' `%s`.', str(ln))
					#raise Exception, sys.exc_info()[1], sys.exc_info()[2]

		if isinstance(self._healthchecks, list):
			for hl in self._healthchecks:
				try:
					hl0 = self.api.configure_healthcheck(**hl)
					data['healthchecks'].append(hl0)
				except:
					LOG.error('HAProxyHandler.on_before_host_up. Failed to configure'\
							' healthcheck `%s`.', str(hl))
					#raise Exception, sys.exc_info()[1], sys.exc_info()[2]
		msg.haproxy = data

	def on_HostUp(self, msg):
		LOG.debug('------------------------')
		LOG.debug('Host_Up')
		LOG.debug('------------------------')
		self._farm_role_id = msg.body.get('farm_role_id')
		self._local_ip = msg.body.get('local_ip')
		try:
			self.api.add_server(ipaddr=self._local_ip, 
								backend='role:%s' % self._farm_role_id)
		except:
			LOG.error('HAProxyHandler.on_before_host_up. Failed add_server `%s`, details:'
					' %s' %	(self._local_ip, sys.exc_info()[1]))

	def on_HostDown(self, msg):
		LOG.debug('------------------------')
		LOG.debug('Host_Down')
		LOG.debug('------------------------')
		# Remove roles from backends
		self._farm_role_id = msg.body.get('farm_role_id')
		self._local_ip = msg.body.get('local_ip')
		try:
			self.api.remove_server(ipaddr=self._local_ip, 
								backend='role:%s' % self._farm_role_id)
		except:
			LOG.error('HAProxyHandler.on_before_host_Down. Failed remove server `%s`, '
					'details: %s' %	(self._local_ip, sys.exc_info()[1]))

	on_BeforeHostTerminate = on_HostDown


	def host_up(self, msg):
		LOG.debug('------------------------')
		LOG.debug('host_up')
		LOG.debug('------------------------')
		
	def host_down(self, msg):
		LOG.debug('------------------------')
		LOG.debug('host_down')
		LOG.debug('------------------------')

	@_result_message('HAProxy_AddServerResult')
	def on_HAProxy_AddServer(self, msg):
		return self.api.add_server(**msg.body)


	@_result_message('HAProxy_RemoveServerResult')
	def on_HAProxy_RemoveServer(self, msg):
		return self.api.remove_server(**msg.body)


	@_result_message('HAProxy_ConfigureHealthcheckResult')
	def on_HAProxy_ConfigureHealthcheck(self, msg):
		return self.api.configure_healthcheck(**msg.body)

	
	@_result_message('HAProxy_GetServersHealth')
	def on_HAProxy_GetServersHealth(self, msg):
		return {'health': self.api.get_servers_health()} 


	@_result_message('HAProxy_ResetHealthcheckResult')
	def on_HAProxy_ResetHealthcheck(self, msg):
		return self.api.reset_healthcheck(msg.target)


	@_result_message('HAProxy_ListListenersResult')
	def on_HAProxy_ListListeners(self, msg):
		return {'listeners': self.api.list_listeners()} 


	@_result_message('HAProxy_ListServersResult')
	def on_HAProxy_ListServers(self, msg):
		return {'servers': self.api.list_servers(msg.backend) }