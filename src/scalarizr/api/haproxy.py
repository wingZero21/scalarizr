'''
Created on Nov 25, 2011

@author: marat
'''

from scalarizr import rpc, exceptions
from scalarizr.libs import validate
from scalarizr.services import haproxy


import logging
import sys
import string

LOG = logging.getLogger(__name__)
HEALTHCHECK_DEFAULTS = {
	'timeout': {'check':'3s'},  
	'interval': '30s',
	'fall_threshold': 2,
	'rise_threshold': 10
}

_rule_protocol = validate.rule(choises=['tcp', 'http'])
_rule_backend = validate.rule(re=r'^role:\d+$')
_rule_hc_target = validate.rule(re='^[tcp|http]:\d+$')


class HAProxyAPI(object):

	def __init__(self, path=None):
		self.path_cfg = path
		self.cfg = haproxy.HAProxyCfg(path)
		self.svs = haproxy.HAProxyInitScript(path)
	
	def __get(self, obj, key):
		try:
			return obj[key]
		except KeyError:
			return None
		except Exception:
			raise Exception, sys.exc_info()[1], sys.exc_info()[2]
	
	def _server_name(self, ipaddr):
		'''@rtype: str'''
		if ':' in ipaddr:
			ipaddr = ipaddr.strip().split(':')[0]
		return ipaddr.replace('.', '-')
	
	'''
	@rpc.service_method
	@validate.param('port', 'server_port', type=int)
	@validate.param('protocol', required=_rule_protocol)
	@validate.param('server_port', optional=True, type=int)
	@validate.param('backend', optional=_rule_backend)'''
	def create_listener(self, port=None, protocol=None, server_port=None, 
					server_protocol=None, backend=None):

		ln = haproxy.naming('listen', protocol, port)
		bnd = haproxy.naming('backend', server_protocol or protocol, server_port or port, backend=backend)
		listener = backend = None
		LOG.debug('HAProxyAPI.create_listener: listener = `%s`, backend = `%s`', ln, bnd)

		try:
			if self.cfg.listener[ln]:
				raise 'Duplicate'
		except Exception, e:
			if 'Duplicate' in e:
				raise exceptions.Duplicate('Listener %s:%s already exists' % (protocol, port))
		
		if protocol == 'tcp': 
			listener = {'balance': 'roundrobin'}
		elif protocol == 'http':
			listener = {'option': {'forwardfor': True}}
		else:
			raise ValueError('Unexpected protocol: %s' % (protocol, ))
			#TODO: not correct for https or ssl...
		# apply defaults
		listener.update({
			'bind': '*:%s' % port,
			'mode': protocol,
			'default_backend': bnd
		})
		
		if (server_protocol or protocol) == 'tcp':
			backend = {}
		elif (server_protocol or protocol) == 'http':
			backend = {'option': {'httpchk': True}}
		else:
			raise ValueError('Unexpected protocol: %s' % (protocol, ))
			#TODO: not correct for https or ssl...
		# apply defaults
		backend.update({
			'mode': server_protocol or protocol,
			'timeout': HEALTHCHECK_DEFAULTS['timeout'],
			'default-server': {
				'fall': HEALTHCHECK_DEFAULTS['fall_threshold'],
				'rise': HEALTHCHECK_DEFAULTS['rise_threshold'],
				'inter': HEALTHCHECK_DEFAULTS['interval']
			}
		})
		# Apply changes
		#with self.svs.trans(exit='running'):
		#	with self.cfg.trans(enter='reload', exit='working'):
		#TODO: change save() and reload(),`if True` condition to `with...` enter, exit
		if True:
				self.cfg['listen'][ln] = listener
				if not self.cfg.backend or not bnd in self.cfg.backend:
					self.cfg['backend'][bnd] = backend

				self.cfg.save()
				self.svs.reload()


	'''
	@rpc.service_method
	@validate.param('target', required=_rule_hc_target)
	@validate.param('interval', 'timeout', re=r'^\d+[sm]$')
	@validate.param('fall_threshold', 'rise_threshold', type=int)'''
	def configure_healthcheck(self, target=None, interval=None, timeout=None, 
							fall_threshold=None, rise_threshold=None):

		bnds = haproxy.naming('backend', backend=target)  
		if not self.cfg.sections(bnds):
			raise exceptions.NotFound('Backend `%s` not found' % target)

		for bnd in self.cfg.sections(bnds):
			if timeout:
				self.cfg['backend'][bnd]['timeout'] = timeout

			default_server = {
				'inter': interval,
				'fall': fall_threshold,
				'rise': rise_threshold
			}

			self.cfg['backend'][bnd]['default-server'] = default_server

			for srv in self.cfg['backend'][bnd]['server']:
				server = self.cfg['backend'][bnd]['server'][srv]
				server.update({'check' : True})
				self.cfg['backend'][bnd]['server'][srv] = server

		#with self.svs.trans(exit='running'):
			#	with self.cfg.trans(enter='reload', exit='working'):

		self.cfg.save()
		self.svs.reload()


	'''
	@rpc.service_method
	@validate.param('ipaddr', type='ipv4')
	@validate.param('backend', optional=_rule_backend)'''
	def add_server(self, ipaddr=None, backend=None):
		'''Add server with ipaddr in backend section''' 
		self.cfg.reload()
		LOG.debug('HAProxyAPI.add_server')
		LOG.debug('	%s' % haproxy.naming('backend', backend=backend))
		bnds = self.cfg.sections(haproxy.naming('backend', backend=backend))

		if not bnds:
			if backend:
				raise exceptions.NotFound('Backend not found: %s' % (backend, ))
			else:
				raise exceptions.Empty('No listeners to add server to')

		#with self.svs.trans(exit='running'):
			#with self.cfg.trans(exit='working'):
		if True:
				for bnd in bnds:
					server = {
						'address': ipaddr,
						'port': bnd.split(':')[-1],
						'check': True
					}
					self.cfg.backends[bnd]['server'][ipaddr.replace('.', '-')] = server

				self.cfg.save()
				self.svs.reload()

	'''
	@rpc.service_method
	@validate.param('ipaddr', type='ipv4', optional=True)'''
	def get_servers_health(self, ipaddr=None):
		#TODO: select parameters what we need with filter by ipaddr
		stats = haproxy.StatSocket().show_stat()
		return stats
	'''
	@rpc.service_method
	@validate.param('port', type=int)
	@validate.param('protocol', required=_rule_protocol)'''
	def delete_listener(self, port=None, protocol=None):
		''' Delete listen section(s) by port (and)or protocol '''

		ln = haproxy.naming('listen', protocol, port)
		if not self.cfg.sections(ln):
			raise exceptions.NotFound('Listen `%s` not found can`t remove it' % ln)
		try:
			default_backend = self.cfg.listener[ln]['default_backend']
		except:
			default_backend = None

		for path in self.cfg.sections(ln):
			del self.cfg['listen'][ln]
			LOG.debug('HAProxyAPI.delete_listener: removed listener `%s`' % ln)
			
		if default_backend:
			has_ref = False
			for ln in self.cfg.listener:
				try:
					if self.cfg.listener[ln]['default_backend'] == default_backend:
						has_ref = True
						break
				except:
					pass
			if not has_ref:
				#it not used in other section, so will be deleting
				del self.cfg.backends[default_backend]

		self.cfg.save()
		self.svs.reload()

	'''
	@rpc.service_method
	@validate.param('target', required=_rule_hc_target)'''
	def reset_healthcheck(self, target):		
		'''Return to defaults for `tartget` backend sections'''
		bnds = haproxy.naming('backend', backend=target)
		if not self.cfg.sections(bnds):
			raise exceptions.NotFound('Backend `%s` not found' % target)
		for bnd in self.cfg.sections(bnds):
			self.cfg['backend'][bnd]['default-server'] = {
				'fall': HEALTHCHECK_DEFAULTS['fall_threshold'],
				'rise': HEALTHCHECK_DEFAULTS['rise_threshold'],
				'inter': HEALTHCHECK_DEFAULTS['interval']
				}
			self.cfg['backend'][bnd]['timeout'] = HEALTHCHECK_DEFAULTS['timeout'] 
			
		#with self.svs.trans(exit='running'):
			#	with self.cfg.trans(enter='reload', exit='working'):
			#TODO: with...
		self.cfg.save()
		self.svs.reload()


	'''
	@rpc.service_method
	@validate.param('ipaddr', type='ipv4')
	@validate.param('backend', optional=_rule_backend)'''
	def remove_server(self, ipaddr=None, backend=None):
		'''Removing server from backend secection with ipaddr'''
		srv_name = self._server_name(ipaddr)
		for bd in self.cfg.sections(haproxy.naming('backend', backend=backend)):
			if srv_name in self.cfg.backends[bd]['server']:
				del self.cfg.backends[bd]['server'][srv_name]

		self.cfg.save()
		self.svs.reload()


	#@rpc.service_method
	def list_listeners(self):
		'''
		@return: Listeners list 
		@rtype: [{
			<port>, 
			<protocol>,
			<server_port>,
			<server_protocol>, 
			<backend>, 
			<servers>: [<ipaddr>, ...]
		}, ...]'''
		self.cfg.reload()
		res = []
		for ln in self.cfg.sections(haproxy.naming('listen')):
			listener = self.cfg.listener[ln]
			bnd_name = listener['default_backend']
			bnd = self.cfg.backends[bnd_name]

			tmp = {
					'port': listener['bind'].replace('*:',''),
					'protocol': self.__get(listener, 'mode'),
					'server_port': bnd_name.split(':')[-1],
					'server_protocol': self.__get(bnd, 'mode'),
					'backend': bnd_name,
				}
			res.append(tmp)
		return res


	'''
	@rpc.service_method
	@validate.param('backend', optional=_rule_backend)'''
	def list_servers(self, backend=None):
		'''
		List all servers, or servers from particular backend
		@rtype: [<ipaddr>, ...]
		'''
		list_section = self.cfg.sections(haproxy.naming('backend', backend=backend))

		res = []
		for bnd in list_section:
			for srv_name in self.cfg.backends[bnd]['server']:
				res.append(self.cfg.backends[bnd]['server'][srv_name]['address'])
		return res