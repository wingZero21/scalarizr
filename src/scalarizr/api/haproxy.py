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

	'''
	@rpc.service_method
	@validate.param('port', 'server_port', type=int)
	@validate.param('protocol', required=_rule_protocol)
	@validate.param('server_port', optional=True, type=int)
	@validate.param('backend', optional=_rule_backend)'''
	def create_listener(self, port=None, protocol=None, server_port=None, 
					server_protocol=None, backend=None):

		ln = haproxy.naming('listen', protocol, port)
		bnd = haproxy.naming('backend', protocol, port, backend=backend)
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
				# >>> print srv
				# 248-64-125-158 248.64.125.158:1154 check
				# @fixme: why the value is server string? key is expected: 248-64-125-158  
				server = self.cfg['backend'][bnd]['server'][srv.split(' ')[0]]
				server.update({'check' : True})
				self.cfg['backend'][bnd]['server'][srv.split(' ')[0]] = server
		
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
			# @todo: implement __delitem__ and delete objects in ths manner:
			# del self.cfg['listen'][ln]
			self.cfg.conf.remove(self.cfg.listener[path].xpath)
			LOG.debug('HAProxyAPI.delete_listener: remove listener `%s`' % ln)
			
		if default_backend:
			has_ref = False
			for ln in self.cfg.listener:
				try:
					if self.cfg.listener[ln]['default_backend'] == default_backend:
						has_ref = True
						break
				except:
					pass
				
				'''
				if self.cfg.el_in_path(self.cfg.listener[lnr].xpath, default_backend):
					flag = False
					break
				'''
			if not has_ref:
				#not used in other section, so it will be deleting
				# @todo: del self.cfg.backends[default_backend]
				self.cfg.conf.remove(self.cfg.backends[default_backend].xpath)

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
		'''
		@fixme: why so messy?
		srv_name = self.server_name(ipaddr)
		for bd in self.cfg.sections(haproxy.naming('backend', backend=backend)):
			if srv_name in self.cfg.backends[bd]['server']:
				del self.cfg.backends[bd]['server']
		'''
		
		for path in self.cfg.sections(haproxy.naming('backend', backend=backend)):
			try:
				for el in self.cfg.backends[path]['server']:
					if el:
						index = 1
					else:
						raise
				for el in self.cfg.backends[path]['server']:
					if el.strip().startswith(ipaddr.replace('.', '-')):
						break
					index += 1
			except:
				index = -1
			if index != -1:
				self.cfg.conf.remove('%s[%s]' % (self.cfg.backends[path]['server'].xpath, index))

			self.cfg.save()
			self.svs.reload()


	#@rpc.service_method
	def list_listeners(self):
		'''
		@fixme: follow return format
		@return: Listeners list 
		@rtype: [{
			<port>, 
			<protocol>, 
			<server_port>, 
			<server_protocol>, 
			<backend>, 
			<servers>: [<ipaddr>, ...] 
		}, ...]
		'''
		self.cfg.reload()
		for ln in self.cfg.sections(haproxy.naming('listen')):
			listener = self.cfg.listener[ln]
			res = {}
			for option in list(set(self.cfg.conf.children(listener.xpath))):
				if	isinstance(listener[option], dict):
					tmp = {}
					for opt_str in listener[option]:
						opt_name = opt_str.strip().replace('\t',' ').split(' ')[0]
						opt_elem = {opt_name: listener[option][opt_name] or True}
						tmp.update(opt_elem) 
					res.update({option: tmp})
				else:
					res.update({option: listener[option]})
			#TODO: or we need to select only some params of `listen` section?, now it return all
			yield {ln: res}
		raise StopIteration()

	'''
	@rpc.service_method
	@validate.param('backend', optional=_rule_backend)'''
	def list_servers(self, backend=None):
		'''
		@fixme: follow descriptoin and return format		
		
		List all servers, or servers from particular backend
		@rtype: [<ipaddr>, ...]
		'''
		
		
		'''yield all servers inside `backend` or `listen` sections 
			@backend: str
			@return type: dict
		'''
		if backend:
			list_section = self.cfg.sections(haproxy.naming('backend', backend=backend))
		else:
			list_section = self.cfg.sections(haproxy.naming('backend'))
		for bnd in list_section:
			for srvstr in self.cfg.backends[bnd]['server']:
				srv_name = filter(None, map(string.strip, srvstr.replace('\t', ' ').split(' ')))[0]
				yield {srv_name: self.cfg.backends[bnd]['server'][srv_name]}
		raise StopIteration()