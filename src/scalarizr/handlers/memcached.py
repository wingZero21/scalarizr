'''
Created on Jul 23, 2010

@author: marat
@author: shaitanich
'''
from scalarizr.handlers import HandlerError
from scalarizr.bus import bus
from scalarizr.config import ScalarizrState, BuiltinBehaviours
from scalarizr.handlers import Handler, ServiceCtlHanler
from scalarizr.util import disttool
from scalarizr.util.filetool import read_file, write_file
from scalarizr.util import iptables
from scalarizr.util.iptables import IpTables, RuleSpec
from scalarizr.messaging import Messages
from scalarizr.util import initdv2
import logging
import re
import os
from scalarizr.service import CnfController, CnfPreset, Options

		
if disttool._is_debian_based:
	mcd_conf_path = '/etc/memcached.conf' 
	expression = re.compile('^\s*-m\s*\d*$', re.M) 
	mem_re = re.compile('^-m\s+(?P<memory>\d+)\s*$', re.M)
	template = '-m AMOUNT' 
else:
	mcd_conf_path = '/etc/sysconfig/memcached'
	expression = re.compile('^\s*CACHESIZE\s*=\s*"\d*"$', re.M)
	mem_re = re.compile('^\s*CACHESIZE\s*=\s*"(?P<memory>\d+)"\s*$', re.M)
	template = 'CACHESIZE="AMOUNT"' 	

def set_cache_size(sub):
		mcd_conf = read_file(mcd_conf_path)
	
		if mcd_conf:
			if expression.findall(mcd_conf):
				write_file(mcd_conf_path, re.sub(expression, sub, mcd_conf))
			else:
				write_file(mcd_conf_path, sub, mode='a')
		print ">>>", read_file(mcd_conf_path)
	
def get_cache_size():
	mcd_conf = read_file(mcd_conf_path)
	if mcd_conf:
		result = re.search(mem_re, mcd_conf)
		if result:
			return result.group('memory')
		else: 
			return MemcachedCnfController.options.cache_size.default_value
			

class MemcachedInitScript(initdv2.ParametrizedInitScript):
	def __init__(self):
		
		pid_file = None
		if disttool.is_redhat_based():
			pid_file = "/var/run/memcached/memcached.pid"
		elif disttool.is_debian_based():
			pid_file = "/var/run/memcached.pid" 
		
		initd_script = '/etc/init.d/memcached'
		if not os.path.exists(initd_script):
			raise HandlerError("Cannot find Memcached init script at %s. Make sure that memcached is installed" % initd_script)

		initdv2.ParametrizedInitScript.__init__(self, 'cassandra', initd_script, pid_file, socks=[initdv2.SockParam(11211)])

initdv2.explore('memcached', MemcachedInitScript)
BEHAVIOUR = BuiltinBehaviours.MEMCACHED


class MemcachedCnfController(CnfController):
	
	class OptionSpec:
		name = None
		get_func = None
		default_value = None
		set_func = None
		
		def __init__(self, name, get_func, set_func, default_value = None):
			self.name = name	
			self.get_func = get_func	
			self.set_func = set_func
			self.default_value = default_value
			
	options = Options(
		OptionSpec('cache_size', get_cache_size, set_cache_size,'64')
		)
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)		
	
	def current_preset(self):
		self._logger.debug('Getting current Memcached preset')	
		preset = CnfPreset(name='current')
		
		vars = {}
		
		for option_spec in self.options:
			current_value = option_spec.get_func()
			vars[option_spec.name] = current_value if current_value else option_spec.default_value
		
		preset.settings = vars
		return preset
		
	def apply_preset(self, preset):	
		self._logger.debug('Applying %s preset' % (preset.name if preset.name else 'undefined'))
			
		for option_spec in self.options:
			if preset.settings.has_key(option_spec.name):
				current_value = option_spec.get_func()
				
				if preset.settings[option_spec.name] == current_value:
					self._logger.debug('%s wasn`t changed.' % option_spec.name)
				else:
					option_spec.set_func(template.replace('AMOUNT', preset.settings[option_spec.name]))


def get_handlers():
	return [MemcachedHandler()]

class MemcachedHandler(Handler):
	
	_logger = None
	
	def __init__(self):
		self._queryenv = bus.queryenv_service
		self._logger = logging.getLogger(__name__)
		
		config = bus.config
		cache_size = config.get(BEHAVIOUR, 'cache_size')
		self.substitute = template.replace('AMOUNT', cache_size)
		
		self._initd = initdv2.lookup('memcached')
		self.ip_tables = IpTables()
		self.rules = []
		ServiceCtlHanler.__init__(self, BEHAVIOUR, self._initd, MemcachedCnfController())
		bus.on("init", self.on_init)
		bus.on("before_host_down", self.on_before_host_down)
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.HOST_UP \
			or message.name == Messages.HOST_DOWN 	
	
	
	def on_init(self):
		bus.on("start", self.on_start)
		bus.on("before_host_up", self.on_before_host_up)


	def on_start(self, *args):
		if self._cnf.state == ScalarizrState.RUNNING:
			try:
				self._logger.info("Starting memcached")
				self._initd.start()
			except initdv2.InitdError, e:
				self._logger.error(e)
	
	def on_before_host_up(self, message):
		
		set_cache_size(self.substitute)
							
		ips = []
		roles = self._queryenv.list_roles()
		
		for role in roles:
			for host in role.hosts:
				ips.append(host.internal_ip or host.external_ip)
		
		for ip in ips:
			allow_rule = RuleSpec(source=ip, protocol=iptables.P_TCP, dport='11211', jump='ACCEPT')
			self.rules.append(allow_rule)
		
		drop_rule = RuleSpec(protocol=iptables.P_TCP, dport='11211', jump='DROP')
		self.rules.append(drop_rule)
			
		for rule in self.rules:
			self.ip_tables.append_rule(rule)


		try:
			self._logger.info("Reloading memcached")
			self._initd.reload()
		except initdv2.InitdError, e:
			self._logger.error(e)


	def on_before_host_down(self, *args):
		try:
			self._logger.info("Stopping memcached")
			self._initd.stop()
		except initdv2.InitdError:
			self._logger.error("Cannot stop memcached")
			if self._initd.running:
				raise
	
	
	def on_HostUp(self, message):
		# Adding iptables rules
		if message.behaviour == BuiltinBehaviours.MEMCACHED:
			if not message.local_ip:
				ip = message.remote_ip
			else:
				ip = message.local_ip
				
			rule = RuleSpec(source=ip, protocol=iptables.P_TCP, dport='11211', jump='ACCEPT')
			self.ip_tables.insert_rule(None, rule)
		
		
	def on_HostDown(self, message):
		if message.behaviour == BuiltinBehaviours.MEMCACHED:
			
			if not message.local_ip:
				ip = message.remote_ip
			else:
				ip = message.local_ip

			rule = RuleSpec(source=ip, protocol=iptables.P_TCP, dport='11211', jump='ACCEPT')
			self.ip_tables.delete_rule(rule)

		"""
		for rule in self.rules:
			self.ip_tables.delete_rule(rule)
		"""