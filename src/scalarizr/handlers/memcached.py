'''
Created on Jul 23, 2010

@author: marat
@author: shaitanich
'''

# Core
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours
from scalarizr.service import CnfController, CnfPreset, Options
from scalarizr.handlers import ServiceCtlHanler, HandlerError
from scalarizr.messaging import Messages

# Libs
from scalarizr.util import disttool, initdv2
from scalarizr.util.filetool import read_file, write_file
from scalarizr.util.iptables import IpTables, RuleSpec, P_TCP

# Stdlibs
import logging, re, os

		
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
BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.MEMCACHED

# FIXME: use manifest
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
		CnfController.__init__(self, BEHAVIOUR, mcd_conf_path, 'memcached')
	
	def current_preset(self):
		self._logger.debug('Getting current Memcached preset')	
		preset = CnfPreset(name='System')
		
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

class MemcachedHandler(ServiceCtlHanler):
	_logger = None
	_queryenv = None
	_ip_tables = None
	_port = None
	
	def __init__(self):
		ServiceCtlHanler.__init__(self, SERVICE_NAME, initdv2.lookup('memcached'), MemcachedCnfController())
		
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._ip_tables = IpTables()
		self._port = 11211

		bus.on("init", self.on_init)

	def on_init(self):
		bus.on("before_host_up", self.on_before_host_up)
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name in (Messages.HOST_UP, Messages.HOST_DOWN, Messages.UPDATE_SERVICE_CONFIGURATION) \
				and BEHAVIOUR in behaviour
	
	def on_before_host_up(self, message):
		# Collect farm servers IP-s					
		ips = []
		for role in self._queryenv.list_roles():
			for host in role.hosts:
				ips.append(host.internal_ip or host.external_ip)
		
		# Allow access from all these IP-s
		rules = []
		for ip in ips:
			allow_rule = RuleSpec(source=ip, protocol=P_TCP, dport=self._port, jump='ACCEPT')
			rules.append(allow_rule)
		
		# Deny from all
		drop_rule = RuleSpec(protocol=P_TCP, dport=self._port, jump='DROP')
		rules.append(drop_rule)
			
		# Apply iptables rules
		for rule in rules:
			self._ip_tables.append_rule(rule)
			
		# Service configured
		bus.fire('service_configured', service_name=SERVICE_NAME)
	
	def on_HostUp(self, message):
		# Append new server to allowed list
		ip = message.local_ip or message.remote_ip
		rule = RuleSpec(source=ip, protocol=P_TCP, dport=self._port, jump='ACCEPT')
		self._ip_tables.insert_rule(None, rule)
		
		
	def on_HostDown(self, message):
		# Remove terminated server from allowed list
		ip = message.local_ip or message.remote_ip
		rule = RuleSpec(source=ip, protocol=P_TCP, dport=self._port, jump='ACCEPT')
		self._ip_tables.delete_rule(rule)

