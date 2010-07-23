'''
Created on Jul 23, 2010

@author: marat
@author: shaitanich
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler, HandlerError, lifecircle
from scalarizr.util import disttool, initd
from scalarizr.util.filetool import read_file, write_file
from scalarizr.util import iptables
from scalarizr.util.iptables import IpTables, RuleSpec
import logging
import re
import os

file = '/var/run/memcached.pid'
pid_file = file if os.path.exists(file) else None

initd_script = '/etc/init.d/memcached'
if not os.path.exists(initd_script):
	raise HandlerError("Cannot find Apache init script at %s. Make sure that apache web server is installed" % initd_script)

# Register memcached service
logger = logging.getLogger(__name__)
logger.debug("Explore memcached service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("memcached", initd_script, pid_file)

def get_handlers():
	return [MemcachedHandler()]

class MemcachedHandler(Handler):
	
	_logger = None
	
	def __init__(self):
		self._queryenv = bus.queryenv_service
		self._logger = logging.getLogger(__name__)
		self.debian_re = re.compile('^\s*-m\s*\d*$', re.M) 
		self.redhat_re = re.compile('^\s*CACHESIZE\s*=\s*"\d*"$', re.M)
		self.mcd_conf_path_deb = '/etc/memcached.conf' 
		self.mcd_conf_path_redhat = '/etc/sysconfig/memcached'
		self.ip_tables = IpTables()
		self.rules = []
	
	def on_init(self):
		bus.on("start", self.on_start)
		bus.on("before_host_up", self.on_before_host_up)

	def on_start(self):
		if lifecircle.get_state() == lifecircle.STATE_RUNNING:
			try:
				self._logger.info("Starting memcached")
				initd.start("memcached")
			except initd.InitdError, e:
				self._logger.error(e)
	
	def on_before_host_up(self):
		config = bus.config
		cache_size = config.get('behaviour_memcached','cache_size')
			
		if disttool._is_debian_based:
			mcd_conf_path = self.mcd_conf_path_deb
			expression = self.debian_re
			substitute = '-m %s' % cache_size
		else:
			mcd_conf_path = self.mcd_conf_path_redhat
			expression = self.redhat_re
			substitute = 'CACHESIZE="%s"' % cache_size
			
		mcd_conf = read_file(mcd_conf_path, logger=self._logger)
		
		if mcd_conf:
			if expression.findall(mcd_conf):
				write_file(mcd_conf_path, re.sub(expression, substitute, mcd_conf), logger=self._logger)
			else:
				write_file(mcd_conf_path, substitute, mode='a', logger = self._logger)
				
		try:
			self._logger.info("Reloading memcached")
			initd.reload("memcached", force=True)
		except initd.InitdError, e:
			self._logger.error(e)

	
	def on_HostUp(self):
		# Adding iptables rules
		ips = []
		
		for role in self._queryenv.list_roles():
			for host in role.hosts:
				ips.append(host.internal_ip or host.external_ip)
		
		for ip in ips:
			allow_rule = RuleSpec(source=ip, protocol=iptables.P_TCP, dport='11211', jump='ACCEPT')
			self.rules.append(allow_rule)
		
		drop_rule = RuleSpec(protocol=iptables.P_TCP, dport='11211', jump='DROP')
		self.rules.append(drop_rule)
			
		for rule in self.rules:
			self.ip_tables.append_rule(rule)
		
		
	def on_HostDown(self):
		for rule in self.rules:
			self.ip_tables.delete_rule(rule)
