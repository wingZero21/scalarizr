'''
Created on Jul 23, 2010

@author: marat
@author: shaitanich
'''
from scalarizr.bus import bus
from scalarizr.handlers import Handler, HandlerError, lifecircle
from scalarizr.util import disttool, initd
from scalarizr.util.filetool import read_file, write_file
from scalarizr.util import iptables
from scalarizr.util.iptables import IpTables, RuleSpec
from scalarizr.messaging import Messages
import logging
import re
import os

pid_file = None
if disttool.is_redhat_based():
	pid_file = "/var/run/memcached/memcached.pid"
elif disttool.is_debian_based():
	pid_file = "/var/run/memcached.pid" 

initd_script = '/etc/init.d/memcached'
if not os.path.exists(initd_script):
	raise HandlerError("Cannot find Memcached init script at %s. Make sure that apache web server is installed" % initd_script)

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
		
		config = bus.config
		cache_size = config.get('behaviour_memcached','cache_size')
		
		if disttool._is_debian_based:
			self.mcd_conf_path = '/etc/memcached.conf' 
			self.expression = re.compile('^\s*-m\s*\d*$', re.M) 
			self.substitute = '-m %s' % cache_size
		else:
			self.mcd_conf_path = '/etc/sysconfig/memcached'
			self.expression = re.compile('^\s*CACHESIZE\s*=\s*"\d*"$', re.M)
			self.substitute = 'CACHESIZE="%s"' % cache_size		

		self.ip_tables = IpTables()
		self.rules = []
		
		bus.on("init", self.on_init)
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.HOST_UP \
			or message.name == Messages.HOST_DOWN	
	
	
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
	
	def on_before_host_up(self, message):
					
		mcd_conf = read_file(self.mcd_conf_path, logger=self._logger)
	
		if mcd_conf:
			if self.expression.findall(mcd_conf):
				write_file(self.mcd_conf_path, re.sub(self.expression, self.substitute, mcd_conf), logger=self._logger)
			else:
				write_file(self.mcd_conf_path, self.substitute, mode='a', logger = self._logger)
				
		try:
			self._logger.info("Reloading memcached")
			initd.reload("memcached", force=True)
		except initd.InitdError, e:
			self._logger.error(e)


	def on_before_host_down(self):
		try:
			self._logger.info("Stopping memcached")
			initd.stop("memcached")
		except initd.InitdError, e:
			self._logger.error("Cannot stop memcached")
			if initd.is_running("memcached"):
				raise
	
	
	def on_HostUp(self, message):
		# Adding iptables rules
		ips = []
		roles = self._queryenv.list_roles()
		self._logger.debug("MEMCACHED ASKED FOR % ROLES:",len(roles))
		
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
		
		
	def on_HostDown(self, message):
		for rule in self.rules:
			self.ip_tables.delete_rule(rule)
		 
