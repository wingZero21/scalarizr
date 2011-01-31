
from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.config import ScalarizrState
from scalarizr.util.iptables import RuleSpec, IpTables, P_TCP, P_UDP
import logging


def get_handlers ():
	return [RackspaceLifeCycleHandler()]

class RackspaceLifeCycleHandler(Handler):
	_logger = None
	_platform = None
	_cnf = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._cnf = bus.cnf
		self._platform = bus.platform
		bus.on("init", self.on_init)
	
	def on_init(self, *args, **kwargs):
		bus.on(before_reboot_finish=self.on_before_reboot_finish)	
		if ScalarizrState.BOOTSTRAPPING == self._cnf.state:
			self._insert_iptables_rules()
			
	def on_before_reboot_finish(self, *args, **kwargs):
		self._insert_iptables_rules()
				
	def _insert_iptables_rules(self):
		self._logger.debug('Adding iptables rules for scalarizr ports')		
		iptables = IpTables()		
		rules = []
		
		# Scalarizr ports
		rules.append(RuleSpec(dport=8012, jump='ACCEPT', protocol=P_TCP))
		rules.append(RuleSpec(dport=8013, jump='ACCEPT', protocol=P_TCP))
		rules.append(RuleSpec(dport=8014, jump='ACCEPT', protocol=P_UDP))
		
		for rule in rules:
			iptables.insert_rule(None, rule_spec = rule)
		
