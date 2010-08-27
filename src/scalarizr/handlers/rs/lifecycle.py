from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.config import ScalarizrState
from scalarizr.util.iptables import RuleSpec, IpTables, P_TCP, P_UDP
import logging


def get_handlers ():
	return [RSLifeCycleHandler()]

class RSLifeCycleHandler(Handler):
	_logger = None
	_platform = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		bus.on("init", self.on_init)
	
	def on_init(self, *args, **kwargs):	
		
		if ScalarizrState.BOOTSTRAPPING == bus.cnf.state:
			iptables = IpTables()
			self._logger.debug('Adding iptables rules for scalarizr ports')
			
			rules = []
			rules.append(RuleSpec(dport=8013, jump='ACCEPT', protocol=P_TCP))
			rules.append(RuleSpec(dport=8014, jump='ACCEPT', protocol=P_UDP))
			for rule in rules:
				iptables.insert_rule(None, rule_spec = rule)
