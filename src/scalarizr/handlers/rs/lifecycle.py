from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.config import ScalarizrState
from scalarizr.util.iptables import RuleSpec, IpTables
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
			for port in [8013, 8014]:
				rule = RuleSpec(dport=port, jump='ACCEPT')
				iptables.insert_rule(rule)
				
				