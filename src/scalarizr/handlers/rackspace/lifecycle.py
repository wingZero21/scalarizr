
from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.config import ScalarizrState
from scalarizr.util import iptables
import logging
import sys

def get_handlers ():
	return [RackspaceLifeCycleHandler()]

class RackspaceLifeCycleHandler(Handler):
	_logger = None
	_platform = None
	_cnf = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		bus.on(init=self.on_init, reload=self.on_reload)
		self.on_reload()
	
	def on_init(self, *args, **kwargs):
		bus.on(before_reboot_finish=self.on_before_reboot_finish)	
		if self._cnf.state in (ScalarizrState.BOOTSTRAPPING, ScalarizrState.IMPORTING):
			self._insert_iptables_rules()
	
	def on_reload(self):
		self._cnf = bus.cnf
		self._platform = bus.platform
			
	def on_before_reboot_finish(self, *args, **kwargs):
		self._insert_iptables_rules()
				
	def _insert_iptables_rules(self):
		self._logger.debug('Adding iptables rules for scalarizr ports')		
		
		try:
			iptables.insert_rule_once('ACCEPT', 8012, iptables.P_TCP)
			iptables.insert_rule_once('ACCEPT', 8013, iptables.P_TCP)
			iptables.insert_rule_once('ACCEPT', 8014, iptables.P_UDP)
		except:
			self._logger.warn('Rule wasn`t added. Detail: %s', sys.exc_info()[1], exc_info=sys.exc_info())