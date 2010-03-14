'''
Created on Dec 25, 2009

@author: marat
'''

from scalarizr.core import Bus, BusEntries, Behaviours
from scalarizr.core.handlers import Handler
from scalarizr.messaging import Messages
import logging


def get_handlers ():
	return [ApacheHandler()]

class ApacheHandler(Handler):
	_logger = None
	_queryenv = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = Bus()[BusEntries.QUERYENV_SERVICE]
	
	def on_VhostReconfigure(self, message):
		self._logger.debug("Entering on_VhostReconfigure")
		
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self._reload_apache()
	
	def _update_vhost(self):
		vhosts = self._queryenv.list_virtual_hosts(vhost_name)
		if len(vhosts):
			self._logger.info("Enabling virtual host %s", vhost_name)
			
			pass
		else:
			self._logger.info("Disabling virtual host %s", vhost_name)
			pass
		
	def _reload_apache(self):
		pass
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return behaviour == Behaviours.APP and message.name == Messages.VHOST_RECONFIGURE
	
