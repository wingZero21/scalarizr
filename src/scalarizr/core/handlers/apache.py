'''
Created on Dec 25, 2009

@author: marat
'''

from scalarizr.core import Bus, BusEntries, Behaviours
from scalarizr.core.handlers import Handler
from scalarizr.messaging import Messages
import logging


def get_handlers ():
	return [ApacheAdapter()]

class ApacheHandler(Handler):
	_logger = None
	_queryenv = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = Bus()[BusEntries.QUERYENV_SERVICE]
	
	def on_VhostReconfigure(self, message):
		self._logger.debug("Entering on_VhostReconfigure")
		
		vhost_name = message.body["VhostName"]
		is_ssl = bool(int(message.body["IsSSLVhost"]))
		self._logger.debug("Received VhostReconfigure message (vhost_name: %s, is_ssl)", vhost_name, is_ssl)
		
		if vhost_name:
			self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
			self._create_vhosts()
		else:
			self._logger.info("Received update vhost notification (vhost_name: %s, is_ssl: %s)", 
					vhost_name, is_ssl)
			self._manage_vhost(vhost_name, is_ssl)
			
		self._reload_apache()
	
	def _manage_vhost(self, vhost_name, is_ssl):
		if not is_ssl:
			vhosts = self._queryenv.list_virtual_hosts(vhost_name)
			if len(vhosts):
				self._logger.info("Enabling virtual host %s", vhost_name)
				
				pass
			else:
				self._logger.info("Disabling virtual host %s", vhost_name)
				pass
		
	def _create_vhosts(self):
		pass
	
	def _reload_apache(self):
		pass
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return behaviour == Behaviours.APP and message.name == Messages.VHOST_RECONFIGURE
	
# It needed ??
class ApacheAdapter(object):
	def enable_vhost(self, vhost_name):
		pass
	
	def disable_vhost(self, vhost_name):
		pass
	
class ApacheDebianAdapter(ApacheAdapter):
	pass