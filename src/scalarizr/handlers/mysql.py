'''
Created on 14.06.2010

@author: spike
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler
import logging

def get_handlers ():
	return [MysqlHandler()]

class MysqlHandler(Handler):
	
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		bus.on("init", self.on_init)		
				
	def on_init(self):
		bus.on("before_host_init", self.on_before_host_init)
		
	def on_before_host_init(self, message):
		
		
		pass
	