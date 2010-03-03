'''
Created on Mar 3, 2010

@author: marat
'''

from scalarizr.core import Bus
from scalarizr.core.handlers import Handler
import logging

def get_handlers ():
	return [HooksHandler()]

class HooksHandler(Handler):
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		Bus().on("init", self.on_init)
		
	def on_init(self):
		bus = Bus()
		for event in bus.list_events():
			bus.on(event, self.create_hook(event))
			
	def create_hook(self, event):
		def hook(*args, **kwargs):
			self._logger.info("Hook on '"+event+"'" + str(args) + " " + str(kwargs))
		return hook
