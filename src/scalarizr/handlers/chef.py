'''
Created on Oct 24, 2011

@author: marat
'''

import logging

from scalarizr.handlers import Handler
from scalarizr.bus import bus

LOG = logging.getLogger(__name__)


def get_handlers():
	return (ChefHandler(), )


class ChefHandler(Handler):
	def __init__(self):
		bus.on(init=self.on_init)
		
	def on_init(self, *args, **kwds):
		bus.on(host_init_response=self.on_host_init_response)
		
	def on_host_init_response(self, msg):
		pass