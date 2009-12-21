'''
Created on Dec 11, 2009

@author: marat
'''
import logging

from scalarizr.core.handlers import Handler

def get_handlers ():
	return [PosixIpListBuilder()]

class PosixIpListBuilder(Handler):
	_logger = None
	
	def __init__(self):
		self._logger = logging.getLogger(__package__ + "." + self.__class__.__name__)
	
	def on_HostUp(self, message):
		self._logger.info("host up")
		pass
	
	def on_HostDown(self, message):
		self._logger.info("host down")
		pass
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == "HostUp" or message.name == "HostDown"



