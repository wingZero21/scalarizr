
from scalarizr.core import Bus, BusEntries
import os
import platform
import logging


class Handler(object):
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return False
	
	def __call__(self, message):
		fn = "on_" + message.name
		if hasattr(self, fn) and callable(getattr(self, fn)):
			getattr(self, fn)(message)
		else:
			raise HandlerError("Handler has no method %s" % (fn))

class HandlerError(BaseException):
	pass

class MessageListener ():
	_logger = None 
	_handlers_chain = None
	_accept_kwargs = {}
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		config = Bus()[BusEntries.CONFIG]
		self._logger.debug("Initialize message listener");
		
		self._accept_kwargs["behaviour"] = config.get("default", "behaviour").split(",")
		self._accept_kwargs["platform"] = config.get("default", "platform")
		self._accept_kwargs["os"] = platform.uname()
		self._accept_kwargs["dist"] = platform.dist()
		self._logger.debug("Gathered _accept_kwargs: %s", self._accept_kwargs)
		
		self._get_handlers_chain()
	

	def _get_handlers_chain (self):
		if self._handlers_chain is None:
			self._handlers_chain = []
			self._logger.debug("Collecting handlers chain");
			
			config = Bus()[BusEntries.CONFIG]
			handlers = config.options("handlers")
			print handlers
			for handler_name in handlers:
				try:
					module_name = config.get("handlers", handler_name)
					skip = False
					
					# Read handler configuration.
					# If will be available in global configuration
					filename = "%s/etc/include/handler.%s.ini" % (Bus()[BusEntries.BASE_PATH], handler_name)
					if os.path.exists(filename):
						try:
							self._logger.debug("Read handler configuration file %s", filename)
							config.read(filename)
							
							"""
							from ConfigParser import ConfigParser
							handler_config = ConfigParser()
							handler_config.read(filename)
							
							self._logger.debug("Inject handler configuration into global config")
							from scalarizr.util import inject_config
							inject_config(config, handler_config)
							"""
							
						except Exception, e:
							skip = True
							self._logger.error("Cannot read handler configuraion (handler: %s, filename: %s)", 
										handler_name, filename)
							self._logger.exception(e)
							
					
					# Import module
					if not skip:
						try:
							module = __import__(module_name, globals(), locals(), ["get_handlers"], -1)
							try:
								self._handlers_chain.extend(module.get_handlers())
							except Exception, e:
								self._logger.error("Cannot get module handlers (module: %s)", module_name)
								self._logger.exception(e)
							
						except Exception, e:
							self._logger.error("Cannot import module %s", module_name)
							self._logger.exception(e)
							
				except Exception, e:
					self._logger.error("Unhandled exception in handler notification loop")
					self._logger.exception(e)
						
			self._logger.debug("Collected handlers chain: %s" % self._handlers_chain)
						
		return self._handlers_chain
	
	def __call__(self, message, queue):
		self._logger.info("Handle '%s'" % (message.name))
		accepted = False
		for handler in self._get_handlers_chain():
			try:
				if handler.accept(message, queue):
					accepted = True
					self._logger.info("Call handler %s" % handler.__class__.__name__)
					try:
						handler(message)
					except Exception, e:
						self._logger.error("Exception in message handler")
						self._logger.exception(e)
			except Exception, e:
				self._logger.error("%s accept() method failed with exception", handler.__class__.__name__)
				self._logger.exception(e)
		
		if not accepted:
			self._logger.warning("No one could handle '%s'", message.name)
			
			
