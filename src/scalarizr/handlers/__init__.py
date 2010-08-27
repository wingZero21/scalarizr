
from scalarizr.bus import bus
from scalarizr.messaging import Queues, Message
from scalarizr.util import configtool
import os
import platform
import logging
import threading


class Handler(object):
	
	def _new_message(self, msg_name, msg_body=None, msg_meta=None, broadcast=False):
		srv = bus.messaging_service		
		msg = srv.new_message(msg_name, msg_meta, msg_body)
		if broadcast:
			self._broadcast_message(msg)
		return msg
	
	def _send_message(self, msg_name, msg_body=None, msg_meta=None, broadcast=False, queue=Queues.CONTROL):
		srv = bus.messaging_service
		msg = msg_name if isinstance(msg_name, Message) else \
				self._new_message(msg_name, msg_body, msg_meta, broadcast)
		srv.get_producer().send(queue, msg)

	def _broadcast_message(self, msg):
		config = bus.config
		platform = bus.platform
		gen_sect = configtool.section_wrapper(config, configtool.SECT_GENERAL)
		
		msg.behaviour = configtool.split_array(gen_sect.get(configtool.OPT_BEHAVIOUR))
		msg.local_ip = platform.get_private_ip()
		msg.remote_ip = platform.get_public_ip()
		msg.role_name = gen_sect.get(configtool.OPT_ROLE_NAME)	

	
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

class MessageListener:
	_logger = None 
	_handlers_chain = None
	_accept_kwargs = {}
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		config = bus.config

		self._logger.debug("Initialize message listener");
		self._accept_kwargs = dict(
			behaviour = configtool.split_array(config.get(configtool.SECT_GENERAL, configtool.OPT_BEHAVIOUR)),
			platform = getattr(bus.platform, "name"),
			os = platform.uname(),
			dist = platform.dist()
		)
		self._logger.debug("Gathered _accept_kwargs: %s", self._accept_kwargs)
		
		self._get_handlers_chain()
	

	def _get_handlers_chain (self):
		if self._handlers_chain is None:
			self._handlers_chain = []
			self._logger.debug("Collecting handlers chain");
			
			config = bus.config
			for handler_name, module_name in config.items(configtool.SECT_HANDLERS):
				try:
					module_name = config.get("handlers", handler_name)
					skip = False
					
					# Read handler configuration.
					# If will be available in global configuration
					'''
					for filename in configtool.get_handler_filename(handler_name, ret=configtool.RET_BOTH):
						if os.path.exists(filename):
							try:
								self._logger.debug("Read handler configuration file %s", filename)
								config.read(filename)
								
							except Exception, e:
								skip = True
								self._logger.error("Cannot read handler configuraion (handler: %s, filename: %s)", 
											handler_name, filename)
								self._logger.exception(e)
					'''		
					
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
							
				except (BaseException, Exception), e:
					self._logger.error("Unhandled exception in handler notification loop")
					self._logger.exception(e)
						
			self._logger.debug("Collected handlers chain: %s" % self._handlers_chain)
						
		return self._handlers_chain
	
	def __call__(self, message, queue):
		self._logger.debug("Handle '%s'" % (message.name))
		
		try:
			# Each message can contains secret data to access platform services.
			# Scalarizr assign access data to platform object and clears it when handlers processing finished 
			pl = bus.platform
			if message.body.has_key("platform_access_data"):
				pl.set_access_data(message.platform_access_data)
			
			accepted = False
			for handler in self._get_handlers_chain():
				hnd_name = handler.__class__.__name__
				try:
					if handler.accept(message, queue, **self._accept_kwargs):
						accepted = True
						self._logger.debug("Call handler %s" % hnd_name)
						try:
							handler(message)
						except (BaseException, Exception), e:
							self._logger.error("Exception in message handler %s", hnd_name)
							self._logger.exception(e)
				except (BaseException, Exception), e:
					self._logger.error("%s accept() method failed with exception", hnd_name)
					self._logger.exception(e)
			
			if not accepted:
				self._logger.warning("No one could handle '%s'", message.name)
		finally:
			pl.clear_access_data()

def async(fn):
	def decorated(*args, **kwargs):
		t = threading.Thread(target=fn, args=args, kwargs=kwargs)
		t.start()
	
	return decorated
	