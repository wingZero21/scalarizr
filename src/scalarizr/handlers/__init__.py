
from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.messaging import Queues, Message, Messages
from scalarizr.util import configtool, initdv2
from scalarizr.util.initdv2 import Status
from scalarizr.service import CnfPresetStore, CnfPreset

import os
import platform
import logging
import threading


class Handler(object):
	
	def new_message(self, msg_name, msg_body=None, msg_meta=None, broadcast=False, include_pad=False, srv=None):
		srv = srv or bus.messaging_service
		pl = bus.platform		
				
		msg = srv.new_message(msg_name, msg_meta, msg_body)
		if broadcast:
			self._broadcast_message(msg)
		if include_pad:
			msg.body['platform_access_data'] = pl.get_access_data()
		return msg
	
	def send_message(self, msg_name, msg_body=None, msg_meta=None, broadcast=False, 
					queue=Queues.CONTROL):
		srv = bus.messaging_service
		msg = msg_name if isinstance(msg_name, Message) else \
				self.new_message(msg_name, msg_body, msg_meta, broadcast)
		srv.get_producer().send(queue, msg)
		
	def send_int_message(self, host, msg_name, msg_body=None, msg_meta=None, broadcast=False, 
						include_pad=False, queue=Queues.CONTROL):
		srv = bus.int_messaging_service
		msg = msg_name if isinstance(msg_name, Message) else \
					self.new_message(msg_name, msg_body, msg_meta, broadcast, include_pad, srv.msg_service)
		srv.new_producer(host).send(queue, msg)

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


class ServiceCtlHanler(Handler):
	_service_name = None
	_cnf_ctl = None 
	_init_script = None
	_logger = None 
	
	def __init__(self, service_name, init_script, cnf_ctl=None):
		self._service_name = service_name
		self._cnf_ctl = cnf_ctl
		self._init_script = init_script
		
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		
		bus.on('init', self.sc_on_init)

	def sc_on_start(self):
		szr_cnf = bus.cnf
		if szr_cnf.state == ScalarizrState.RUNNING and not self._init_script.running:
			self._start_service()	
		self._reconfigure()	

	def _reconfigure(self):
		msg = self._new_message(Messages.UPDATE_SERVICE_CONFIGURATION_RESULT)
		msg.status = 'ok'
		
		storage = CnfPresetStore()	
		try:		
			try:
				if not self._init_script.running:
					self._start_service()
					
				last = self._cnf_ctl.current_preset()
			
			except (BaseException, Exception), e:
				last = storage.load(self._service_name, CnfPresetStore.PresetType.DEFAULT)
				self._start_service()
			finally:
				storage.save(self._service_name, last, CnfPresetStore.PresetType.LAST_SUCCESSFUL)
			
			configuration = self._queryenv.get_service_configuration()
			new_preset = CnfPreset(configuration.name, configuration.settings)
			
			CnfPresetStore.save(self._service_name, new_preset, CnfPresetStore.PresetType.CURRENT)
			self._cnf_ctl.apply_preset(self, new_preset)
			
			if new_preset.restart_service: 
				try:
					self._restart_service()
				except initdv2.InitdError, e:	
					self._cnf_ctl.apply_preset(last)
					self._start_service()
					storage.save(self._service_name, last, CnfPresetStore.PresetType.CURRENT)	
					
		except (BaseException, Exception), e:
			msg.status = 'error'
			msg.last_error = str(e)	
			self._logger.error(e)	
		
		self._send_message(msg)
		
	
	def on_UpdateServiceConfiguration(self, message):
		if self._service_name != message.behaviour:
			return

		storage = CnfPresetStore()

		msg = self._new_message(Messages.UPDATE_SERVICE_CONFIGURATION_RESULT)
		msg.behaviour = message.behaviour
		
		if message.reset_to_defaults:
			msg.preset = 'default'
			msg.status = 'ok'
			try:			
				default_preset = storage.load(message.behaviour, CnfPresetStore.PresetType.DEFAULT)
				self._cnf_ctl.apply_preset(self, default_preset)
				if message.restart_service:
					self._restart_service()
			except (BaseException, Exception), e:
				msg.status = 'error'
				msg.last_error = str(e)
			
			self._send_message(msg)
				
		else:
			
			self._reconfigure()

			"""
			try:
				try:
					if not self._init_script.running:
						self._init_script.start()
					last = self._cnf_ctl.current_preset()
				except (BaseException, Exception), e:
					last = storage.load(message.behaviour, PresetType.DEFAULT)
				
				storage.save(message.behaviour, last, PresetType.LAST_SUCCESSFUL)
				
				configuration = self._queryenv.get_service_configuration()
				new_preset = CnfPreset(configuration.name, configuration.settings)
				
				CnfPresetStore.save(message.behaviour, new_preset, PresetType.CURRENT)
				self._cnf_ctl.apply_preset(self, new_preset)
				
				if new_preset.restart_service: 
					try:
						self._restart_service(message.behaviour)
					except initdv2.InitdError, e:	
						self._cnf_ctl.apply_preset(last)
						self._start_service(message.behaviour)
						storage.save(message.behaviour, last, PresetType.CURRENT)
						
			except (BaseException, Exception), e:
				msg.status = 'error'
				msg.last_error = str(e)
					
			self._send_message(msg)
			"""
			
			
	def _start_service(self):
		try:
			self._logger.info("Starting %s" % self._service_name)
			self._init_script.restart()
			self._logger.debug("%s started" % self._service_name)
		except initdv2.InitdError, e:
			self._logger.error("Cannot start %s: e." % (self._service_name, e))
			raise				
	
	def _restart_service(self):
		try:
			self._logger.info("Restarting %s" % self._service_name)
			self._init_script.restart()
			self._logger.debug("%s restarted" % self._service_name)
		except initdv2.InitdError, e:
			self._logger.error("Cannot restart %s\n%s\n. Trying to roll back." % (self._service_name,e))
			raise

	def sc_on_init(self):
		bus.on(
			start=self.sc_on_start,
			service_configured=self.sc_on_configured,
			before_host_down=self.sc_on_before_host_down
		)

	def sc_on_before_host_down(self):
		try:
			self._logger.info("Stopping %s", self._service_name)
			self._init_script.stop()
		except:
			self._logger.error("Cannot stop %s", self._service_name)
			if self._init_script.running:
				raise		
	
	def sc_on_configured(self, service_name):
		if not self._init_script.running:
			self._start_service()
			
		last = self._cnf_ctl.current_preset()
		storage = CnfPresetStore()
		storage.save(service_name, last, CnfPresetStore.PresetType.DEFAULT)
		
		self._reconfigure()	
