
from scalarizr import config
from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.messaging import Queues, Message, Messages
from scalarizr.util import initdv2, disttool
from scalarizr.util.initdv2 import Status
from scalarizr.service import CnfPresetStore, CnfPreset, CnfController,\
	PresetType

import os
import platform
import logging
import threading
from distutils.file_util import write_file


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
					self.new_message(msg_name, msg_body, msg_meta, broadcast, include_pad, srv)
		srv.new_producer(host).send(queue, msg)

	def _broadcast_message(self, msg):
		cnf = bus.cnf
		platform = bus.platform

		msg.local_ip = platform.get_private_ip()
		msg.remote_ip = platform.get_public_ip()
		msg.behaviour = config.split(cnf.rawini.get(config.SECT_GENERAL, config.OPT_BEHAVIOUR))
		msg.role_name = cnf.rawini.get(config.SECT_GENERAL, config.OPT_ROLE_NAME)

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
		cnf = bus.cnf
		platform = bus.platform

		self._logger.debug("Initialize message listener");
		self._accept_kwargs = dict(
			behaviour = config.split(cnf.rawini.get(config.SECT_GENERAL, config.OPT_BEHAVIOUR)),
			platform = platform.name,
			os = disttool.uname(),
			dist = disttool.linux_dist()
		)
		self._logger.debug("Gathered _accept_kwargs: %s", self._accept_kwargs)
		
		self._get_handlers_chain()
	

	def _get_handlers_chain (self):
		if self._handlers_chain is None:
			self._handlers_chain = []
			self._logger.debug("Collecting handlers chain");
			
			cnf = bus.cnf 
			for handler_name, module_name in cnf.rawini.items(config.SECT_HANDLERS):
				try:
					module_name = cnf.rawini.get(config.SECT_HANDLERS, handler_name)
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
					self._logger.error('Unhandled exception in notification loop')
					self._logger.exception(e)
						
			self._logger.debug("Collected handlers chain: %s" % self._handlers_chain)
						
		return self._handlers_chain
	
	def __call__(self, message, queue):
		self._logger.debug("Handle '%s'" % (message.name))
		
		try:
			# Each message can contains secret data to access platform services.
			# Scalarizr assign access data to platform object and clears it when handlers processing finished 
			pl = bus.platform
			cnf = bus.cnf
			if message.body.has_key("platform_access_data"):
				pl.set_access_data(message.platform_access_data)
			if 'scalr_version' in message.meta:
				try:
					ver = tuple(map(int, message.meta['scalr_version'].strip().split('.')))
				except:
					pass
				else:
					write_file(cnf.private_path('.scalr-version'), '.'.join(map(str, ver)))
					bus.scalr_version = ver					
			
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
	_logger = None 	
	
	_service_name = None
	_cnf_ctl = None 
	_init_script = None
	_preset_store = None
	
	def __init__(self, service_name, init_script, cnf_ctl=None):
		self._logger = logging.getLogger(__name__)		
		
		self._service_name = service_name
		self._cnf_ctl = cnf_ctl
		self._init_script = init_script
		self._preset_store = CnfPresetStore(self._service_name)
		
		self._queryenv = bus.queryenv_service
		bus.on('init', self.sc_on_init)
		bus.define_events(
			self._service_name + '_reload',
			'before_' + self._service_name + '_configure',
			self._service_name + '_configure'
		)


	def on_UpdateServiceConfiguration(self, message):
		if self._service_name != message.behaviour:
			return

		result = self.new_message(Messages.UPDATE_SERVICE_CONFIGURATION_RESULT)
		result.behaviour = message.behaviour
		
		# Obtain current configuration preset
		if message.reset_to_defaults == '1':
			new_preset = self._preset_store.load(PresetType.DEFAULT)
		else:
			new_preset = self._obtain_current_preset()
		result.preset = new_preset.name
		
		# Apply current preset
		try:
			self._logger.info("Applying preset '%s' to %s %s service restart", 
							new_preset.name, self._service_name, 
							'with' if message.restart_service == '1' else 'without')
			self._cnf_ctl.apply_preset(new_preset)
			if message.restart_service == '1':
				self._start_service_with_preset(new_preset)
			result.status = 'ok'
		except (BaseException, Exception), e:
			result.status = 'error'
			result.last_error = str(e)
			
		# Send result
		self.send_message(result)
			
	def _start_service(self):
		self._logger.info("Starting %s" % self._service_name)
		try:
			self._init_script.start()
		except BaseException, e:
			if not self._init_script.running:
				raise
			self._logger.warning(str(e))
		self._logger.debug("%s started" % self._service_name)

	def _stop_service(self):
		self._logger.info("Stopping %s", self._service_name)
		try:
			self._init_script.stop()
		except:
			if self._init_script.running:
				raise
		self._logger.debug("%s stopped", self._service_name)
	
	def _restart_service(self):
		self._logger.info("Restarting %s", self._service_name)
		self._init_script.restart()
		self._logger.debug("%s restarted", self._service_name)

	def _reload_service(self):
		self._logger.info("Reloading %s", self._service_name)
		try:
			self._init_script.reload()
			bus.fire(self._service_name + '_reload')
		except initdv2.InitdError, e:
			if e.code == initdv2.InitdError.NOT_RUNNING:
				self._logger.debug('%s not running', self._service_name)
			else:
				raise
		self._logger.debug("%s reloaded", self._service_name)
		
	def _obtain_current_preset(self):
		service_conf = self._queryenv.get_service_configuration(self._service_name)
		
		cur_preset = CnfPreset(service_conf.name, service_conf.settings)			
		if cur_preset.name == 'default':
			try:
				cur_preset = self._preset_store.load(PresetType.DEFAULT)
			except IOError, e:
				if e.errno == 2:
					cur_preset = self._cnf_ctl.current_preset()
					self._preset_store.save(cur_preset, PresetType.DEFAULT)
				else:
					raise
		return cur_preset

	def _start_service_with_preset(self, preset):
		try:
			self._start_service()
		except BaseException, e:
			self._logger.error('Cannot start %s with current configuration preset. ' % self._service_name
					+ '[Reason: %s] ' % str(e)
					+ 'Rolling back to the last successful preset')
			preset = self._preset_store.load(PresetType.LAST_SUCCESSFUL)
			self._cnf_ctl.apply_preset(preset)
			self._start_service()
			
		self._logger.debug("Set %s configuration preset '%s' as last successful", self._service_name, preset.name)
		self._preset_store.save(preset, PresetType.LAST_SUCCESSFUL)		

	def sc_on_init(self):
		bus.on(
			start=self.sc_on_start,
			service_configured=self.sc_on_configured,
			before_host_down=self.sc_on_before_host_down
		)
		
	def sc_on_start(self):
		szr_cnf = bus.cnf
		if szr_cnf.state == ScalarizrState.RUNNING:
			if self._cnf_ctl:
				# Stop servive if it's already running
				self._stop_service()
				
				# Obtain current configuration preset
				cur_preset = self._obtain_current_preset()

				# Apply current preset
				my_preset = self._cnf_ctl.current_preset()
				if not self._cnf_ctl.preset_equals(cur_preset, my_preset):
					self._cnf_ctl.apply_preset(cur_preset)
				else:
					self._logger.debug("%s configuration satisfies current preset '%s'", self._service_name, cur_preset.name)
					
				# Start service with updated configuration
				self._start_service_with_preset(cur_preset)
			else:
				self._start_service()


	def sc_on_before_host_down(self, msg): 
		self._stop_service()
	
	def sc_on_configured(self, service_name, **kwargs):
		if self._service_name != service_name:
			return
		
		if self._cnf_ctl:
			# Stop service if it's already running
			self._stop_service()		
			
			# Backup default configuration
			my_preset = self._cnf_ctl.current_preset()
			self._preset_store.save(my_preset, PresetType.DEFAULT)
			
			
			# Fetch current configuration preset
			service_conf = self._queryenv.get_service_configuration(self._service_name)
			cur_preset = CnfPreset(service_conf.name, service_conf.settings, self._service_name)
			self._preset_store.copy(PresetType.DEFAULT, PresetType.LAST_SUCCESSFUL, override=False)
			
			if cur_preset.name == 'default':
				# Scalr respond with default preset
				self._logger.debug('%s configuration is default', self._service_name)
				#self._preset_store.copy(PresetType.DEFAULT, PresetType.LAST_SUCCESSFUL)
				self._start_service()
				return
			
			elif self._cnf_ctl.preset_equals(cur_preset, my_preset):
				self._logger.debug("%s configuration satisfies current preset '%s'", self._service_name, cur_preset.name)
				return
			
			else:
				self._cnf_ctl.apply_preset(cur_preset)
				
			# Start service with updated configuration
			self._start_service_with_preset(cur_preset)
		else:
			self._start_service()
			
		bus.fire(self._service_name + '_configure', **kwargs)		


class RebundleLogHandler(logging.Handler):
	def __init__(self, bundle_task_id=None):
		logging.Handler.__init__(self)
		self.bundle_task_id = bundle_task_id
		self._msg_service = bus.messaging_service
		
	def emit(self, record):
		msg = self._msg_service.new_message(Messages.REBUNDLE_LOG, body=dict(
			bundle_task_id = self.bundle_task_id,
			message = str(record.msg) % record.args if record.args else str(record.msg)
		))
		self._msg_service.get_producer().send(Queues.LOG, msg)
