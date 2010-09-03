'''
Created on Mar 3, 2010

@author: marat
'''

import scalarizr.handlers
from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.messaging import Messages, MetaOptions
from scalarizr.util import cryptotool, configtool, log

import logging, os, sys, binascii
from subprocess import Popen, PIPE



_lifecycle = None
def get_handlers():
	if not _lifecycle:
		globals()["_lifecycle"] = LifeCycleHandler()
	return [_lifecycle]

class LifeCycleHandler(scalarizr.handlers.Handler):
	_logger = None
	_bus = None
	_msg_service = None
	_producer = None
	_platform = None
	_config = None
	_cnf = None
	
	_new_crypto_key = None
	
	FLAG_REBOOT = "reboot"
	FLAG_HALT = "halt"
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		
		bus.define_events(
			# Fires before HostInit message is sent
			# @param msg 
			"before_host_init",
			
			# Fires after HostInit message is sent
			"host_init",
			
			# Fires when HostInitResponse received
			# @param msg
			"host_init_response",
			
			# Fires before HostUp message is sent
			# @param msg
			"before_host_up",
			
			# Fires after HostUp message is sent
			"host_up",
			
			# Fires before RebootStart message is sent
			# @param msg
			"before_reboot_start",
			
			# Fires after RebootStart message is sent
			"reboot_start",
			
			# Fires before RebootFinish message is sent
			# @param msg
			"before_reboot_finish",
			
			# Fires after RebootFinish message is sent
			"reboot_finish",
			
			# Fires before Restart message is sent
			# @param msg: Restart message
			"before_restart",
			
			# Fires after Restart message is sent
			"restart",
			
			# Fires before Hello message is sent
			# @param msg
			"before_hello",
			
			# Fires after Hello message is sent
			"hello",
			
			# Fires after HostDown message is sent
			# @param msg
			"before_host_down",
			
			# Fires after HostDown message is sent
			"host_down"
		)
		bus.on("init", self.on_init)
		
		self._msg_service = bus.messaging_service
		self._producer = self._msg_service.get_producer()
		self._config = bus.config
		self._cnf = bus.cnf
		self._platform = bus.platform
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.INT_SERVER_REBOOT \
			or message.name == Messages.INT_SERVER_HALT	\
			or message.name == Messages.HOST_INIT_RESPONSE \
			or message.name == Messages.SCALARIZR_UPDATE_AVAILABLE

	
	def on_init(self):
		bus.on("start", self.on_start)
		self._producer.on("before_send", self.on_before_message_send)
		
		# Add internal messages to scripting skip list
		try:
			map(scalarizr.handlers.script_executor.skip_events.add, (
				Messages.INT_SERVER_REBOOT, 
				Messages.INT_SERVER_HALT, 
				Messages.HOST_INIT_RESPONSE
			))
		except AttributeError:
			pass



	def on_start(self):
		optparser = bus.optparser

		'''
		file = self._get_flag_filename('update')
		if not os.path.exists(file) or time.time() - os.stat(file).st_mtime > 86400:
			self._update_package()
		'''

		if self._flag_exists(self.FLAG_REBOOT):
			self._logger.info("Scalarizr resumed after reboot")
			self._clear_flag(self.FLAG_REBOOT)			
			self._start_after_reboot()
		elif self._flag_exists(self.FLAG_HALT):
			self._logger.info("Scalarizr resumed after stop")
			self._clear_flag(self.FLAG_HALT)
			self._start_after_stop()
			
		elif optparser.values.import_server:
			self._logger.info("Server will be imported into Scalr")
			self._start_import()
			
		elif self._cnf.state in (ScalarizrState.BOOTSTRAPPING, ScalarizrState.IMPORTING):
			self._logger.info("Starting initialization")
			self._start_init()
		else:
			self._logger.info("Normal start")


	def _start_after_reboot(self):
		msg = self._new_message(Messages.REBOOT_FINISH, broadcast=True)
		bus.fire("before_reboot_finish", msg)
		self._send_message(msg)
		bus.fire("reboot_finish")		

	def _start_after_stop(self):
		msg = self._new_message(Messages.RESTART)
		bus.fire("before_restart". msg)
		self._send_message(msg)
		bus.fire("restart")
	
	def _start_init(self):
		# Regenerage key
		self._new_crypto_key = cryptotool.keygen()
		
		# Prepare HostInit
		msg = self._new_message(Messages.HOST_INIT, dict(
			crypto_key=self._new_crypto_key,
			snmp_port=self._config.get(configtool.SECT_SNMP, configtool.OPT_PORT),
			snmp_community_name=self._config.get(configtool.SECT_SNMP, configtool.OPT_COMMUNITY_NAME)
		), broadcast=True)
		bus.fire("before_host_init", msg)
		self._send_message(msg)
		
		# Update key file
		key_path = self._config.get(configtool.SECT_GENERAL, configtool.OPT_CRYPTO_KEY_PATH)		
		configtool.write_key(key_path, self._new_crypto_key, key_title="Scalarizr crypto key")

		# Update key in QueryEnv
		queryenv = bus.queryenv_service
		queryenv.key = binascii.a2b_base64(self._new_crypto_key)
		
		del self._new_crypto_key
		
		bus.cnf.state = ScalarizrState.INITIALIZING
		bus.fire("host_init")		
		

	
	def _start_import(self):
		# Send Hello
		msg = self._new_message(Messages.HELLO, {"architecture" : self._platform.get_architecture()})		
		bus.fire("before_hello", msg)
		self._send_message(msg)
		bus.fire("hello")


	def on_IntServerReboot(self, message):
		# Scalarizr must detect that it was resumed after reboot
		self._set_flag(self.FLAG_REBOOT)
		# Send message 
		msg = self._new_message(Messages.REBOOT_START, broadcast=True)
		bus.fire("before_reboot_start", msg)
		self._send_message(msg)
		bus.fire("reboot_start")
		
	
	def on_IntServerHalt(self, message):
		self._set_flag(self.FLAG_HALT)
		msg = self._new_message(Messages.HOST_DOWN, broadcast=True)
		bus.fire("before_host_down", msg)
		self._send_message(msg)		
		bus.fire("host_down")

	def on_HostInitResponse(self, message):
		bus.fire("host_init_response", message)
		msg = self._new_message(Messages.HOST_UP, broadcast=True)
		bus.fire("before_host_up", msg)
		self._send_message(msg)
		bus.cnf.state = ScalarizrState.RUNNING
		bus.fire("host_up")


	def on_ScalarizrUpdateAvailable(self, message):
		self._update_package()


	def _update_package(self):
		up_script = self._config.get(configtool.SECT_GENERAL, configtool.OPT_SCRIPTS_PATH) + "/update"
		cmd = [sys.executable, up_script]
		p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=False, close_fds=True)
		p.communicate()
		self._set_flag('update')

	def on_before_message_send(self, queue, message):
		"""
		Add scalarizr version to meta
		"""
		message.meta[MetaOptions.SZR_VERSION] = scalarizr.__version__
		
		
	def _get_flag_filename(self, name):
		return self._cnf.private_path('.%s' % name)
	
	def _set_flag(self, name):
		file = self._get_flag_filename(name)
		try:
			self._logger.debug("Touch file '%s'", file)
			open(file, "w+").close()
		except IOError, e:
			self._logger.error("Cannot touch file '%s'. IOError: %s", file, str(e))		
	
	def _flag_exists(self, name):
		return os.path.exists(self._get_flag_filename(name))
	
	def _clear_flag(self, name):
		os.remove(self._get_flag_filename(name))	
	
