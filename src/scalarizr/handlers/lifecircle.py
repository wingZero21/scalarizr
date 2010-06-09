'''
Created on Mar 3, 2010

@author: marat
'''

import scalarizr
from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.messaging import Queues, Messages, MetaOptions
from scalarizr.util import cryptotool, configtool
import logging
import os
import binascii



def get_handlers():
	return [LifeCircleHandler()]

class LifeCircleHandler(Handler):
	_logger = None
	_bus = None
	_msg_service = None
	_producer = None
	_platform = None
	_config = None
	
	_new_crypto_key = None
	
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
			
			# Fires after RebootStart message is sent
			"reboot_start",
			
			# Fires before RebootFinish message is sent
			# @param msg
			"before_reboot_finish",
			
			# Fires after RebootFinish message is sent
			"reboot_finish",
			
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
		self._platform = bus.platfrom
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.INT_SERVER_REBOOT \
			or message.name == Messages.INT_SERVER_HALT	\
			or message.name == Messages.HOST_INIT_RESPONSE	

	
	def on_init(self):
		bus.on("start", self.on_start)
		self._producer.on("before_send", self.on_before_message_send)		


	def on_start(self):
		reboot_flag = os.path.join(bus.etc_path, ".reboot")
		hostinit_flag = os.path.join(bus.etc_path, ".hostinit")
		optparser = bus.optparser
		
		if os.path.exists(reboot_flag):
			self._logger.info("Scalarizr resumed after reboot")
			os.remove(reboot_flag)			
			self._start_after_beboot()
			
		elif optparser.values.run_import:
			self._logger.info("Server will be imported into Scalr")
			self._start_import()
			
		elif not os.path.exists(hostinit_flag):
			self._logger.info("Starting initialization")
			self._start_init()
		else:
			self._logger.info("Normal start")


	def _start_after_beboot(self):
		# Send RebootFinish
		msg = self._msg_service.new_message(Messages.REBOOT_FINISH)
		self._put_broadcast_data(msg)
		
		bus.fire("before_reboot_finish", msg)
		self._producer.send(Queues.CONTROL, msg)
		
		bus.fire("reboot_finish")		

	
	def _start_init(self):
		"""
		# Add init scripts
		
		
		# Add reboot script
		dst = "/etc/rc6.d/K10scalarizr"
		if not os.path.exists(dst):
			path = bus.base_path + "/src/scalarizr/scripts/reboot.py"
			try:
				os.symlink(path, dst)
			except OSError:
				self._logger.error("Cannot create symlink %s -> %s", dst, path)
				raise
		"""
		"""
		OpenSolaris:
		2010-03-15 19:10:15,448 - ERROR - scalarizr.handlers.lifecircle - Cannot create symlink /etc/rc6.d/K10scalarizr -> /opt/scalarizr/src/scalarizr/scripts/reboot.py
		2010-03-15 19:10:15,449 - ERROR - scalarizr.util - [Errno 2] No such file or directory
		
		SOLUTION:
		/sbin/rc6 - shell script file, executed on reboot
		add scripts/reboot.py into the begining of this file
		"""
		
		"""
		# Add halt script
		dst = "/etc/rc0.d/K10scalarizr"
		if not os.path.exists(dst):
			path = bus.base_path + "/src/scalarizr/scripts/halt.py"
			try:
				os.symlink(path, dst)
			except OSError:
				self._logger.error("Cannot create symlink %s -> %s", dst, path)
				raise
		
		if os.path.exists("/var/lock/subsys"):
			# Touch /var/lock/subsys/scalarizr
			# This file represents that a service's subsystem is locked, which means the service should be running
			# @see http://www.redhat.com/magazine/008jun05/departments/tips_tricks/
			f = "/var/lock/subsys/scalarizr"
			try:
				open(f, "w+").close()
			except OSError:
				self._logger.error("Cannot touch file '%s'", f)
				raise 
		"""
		
		# Regenerage key
		self._new_crypto_key = cryptotool.keygen()
		
		# Prepare HostInit
		msg = self._msg_service.new_message(Messages.HOST_INIT)
		self._put_broadcast_data(msg)
		msg.crypto_key = self._new_crypto_key
		
		bus.fire("before_host_init", msg)
		
		# Update crypto key when HostInit will be delivered 
		# TODO: Remove this confusing listener when blocking will be implemented in message producer 
		self._producer.on("send", self.on_hostinit_send)
		# Send HostInit
		self._producer.send(Queues.CONTROL, msg) 

	
	def _start_import(self):
		# Send Hello		
		msg = self._msg_service.new_message(Messages.HELLO)
		msg.architecture = self._platform.get_architecture()
		
		bus.fire("before_hello", msg)
		self._producer.send(Queues.CONTROL, msg)
		
		bus.fire("hello")


	def on_hostinit_send(self, *args, **kwargs):
		# Remove listener
		self._producer.un("send", self.on_hostinit_send)
				
		# Update key file
		key_path = self._config.get(configtool.SECT_GENERAL, configtool.OPT_CRYPTO_KEY_PATH)		
		configtool.write_key(key_path, self._new_crypto_key, key_title="Scalarizr crypto key")

		# Update key in QueryEnv
		queryenv = bus.queryenv_service
		queryenv.key = binascii.a2b_base64(self._new_crypto_key)
		
		del self._new_crypto_key
		
		
		hostinit_file = os.path.join(bus.etc_path, ".hostinit")
		try:
			self._logger.debug("Touch file '%s'", hostinit_file)
			open(hostinit_file, "w+").close()
		except IOError, e:
			self._logger.error("Cannot touch file '%s'. IOError: %s", hostinit_file, str(e))
			
		bus.fire("host_init")		


	def on_IntServerReboot(self, message):
		# Scalarizr must detect that it was resumed after reboot
		reboot_file = os.path.join(bus.etc_path, ".reboot")
		try:
			self._logger.debug("Touch file '%s'", reboot_file)
			open(reboot_file, "w+").close()
		except IOError, e:
			self._logger.error("Cannot touch file '%s'. IOError: %s", reboot_file, str(e))
			
		# Send message 
		msg = self._msg_service.new_message(Messages.REBOOT_START)
		self._put_broadcast_data(msg)
		self._producer.send(Queues.CONTROL, msg)
			
		bus.fire("reboot_start")
		
	
	def on_IntServerHalt(self, message):
		msg = self._msg_service.new_message(Messages.HOST_DOWN)
		self._put_broadcast_data(msg)
		
		bus.fire("before_host_down", msg)		
		self._producer.send(Queues.CONTROL, msg)

		bus.fire("host_down")

	def on_HostInitResponse(self, message):
		bus.fire("host_init_response", message)
		
		msg = self._msg_service.new_message(Messages.HOST_UP)
		self._put_broadcast_data(msg)
		
		bus.fire("before_host_up", msg)
		self._producer.send(Queues.CONTROL, msg)
		
		bus.fire("host_up")

	def on_before_message_send(self, queue, message):
		"""
		Add scalarizr version to meta
		"""
		message.meta[MetaOptions.SZR_VERSION] = scalarizr.__version__
	
	