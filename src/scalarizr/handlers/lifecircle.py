'''
Created on Mar 3, 2010

@author: marat
'''

from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.messaging import Queues, Messages
from scalarizr.util import cryptotool, configtool
import logging
import os


def get_handlers():
	return [LifeCircleHandler()]

class LifeCircleHandler(Handler):
	_logger = None
	_bus = None
	_msg_service = None
	_producer = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		
		bus.define_events(
			# Fires before HostInit message is sent
			"before_host_init",
			
			# Fires after HostInit message is sent
			"host_init",
			
			# Fires after RebootStart message is sent
			"reboot_start",
			
			# Fires before RebootFinish message is sent
			"before_reboot_finish",
			
			# Fires after RebootFinish message is sent
			"reboot_finish",
			
			# Fires after Go2Halt message is sent
			"go2halt",
			
			# Fires after HostDown message is sent
			"host_down"
		)
		bus.on("init", self.on_init)
		
		self._msg_service = bus.messaging_service
		self._producer = self._msg_service.get_producer()
	
	
	def on_init(self):
		bus.on("start", self.on_start)
		self._producer.on("before_send", self.on_before_message_send)
		
	
	def on_start(self):
		reboot_file = os.path.join(bus.etc_path, ".reboot")
		if not os.path.exists(reboot_file):
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
			
			# Notify listeners
			bus.fire("before_host_init")
			
			# Regenerage key
			config = bus.config
			key_path = config.get(configtool.SECT_GENERAL, configtool.OPT_CRYPTO_KEY_PATH)
			key = cryptotool.keygen()
			configtool.write_key(key_path, key, key_title="Scalarizr crypto key")
			
			# Update key in QueryEnv
			queryenv = bus.queryenv_service
			queryenv.key = key
			
			# Update key in Messaging
			# FIXME: disclosure of the interface implementation
			msg_service = bus.messaging_service
			msg_service.get_consumer().crypto_key = key
			msg_service.get_producer().crypto_key = key

			# Send HostInit			
			msg = self._msg_service.new_message(Messages.HOST_INIT)
			msg.key = key
			self._producer.send(Queues.CONTROL, msg) 

			# Notify listeners
			bus.fire("host_init")
			
		else:
			self._logger.info("Scalarizr is resumed after reboot")
			os.remove(reboot_file)
			
			# Notify listeners
			bus.fire("before_reboot_finish")
			
			# Send RebootFinish
			msg = self._msg_service.new_message(Messages.REBOOT_FINISH)
			self._producer.send(Queues.CONTROL, msg)
			
			# Notify listeners
			bus.fire("reboot_finish")


	def on_ServerReboot(self, message):
		# Scalarizr must detect that it was resumed after reboot
		reboot_file = os.path.join(bus.etc_path, ".reboot")
		try:
			self._logger.debug("Touch file '%s'", reboot_file)
			open(reboot_file, "w+").close()
		except IOError, e:
			self._logger.error("Cannot touch file '%s'. IOError: %s", reboot_file, str(e))
			
		# Send message 
		msg = self._msg_service.new_message(Messages.REBOOT_START)
		self._producer.send(Queues.CONTROL, msg)
			
		bus.fire("reboot_start")
			
		# Shutdown routine
		#self._shutdown()
		
	
	def on_ServerHalt(self, message):
		#msg = self._msg_service.new_message(Messages.GO2HALT)
		#self._producer.send(Queues.CONTROL, msg)
		
		#bus.fire("go2halt")

		# Shutdown routine
		#self._shutdown()
		
		msg = self._msg_service.new_message(Messages.HOST_DOWN)
		self._producer.send(Queues.CONTROL, msg)

		bus.fire("host_down")

	"""
	def _shutdown(self):
		msg = self._msg_service.new_message(Messages.HOST_DOWN)
		self._producer.send(Queues.CONTROL, msg)

		bus.fire("host_down")
	"""

	def on_before_message_send(self, queue, message):
		"""
		@todo: Add scalarizr version to meta
		"""
		pass
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.SERVER_REBOOT \
			or message.name == Messages.SERVER_HALT	
	