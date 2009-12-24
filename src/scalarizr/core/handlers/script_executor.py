'''
Created on Dec 24, 2009

@author: marat
'''

from scalarizr.core import Bus, BusEntries
from scalarizr.core.handlers import Handler
import threading
import timemodule as time
import subprocess
import os
import logging

def get_handlers ():
	return [ScriptExecutor()]

class ScriptExecutor(Handler):
	_logger = None
	_config = None
	_queryenv = None
	_platform = None
	_tmp_dir = None
	
	def __init__(self):
		self._logger = logging.getLogger(__package__ + "." + self.__class__.__name__)
		self._queryenv = Bus()[BusEntries.QUERYENV_SERVICE]
		self._platform = Bus()[BusEntries.PLATFORM]
		self._config = Bus()[BusEntries.CONFIG]
		
	def on_EventNotice(self, message):
		self._logger.debug("Entering on_EventNotice")
		
		internal_ip = message.body["InternalIP"]
		role_name = message.body["RoleName"]
		event_name = message.body["EventName"]
		self._logger.info("Received event notice (event_name: %s, role_name: %s, ip: %s)", 
						event_name, role_name, internal_ip)
		
		if self._platform.get_private_ip() == internal_ip:
			self._logger.info("Ignore event with the same ip as mine")
			return 
		
		if internal_ip == "0.0.0.0":
			self._logger.info("Custom event %s fired", event_name)
		else:
			self._logger.info("Scalr notified me that %s (role: $s) fired event %s", 
							internal_ip, role_name, event_name)
		
		self._logger.debug("Fetching scripts for event %s", event_name)	
		scripts = self._queryenv.list_scripts(event_name)
		self._logger.debug("Fetched %d scripts", len(scripts))
		
		if len(scripts) > 0:
			self._logger.info("Executing scripts on event %s fired on host %s", 
							event_name, internal_ip)
			
			self._tmp_dir = self._config.get("handler_script_executor", "tmp_dir_prefix") + os.path.basename(os.tempnam())
			os.makedirs(self._tmp_dir) 

			for script in scripts:
				self._logger.debug("Execute script %s in %s mode; exec timeout: %d", 
								script.name, "async" if script.asynchronous else "sync", script.exec_timeout)
				if script.asynchronous:
					# Start new thread
					t = threading.Thread(target=self._execute_script, args=(script))
					t.start()
				else:
					self._execute_script(script)
				
			# не забыть замочить _tmp_dir
			
	def _execute_script(self, script):
		
		f = os.tmpfile()
		
		subprocess.Popen()
		start_time = time.time()
		
		pass
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == "EventNotice"