'''
Created on Dec 24, 2009

@author: marat
'''

from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.messaging import Queues, Messages
from scalarizr.util import parse_size, format_size
from scalarizr.util.filetool import write_file
import threading
from scalarizr.config import ScalarizrState
try:
	import time
except ImportError:
	import timemodule as time
import subprocess
import re
import os
import shutil
import stat
import logging
import binascii



def get_handlers ():
	return [ScriptExecutor()]

skip_events = set()
"""
@var ScriptExecutor will doesn't request scripts on passed events 
"""

class ScriptExecutor(Handler):
	name = "script_executor"
	
	OPT_EXEC_DIR_PREFIX = "exec_dir_prefix"
	OPT_LOGS_DIR_PREFIX = "logs_dir_prefix"
	OPT_LOGS_TRUNCATE_OVER = "logs_truncate_over"	
	
	_logger = None
	_queryenv = None
	_msg_service = None
	_platform = None
	_cnf = None
	
	_event_name = None
	_num_pending_async = 0
	_cleaner_running = False
	_lock = None
	
	_exec_dir_prefix = None
	_exec_dir = None
	_logs_dir_prefix = None
	_logs_dir = None
	_logs_truncate_over = None
	
	_wait_async = False

	def __init__(self, wait_async=False):
		self._logger = logging.getLogger(__name__)	
		self._wait_async = wait_async
		
		self._queryenv = bus.queryenv_service
		self._msg_service = bus.messaging_service
		self._platform = bus.platform
		self._config = bus.config
		self._cnf = bus.cnf
		self._lock = threading.Lock()
		
		self.hashbang_re = re.compile('^#!(\S+)\s*')
		
		sect_name = self.name
		if not self._config.has_section(sect_name):
			raise Exception("Script executor handler is not configured. "
						    + "Config has no section '%s'" % sect_name)
		
		# read exec_dir_prefix
		self._exec_dir_prefix = self._config.get(sect_name, self.OPT_EXEC_DIR_PREFIX)
		if not os.path.isabs(self._exec_dir_prefix):
			self._exec_dir_prefix = bus.base_path + os.sep + self._exec_dir_prefix
			
		# read logs_dir_prefix
		self._logs_dir_prefix = self._config.get(sect_name, self.OPT_LOGS_DIR_PREFIX)
		if not os.path.isabs(self._logs_dir_prefix):
			self._logs_dir_prefix = bus.base_path + os.sep + self._logs_dir_prefix
		
		# logs_truncate_over
		self._logs_truncate_over = parse_size(self._config.get(sect_name, self.OPT_LOGS_TRUNCATE_OVER))


	def exec_scripts_on_event (self, event_name, event_id=None):
		self._logger.debug("Fetching scripts for event %s", event_name)	
		scripts = self._queryenv.list_scripts(event_name, event_id)
		self._logger.debug("Fetched %d scripts", len(scripts))
		
		if scripts:
			self._logger.info("Executing %d script(s) on event %s", len(scripts), event_name)
			
			self._exec_dir = self._exec_dir_prefix + str(time.time())
			if not os.path.isdir(self._exec_dir):
				self._logger.debug("Create temp exec dir %s", self._exec_dir)
				os.makedirs(self._exec_dir)
			
			self._logs_dir = self._logs_dir_prefix + str(time.time())
			if not os.path.isdir(self._logs_dir):
				self._logger.debug("Create temp logs dir %s", self._logs_dir)
				os.makedirs(self._logs_dir) 

			if self._wait_async:
				async_threads = []
				

			if any(script.asynchronous for script in scripts) and not self._cleaner_running:
				self._num_pending_async = 0				
				c = threading.Thread(target=self._cleanup)
				c.setDaemon(True)

			for script in scripts:
				self._logger.debug("Execute script '%s' in %s mode; exec timeout: %d", 
								script.name, "async" if script.asynchronous else "sync", script.exec_timeout)
				if script.asynchronous:
					self._lock.acquire()
					self._num_pending_async += 1
					if not self._cleaner_running:
						c.start()
						self._cleaner_running = True
					self._lock.release()
					
					# Start new thread
					t = threading.Thread(target=self._execute_script, args=[script])
					t.start()
					if self._wait_async:
						async_threads.append(t)
				else:
					self._execute_script(script)

			# Wait
			if self._wait_async:
				for t in async_threads:
					t.join()
			
					
	def _cleanup(self):
		try:
			self._logger.debug("[cleanup] Starting")		
			while self._num_pending_async > 0:
				time.sleep(0.5)
			self._logger.debug("[cleanup] Doing cleanup")
			shutil.rmtree(self._exec_dir)
			shutil.rmtree(self._logs_dir)
			self._logger.debug("[cleanup] Done")
		finally:
			self._cleaner_running = False

			
	def _execute_script(self, script):
		# Create script file in local fs
		script_path = os.path.join(self._exec_dir, script.name)
		stdout_path = os.path.join(self._logs_dir, script.name + "-out")
		stderr_path = os.path.join(self._logs_dir, script.name + "-err")
		try:
			self._logger.debug("Put script contents into file %s", script_path)
			
			write_file(script_path, script.body, logger=self._logger)
			os.chmod(script_path, stat.S_IREAD | stat.S_IEXEC)
			self._logger.debug("%s exists: %s", script_path, os.path.exists(script_path))
	
			self._logger.debug("Executing script '%s'", script.name)
			
			# Create stdout and stderr log files
			stdout = open(stdout_path, "w")
			stderr = open(stderr_path, "w")
			self._logger.debug("Redirect stdout > %s stderr > %s", stdout.name, stderr.name)		
	
	
			self._logger.debug("Finding interpreter path in the scripts first line")
			interpreter = ''
			hashbang = re.search(self.hashbang_re,script.body)
			if hashbang:
				interpreter = hashbang.group(1)
				self._logger.debug("Found interpreter %s", interpreter)
			if hashbang and not os.path.exists(interpreter):
				stderr.write('Script execution failed: Interpreter %s does not exist.' % interpreter)
				elapsed_time = 0
				
			else:
				# Start process
				try:
					proc = subprocess.Popen(script_path, stdout=stdout, stderr=stderr)
				except OSError, e:
					self._logger.error("Cannot execute script '%s' (script path: %s). %s", 
							script.name, script_path, str(e))
					stderr.write("Script execution failed: %s." % str(e))
					elapsed_time = 0
				else:
					# Communicate with process
					self._logger.debug("Communicate with '%s'", script.name)
					start_time = time.time()		
					while time.time() - start_time < script.exec_timeout:
						if proc.poll() is None:
							time.sleep(0.5)
						else:
							# Process terminated
							self._logger.debug("Script '%s' terminated", script.name)
							break
					else:
						# Process timeouted
						self._logger.warn("Script '%s' execution timeout (%d seconds). Kill process", 
								script.name, script.exec_timeout)
						if hasattr(proc, "kill"):
							# python >= 2.6
							proc.kill()
						else:
							import signal
							os.kill(proc.pid, signal.SIGKILL)
									
					elapsed_time = time.time() - start_time
			
			stdout.close()
			stderr.close()
			
			self._logger.debug("Script '%s' execution finished. Elapsed time: %.2f seconds, stdout: %s, stderr: %s", 
					script.name, elapsed_time, 
					format_size(os.path.getsize(stdout.name)), 
					format_size(os.path.getsize(stderr.name)))
			
			# Notify scalr
			self._send_message(Messages.EXEC_SCRIPT_RESULT, dict(
				stdout=binascii.b2a_base64(self._get_truncated_log(stdout.name, self._logs_truncate_over)),
				stderr=binascii.b2a_base64(self._get_truncated_log(stderr.name, self._logs_truncate_over)),
				time_elapsed=elapsed_time,
				script_name=script.name,
				script_path=script_path,
				event_name=self._event_name
			), queue=Queues.LOG)
			
		except (Exception, BaseException), e:
			self._logger.error("Caught exception while execute script '%s'", script.name)
			self._logger.exception(e)
			
		finally:
			os.remove(script_path)
			os.remove(stderr_path)
			os.remove(stdout_path)
			self._lock.acquire()
			self._num_pending_async -= 1
			self._lock.release()

	
	def _get_truncated_log(self, logfile, maxsize):
		f = open(logfile, "r")
		try:
			ret = f.read(int(maxsize))
			if (os.path.getsize(logfile) > maxsize):
				ret += "... Truncated. See the full log in " + logfile
			return ret
		finally:
			f.close()
				
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return not message.name in skip_events
	
	def __call__(self, message):
		self._event_name = message.event_name if message.name == Messages.EXEC_SCRIPT else message.name
		self._logger.debug("Scalr notified me that '%s' fired", self._event_name)		
		
		if self._cnf.state == ScalarizrState.IMPORTING:
			self._logger.debug('Scripting is OFF when state: %s', ScalarizrState.IMPORTING)
			return
		
		if message.name == Messages.EXEC_SCRIPT:
			self.exec_scripts_on_event(self._event_name, message.meta["event_id"])
		else:
			self.exec_scripts_on_event(self._event_name)
