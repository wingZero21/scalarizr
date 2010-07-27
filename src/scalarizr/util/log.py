'''
Created on 22.01.2010

@author: Dmytro Korsakov
'''
import re
import logging
import logging.config
import threading
import traceback
import cStringIO
import string
import os

from scalarizr.bus import bus
from scalarizr.messaging import Queues, Messages



INTERVAL_RE = re.compile('((?P<minutes>\d+)min\s?)?((?P<seconds>\d+)s)?')

class RotatingFileHandler(logging.handlers.RotatingFileHandler):
	def __init__(self, filename, mode, maxBytes, backupCount, chmod = 0600):
		logging.handlers.RotatingFileHandler.__init__(self, filename, mode, maxBytes, backupCount)
		try:
			os.chown(self.baseFilename, os.getuid(), os.getgid())
			os.chmod(self.baseFilename, chmod)
		except OSError:
			pass
		
class MessagingHandler(logging.Handler):
	
	num_entries = None
	send_interval = None
	
	_sender_thread = None
	_send_event = None
	_stop_event = None
	_initialized = False
	
	def __init__(self, num_entries = 1, send_interval = "1s"):
		logging.Handler.__init__(self)	
		
		m = INTERVAL_RE.match(send_interval)
		self.send_interval = (int(m.group('seconds') or 0) + 60*int(m.group('minutes') or 0)) or 1
		
		self.num_entries = num_entries		
	
		
	def __del__(self):
		if self._send_event:
			self._stop_event.set()
			self._send_event.set()
			
	def _init(self):
		self.entries = []		
		
		self._send_event = threading.Event()
		self._stop_event = threading.Event()
		self._lock = threading.Lock()
		
		self._sender_thread = threading.Thread(target=self._sender)
		self._sender_thread.setDaemon(True)
		self._sender_thread.start()		

		self._initialized = True		


	def emit(self, record):
		
		if not self._initialized:
			self._init()
		
		msg = str(record.msg) % record.args if record.args else str(record.msg)
		
		stack_trace = None
		if record.exc_info:
			output = cStringIO.StringIO()
			traceback.print_tb(record.exc_info[2], file=output)
			stack_trace =  output.getvalue()
			output.close()			

		ent = dict(
			name = record.name,
			level = record.levelname,
			pathname = record.pathname,
			lineno = record.lineno,
			msg = msg,
			stack_trace = stack_trace
		)
		
		self._lock.acquire()		
		try:
			self.entries.append(ent)
			if len(self.entries) >= self.num_entries:
				self._send_event.set()
		finally:
			self._lock.release()
			
	def _send_message(self):
		entries = ()
		self._lock.acquire()
		try:
			entries = self.entries[:]
			self.entries = []
		finally:
			self._lock.release()
		
		try:
			if entries:
				msg_service = bus.messaging_service
				message = msg_service.new_message(Messages.LOG)
				message.body["entries"] = entries
				logger = logging.getLogger("scalarizr")
				logger.removeHandler(self)	
				msg_service.get_producer().send(Queues.LOG, message)
				logger.addHandler(self)
		except (BaseException, Exception):
			# silently
			pass	

	
	def _sender(self):
		while not self._stop_event.isSet():
			try:
				self._send_event.wait(self.send_interval)
				self._send_message()
			finally:
				self._send_event.clear()

	
def fix_python25_handler_resolve():
	
	def _resolve(name):
		"""Resolve a dotted name to a global object."""
		name = string.split(name, '.')
		used = name.pop(0)
		found = __import__(used)
		for n in name:
			used = used + '.' + n
			try:
				found = getattr(found, n)
			except AttributeError:
				__import__(used)
				found = getattr(found, n)
		return found
	
	def _strip_spaces(alist):
		return map(lambda x: string.strip(x), alist)
	
	def _install_handlers(cp, formatters):
		"""Install and return handlers"""
		hlist = cp.get("handlers", "keys")
		if not len(hlist):
			return {}
		hlist = string.split(hlist, ",")
		hlist = _strip_spaces(hlist)
		handlers = {}
		fixups = [] #for inter-handler references
		for hand in hlist:
			sectname = "handler_%s" % hand
			klass = cp.get(sectname, "class")
			opts = cp.options(sectname)
			if "formatter" in opts:
				fmt = cp.get(sectname, "formatter")
			else:
				fmt = ""
			try:
				klass = eval(klass, vars(logging))
			except (AttributeError, NameError):
				klass = _resolve(klass)
			args = cp.get(sectname, "args")
			args = eval(args, vars(logging))
			h = klass(*args)
			if "level" in opts:
				level = cp.get(sectname, "level")
				h.setLevel(logging._levelNames[level])
			if len(fmt):
				h.setFormatter(formatters[fmt])
			if issubclass(klass, logging.handlers.MemoryHandler):
				if "target" in opts:
					target = cp.get(sectname,"target")
				else:
					target = ""
				if len(target): #the target handler may not be loaded yet, so keep for later...
					fixups.append((h, target))
			handlers[hand] = h
			
		#now all handlers are loaded, fixup inter-handler references...
		for h, t in fixups:
			h.setTarget(handlers[t])
		return handlers

	logging.config._install_handlers = _install_handlers
	logging.config._resolve = _resolve
	logging.config._strip_spaces = _strip_spaces	
	
			