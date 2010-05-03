import os
import logging
import threading
import weakref
import time
import sys


class UtilError(BaseException):
	pass

class Observable(object):
	
	def __init__(self):
		self._listeners = {}
		self._events_suspended = False
	
	def define_events(self, *args):
		for event in args:
			self._listeners[event] = list()
	
	def list_events(self):
		return self._listeners.keys()
	
	def fire(self, event, *args, **kwargs):
		logger = logging.getLogger(__name__)
		logger.debug(self.__class__.__name__ + " fires " + event)
		if not self._events_suspended:
			if self._listeners.has_key(event):
				for ln in self._listeners[event]:
					try:
						ln(*args, **kwargs)
					except (Exception, BaseException), e:
						logger.exception(e)
	
	def on(self, *args, **kwargs):
		"""
		Add listener
		
		1) Add listeners to one event
		obj.on("add", func1, func2, ...)
		2) Add listeners to many events
		obj.on(add=func1, remove=func2, apply=func3, ...)
		"""
		if len(args) >= 2:
			event = args[0]
			if not self._listeners.has_key(event):
				raise Exception("Event '%s' is not defined" % event)
			for listener in args[1:]:
				if not listener in self._listeners[event]:
					self._listeners[event].append(listener)
		elif kwargs:
			for event in kwargs.keys():
				self.on(event, kwargs[event])
	
	def un(self, event, listener):
		"""
		Remove listener
		"""
		if self._listeners.has_key(event):
			if listener in self._listeners[event]:
				self._listeners[event].remove(listener)
	
	def suspend_events(self):
		self._events_suspended = True
	
	def resume_events(self):
		self._events_suspended = False

	
class LocalObject:
	
	def __init__(self, creator):
		self._logger = logging.getLogger(__name__)
		self._creator = creator		
		self._object = threading.local()
	
	def get(self):
		try:
			o = self._object.current()
			if o:
				return o
			else:
				self._logger.warn("Current weakref is empty")
		except AttributeError, e:
			self._logger.warn(str(e))
			pass
		
		self._logger.info("Creating new object...")
		o = self._creator()
		self._logger.info("Created %s", o)
		self._object.current = weakref.ref(o)
		self._logger.info("Added weakref %s", self._object.current)
		return o
	
# TODO: LocalObject need to be extended to support SQLite connections managing
# Current problems:
# 1. Cannot create weak reference to 'sqlite3.Connection' object
# 2. If we use any wrapper object for sqlite that returns connection with get_connection() method
#    LocalObject lose weakref with this code:
#    conn = localobj.get().get_connection()
#    but weakref alives with this code:
#    wrap = localobj.get()
#    conn = wrap.get_connection()
	
	
def system(args, shell=True):
	import subprocess
	logger = logging.getLogger(__name__)
	logger.debug("system: " + args if isinstance(args, str) else " ".join(args))
	p = subprocess.Popen(args, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	if out:
		logger.debug("stdout: " + out)
	if err:
		logger.warning("stderr: " + err)
	return out, err, p.returncode

def xml_strip(el):
	for child in list(el.childNodes):
		if child.nodeType==child.TEXT_NODE and child.nodeValue.strip() == '':
			el.removeChild(child)
		else:
			xml_strip(child)	

def parse_size(size):
	"""
	Read string like 10K, 12M, 1014B and return size in bytes
	"""
	ret = str(size)
	dim = ret[-1]		
	ret = float(ret[0:-1])
	if dim.lower() == "b":
		pass		
	elif dim.lower() == "k":
		ret *= 1024
	elif dim.lower() == "m":
		ret *= 1048576	
	
	return ret
	
def format_size(size, precision=2):
	"""
	Format size in Bytes, KBytes and MBytes
	"""
	ret = float(size)
	dim = "B"
	if ret > 1000:
		ret = ret/1024
		dim = "K"
	if ret > 1000:
		ret = ret/1024
		dim = "M"
		
	s = "%."+str(precision)+"f%s"
	return s % (ret, dim)	

def timethis(what):
	try:
		import time
	except ImportError:
		import timemodule as time
	from contextlib import contextmanager	
	
	@contextmanager
	def benchmark():
		start = time.time()
		yield
		end = time.time()
		print("%s : %0.3f seconds" % (what, end-start))
	if hasattr(what,"__call__"):
		def timed(*args,**kwargs):
			with benchmark():
				return what(*args,**kwargs)
		return timed
	else:
		return benchmark()


def init_tests():
	logging.basicConfig(
			format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", 
			stream=sys.stdout, 
			level=logging.DEBUG)
	
	from scalarizr import _init
	_init()
