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
	def __init__(self, creator, pool_size=10):
		self._logger = logging.getLogger(__name__)
		self._creator = creator		
		self._object = threading.local()
		
		self._all_conns = set()
		self.size = pool_size
	
	def do_create(self):
		return self._creator()
	
	def get(self):
		try:
			o = self._object.current
			if o():
				return o()
			else:
				self._logger.warn("Current weakref is empty")
		except AttributeError, e:
			self._logger.warn(str(e))
			pass
		
		self._logger.debug("Creating new object...")
		o = self.do_create()
		self._logger.debug("Created %s", o)
		self._object.current = weakref.ref(o)
		self._logger.debug("Added weakref %s", self._object.current)
		self._all_conns.add(o)
		if len(self._all_conns) > self.size:
			self.cleanup()
		return o
	
	def cleanup(self):
		for conn in list(self._all_conns):
			self._all_conns.discard(conn)
			if len(self._all_conns) <= self.size:
				return
	
class SqliteLocalObject(LocalObject):
	def do_create(self):
		return _SqliteConnection(self, self._creator)
	
class _SqliteConnection(object):
	_conn = None
	#_lo = None
	_creator = None
	
	def __init__(self, lo, creator):
		#self._lo = lo
		self._creator = creator
	
	def get_connection(self):
		if not self._conn:
			self._conn = self._creator()
		return self._conn
	
	
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

"""
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
"""

def init_tests():
	logging.basicConfig(
			format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", 
			stream=sys.stdout, 
			level=logging.DEBUG)
	from scalarizr.bus import bus
	bus.etc_path = os.path.realpath(os.path.dirname(__file__) + "/../../../test/resources/etc")
	from scalarizr import _init
	_init()