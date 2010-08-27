import os
import logging
import threading
import weakref
import time
import sys
import socket


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
					ln(*args, **kwargs)

	
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
				self._logger.debug("Current weakref is empty")
		except AttributeError, e:
			self._logger.debug("Caught: %s", e)
		
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
	
def cached(f, cache={}):
	'''
	Decorator
	'''
	def g(*args, **kwargs):
		key = (f, tuple(args), frozenset(kwargs.items()))
		if key not in cache:
			cache[key] = f(*args, **kwargs)
		return cache[key]
	return g	

def firstmatched(function, sequence, default=None):
	for s in sequence:
		if function(s):
			return s
			break
	else:
		return default	






def daemonize():
	# First fork
	pid = os.fork()
	if pid > 0:
		sys.exit(0) 	
	
	os.chdir("/")
	os.setsid()
	os.umask(0)
	
	# Second fork
	pid = os.fork()
	if pid > 0:
		sys.exit(0)
		
	# Redirect standard file descriptors
	sys.stdout.flush()
	sys.stderr.flush()
	si = file(os.devnull, 'r')
	so = file(os.devnull, 'a+')
	se = file(os.devnull, 'a+', 0)
	os.dup2(si.fileno(), sys.stdin.fileno())
	os.dup2(so.fileno(), sys.stdout.fileno())
	os.dup2(se.fileno(), sys.stderr.fileno())
	
	
def system(args, shell=True):
	import subprocess
	logger = logging.getLogger(__name__)
	logger.debug("system: %s", args)
	p = subprocess.Popen(args, shell=shell, env={'LANG' : 'en_US'}, 
			stdout=subprocess.PIPE, stderr=subprocess.PIPE)
	out, err = p.communicate()
	if out:
		logger.debug("stdout: " + out)
		#print "stdout: " + out
	if err:
		logger.warning("stderr: " + err)
		#print "stderr: " + err
	return out, err, p.returncode


def wait_until(target, args=None, sleep=5, logger=None):
	args = args or ()
	while not target(*args):
		if logger:
			logger.debug("Wait %d seconds before the next attempt", sleep)
		time.sleep(sleep)


def xml_strip(el):
	for child in list(el.childNodes):
		if child.nodeType==child.TEXT_NODE and child.nodeValue.strip() == '':
			el.removeChild(child)
		else:
			xml_strip(child)
	return el	

def url_replace_hostname(url, newhostname):
	import urlparse	
	r = url if isinstance(url, tuple) else urlparse.urlparse(url)
	r2 = list(r)
	r2[1] = newhostname
	if r.port:
		r2[1] += ":" + str(r.port)
	return urlparse.urlunparse(r2)
	

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
		ret = ret/1000
		dim = "K"
	if ret > 1000:
		ret = ret/1000
		dim = "M"
		
	s = "%."+str(precision)+"f%s"
	return s % (ret, dim)	

def backup_file(filename):
	import shutil
	logger = logging.getLogger(__name__)
	max_backups = 50
	
	for i in range(0, max_backups):
		bkname = '%s.bak.%s' % (filename, i)		
		if not os.path.exists(bkname):
			logger.debug('Backuping %s to %s', filename, bkname)
			shutil.copy(filename, bkname)
			return bkname
	raise UtilError("Max backups limit %d exceed for file %s" % (max_backups, filename))


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
	import scalarizr as szr
	from scalarizr.bus import bus
	bus.etc_path = os.path.realpath(os.path.dirname(__file__) + "/../../../test/resources/etc")
	szr._init()
	bus.cnf.bootstrap()

	
def ping_service(host=None, port=None, timeout=None, proto='tcp'):
	if None == timeout:
		timeout = 5
	if 'udp' == proto:
		socket_proto = socket.SOCK_DGRAM
	else:
		socket_proto = socket.SOCK_STREAM
	s = socket.socket(socket.AF_INET, socket_proto)
	time_start = time.time()
	while time.time() - time_start < timeout:
		try:
			s.connect((host, port))
			s.shutdown(2)
			return
		except:
			time.sleep(0.1)
			pass
	raise UtilError ("Service unavailable after %d seconds of waiting" % timeout)