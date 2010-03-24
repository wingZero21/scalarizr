
import logging

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


def save_config():
	from scalarizr.core import Bus, BusEntries
	logger = logging.getLogger(__name__)
	bus = Bus()
	
	# Save configuration
	filename = bus[BusEntries.BASE_PATH] + "/etc/config.ini"
	logger.debug("Save configuration into '%s'" % filename)
	f = open(filename, "w")
	bus[BusEntries.CONFIG].write(f)
	f.close()	
	
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
	

import binascii
class CryptoUtil(object):
	_instance = None
	
	def __new__(cls):
		if cls._instance is None:
			cls._instance = object.__new__(cls)
		return cls._instance
	
	def keygen(self, length=40):
		from M2Crypto.Rand import rand_bytes
		return binascii.b2a_base64(rand_bytes(length))	
			
	def _init_chiper(self, key, op_enc=1):
		from M2Crypto.EVP import Cipher
		k = binascii.a2b_base64(key)
		return Cipher("bf_cfb", k[0:len(k)-9], k[len(k)-8:], op=op_enc)

		
	def encrypt (self, s, key):
		c = self._init_chiper(key, 1)
		ret = c.update(s)
		ret += c.final()
		del c
		return binascii.b2a_base64(ret)
	
	def decrypt (self, s, key):
		c = self._init_chiper(key, 0)
		ret = c.update(binascii.a2b_base64(s))
		ret += c.final()
		del c
		return ret

	_BUF_SIZE = 1024 * 1024	 # Buffer size in bytes
	
	def digest_file(self, digest, file):
		while 1:
			buf = file.read(self._BUF_SIZE)
			if not buf:
				break;
			digest.update(buf)
		return digest.final()

	def crypt_file(self, cipher, in_file, out_file):
		while 1:
			buf = in_file.read(self._BUF_SIZE)
			if not buf:
				break
			out_file.write(cipher.update(buf))
		out_file.write(cipher.final())
	
