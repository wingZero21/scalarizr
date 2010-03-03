
import logging

class Observable(object):
	
	_listeners = {}
	_events_suspended = False
	
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
	
	def on(self, event, *args):
		if not self._listeners.has_key(event):
			raise Exception("Event '%s' is not defined" % event)
		for listener in args:
			if not listener in self._listeners[event]:
				self._listeners[event].append(listener)
	
	def un(self, event, listener):
		if self._listeners.has_key(event):
			if listener in self._listeners[event]:
				self._listeners[event].remove(listener)
	
	def suspend_events(self):
		self._events_suspended = True
	
	def resume_events(self):
		self._events_suspended = False


def inject_config(config, inj_config, sections_prefix=""):
	logger = logging.getLogger(__name__)
	
	logger.debug("Injecting config with a section prefix: '%s'", sections_prefix)
	for inj_section in inj_config.sections():
		section = sections_prefix + inj_section
		config.add_section(section)
		logger.debug("Inject section %s as %s", inj_section, section)
		for k, v in inj_config.items(inj_section):
			config.set(section, k, v)
	logger.debug("%d sections injected", len(inj_config.sections()))		

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
	return "%."+precision+"f%s" % (ret, dim)	
	

import binascii
class _CryptoUtil(object):
	def keygen(self, length=40):
		from Crypto.Util.randpool import RandomPool
		from Crypto.Hash import SHA256
						
		pool = RandomPool(hash=SHA256)
		pool.stir()
		return binascii.b2a_base64(pool.get_bytes(length))	
			
	def _init_chiper(self, key):
		from Crypto.Cipher import Blowfish
				
		k = binascii.a2b_base64(key)
		return Blowfish.new(k[0:len(k)-9], Blowfish.MODE_CFB, k[len(k)-8:])		
		
	def encrypt (self, s, key):
		c = self._init_chiper(key)
		return binascii.b2a_base64(c.encrypt(s))
	
	def decrypt (self, s, key):
		c = self._init_chiper(key)
		return c.decrypt(binascii.a2b_base64(s))
		
_crypto_util = None
def CryptoUtil():
	global _crypto_util
	if _crypto_util is None:
		_crypto_util = _CryptoUtil()
	return _crypto_util
