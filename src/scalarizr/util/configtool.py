'''
Created on Apr 1, 2010

@author: marat
'''
from scalarizr.bus import bus
import binascii
import os

class ConfigError(BaseException):
	pass

RET_BOTH = 1
RET_PUBLIC = 2
RET_PRIVATE = 3

SECT_GENERAL = "general"
OPT_SERVER_ID = "server_id"
OPT_BEHAVIOUR = "behaviour"
OPT_STORAGE_PATH = "storage_path"
OPT_CRYPTO_KEY_PATH = "crypto_key_path"
OPT_PLATFORM = "platform"
OPT_QUERYENV_URL = "queryenv_url"

SECT_MESSAGING = "messaging"
OPT_ADAPTER = "adapter"

SECT_HANDLERS = "handlers"

def _get_filename(basename, ret):
	etc_path = bus.etc_path
	if ret == RET_PUBLIC:
		return os.path.join(etc_path, "public.d", basename)
	elif ret == RET_PRIVATE:
		return os.path.join(etc_path, "private.d", basename)
	elif ret == RET_BOTH:
		return (os.path.join(etc_path, "public.d", basename), 
			os.path.join(etc_path, "private.d", basename))
	else:
		raise ConfigError("Incorrect method call.`ret` must be one of RET_* constants")

def get_handler_filename(handler_name, ret=RET_BOTH):
	return _get_filename("handler.%s.ini" % (handler_name), ret)

def get_behaviour_filename(behaviour_name, ret=RET_BOTH):
	return _get_filename("behaviour.%s.ini" % (behaviour_name), ret)	

def get_platform_filename(platform_name, ret=RET_BOTH):
	return _get_filename("platform.%s.ini" % (platform_name), ret)

def get_platform_section_name(platform_name):
	return "platform_%s" % (platform_name)

def get_behaviour_section_name(behaviour_name):
	return "behaviour_%s" % (behaviour_name)

def get_handler_section_name(handler_name):
	return "handler_%s" % (handler_name)
	
def get_key_filename(key_name, private=True):
	etc_path = bus.etc_path
	return os.path.join(etc_path, "private.d" if private else "public.d", "keys", key_name)
	
def write_key(path, key, key_title=None, base64encode=False):
	"""
	Writes key into $etc/.private.d/keys, $etc/public.d/keys
	"""
	filename = os.path.join(bus.etc_path, path)
	file = None
	try:
		file = open(filename, "w+")
		file.write(binascii.b2a_base64(key) if base64encode else key)
		os.chmod(filename, 0400)
	except OSError, e:
		raise ConfigError("Cannot write %s in file '%s'. %s" % (key_title or "key", filename, str(e)))
	finally:
		if file:
			file.close()
	
def read_key(path, key_title=None):
	"""
	Reads key from $etc/.private.d/keys, $etc/public.d/keys
	"""
	filename = os.path.join(bus.etc_path, path)
	file = None
	try:
		file = open(filename, "r")
		return file.read().strip()
	except OSError, e:
		raise ConfigError("Cannot read %s file '%s'. %s" % (key_title or "key", filename, str(e)))
	finally:
		if file:
			file.close()


def save(config, filename):
	# TODO: implement with comments preserved
	raise ConfigError("Not implemented")