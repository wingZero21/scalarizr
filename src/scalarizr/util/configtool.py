'''
Created on Apr 1, 2010

@author: marat
@deprecated
'''
from scalarizr.bus import bus
from ConfigParser import RawConfigParser
import logging
import binascii
import os
import shutil


#TODO method to remove individual options

class ConfigError(BaseException):
	pass

RET_BOTH = 1
RET_PUBLIC = 2
RET_PRIVATE = 3

#TODO: better to move these constants into 'scalarizr' package 
SECT_GENERAL = "general"
OPT_SERVER_ID = "server_id"
OPT_BEHAVIOUR = "behaviour"
OPT_ROLE_NAME = "role_name"
OPT_STORAGE_PATH = "storage_path"
OPT_CRYPTO_KEY_PATH = "crypto_key_path"
OPT_FARM_CRYPTO_KEY_PATH = "farm_crypto_key_path"
OPT_PLATFORM = "platform"
OPT_QUERYENV_URL = "queryenv_url"
OPT_SCRIPTS_PATH = "scripts_path"

SECT_MESSAGING = "messaging"
OPT_ADAPTER = "adapter"

SECT_SNMP = "snmp"
OPT_PORT = "port"
OPT_SECURITY_NAME = "security_name"
OPT_COMMUNITY_NAME = "community_name"

SECT_HANDLERS = "handlers"



def _get_filename(basename, ret):
	if ret == RET_PUBLIC:
		return os.path.join(get_public_d_path(), basename)
	elif ret == RET_PRIVATE:
		return os.path.join(get_private_d_path(), basename)
	elif ret == RET_BOTH:
		return (os.path.join(get_public_d_path(), basename), 
			os.path.join(get_private_d_path(), basename))
	else:
		raise ConfigError("Incorrect method call.`ret` must be one of RET_* constants")

def get_handler_basename(handler_name):
	return "handler.%s.ini" % handler_name

def get_handler_filename(handler_name, ret=RET_BOTH):
	return _get_filename(get_handler_basename(handler_name), ret)

def get_behaviour_basename(behaviour_name):
	return "behaviour.%s.ini" % behaviour_name

def get_behaviour_filename(behaviour_name, ret=RET_BOTH):
	return _get_filename(get_behaviour_basename(behaviour_name), ret)	

def get_platform_basename(platform_name):
	return "platform.%s.ini" % platform_name

def get_platform_filename(platform_name, ret=RET_BOTH):
	return _get_filename(get_platform_basename(platform_name), ret)

def get_handler_section_name(handler_name):
	return "handler_%s" % (handler_name)

def get_behaviour_section_name(behaviour_name):
	return "behaviour_%s" % (behaviour_name)

def get_platform_section_name(platform_name):
	return "platform_%s" % (platform_name)

def get_public_d_path(basename=None):
	args = [bus.etc_path, "public.d"]
	if basename:
		args.append(basename)
	return os.path.join(*args)

def get_private_d_path(basename=None):
	args = [bus.etc_path, "private.d"]
	if basename:
		args.append(basename)
	return os.path.join(*args)

def get_d_path(private, basename=None):
	return get_private_d_path(basename) if private else get_public_d_path(basename)
	
def get_key_filename(key_name, private=True):
	return os.path.join(get_d_path(private), "keys", key_name)
	
def write_key(path, key, key_title=None, private=None, base64encode=False):
	"""
	Writes key into $etc/.private.d/keys, $etc/public.d/keys
	"""
	filename = os.path.join(bus.etc_path, path) if private is None \
			else get_key_filename(os.path.basename(path), private)
	file = None
	try:
		keys_dir = os.path.dirname(filename)
		if not os.path.exists(keys_dir):
			os.makedirs(keys_dir)
		if os.path.exists(filename):
			os.chmod(filename, 0600)
		file = open(filename, "w+")
		file.write(binascii.b2a_base64(key) if base64encode else key)
		os.chmod(filename, 0400)
	except (IOError, OSError), e:
		raise ConfigError("Cannot write %s in file '%s'. %s" % (key_title or "key", filename, str(e)))
	finally:
		if file:
			file.close()
	
def read_key(path, key_title=None, private=None):
	"""
	Reads key from $etc/.private.d/keys, $etc/public.d/keys
	"""
	filename = os.path.join(bus.etc_path, path) if private is None \
			else get_key_filename(os.path.basename(path), private)
	file = None
	try:
		file = open(filename, "r")
		return file.read().strip()
	except IOError, e:
		raise ConfigError("Cannot read %s file '%s'. %s" % (key_title or "key", filename, str(e)))
	finally:
		if file:
			file.close()

def split_array(value, separator=",", allow_empty=False, ct=list):
	return ct(v.strip() for v in value.split(separator) if allow_empty or (not allow_empty and v)) if value else ct()

def update(filename, sections):
	class Comment:
		type = "comment"
		def __init__(self, text):
			self.text = text
		def __str__(self):
			return self.text
		
	class Option:
		type = "option"
		def __init__(self, key, value):
			self.key = key
			self.value = value
		def __str__(self):
			return "%s = %s%s" % (self.key, self.value, os.linesep)
	
	class Section:
		type = "section"
		def __init__(self, name):
			self.items = []
			self.name = name
		def __str__(self):
			ret = "[%s]%s" % (self.name, os.linesep)
			for item in self.items:
				ret += str(item)
			return ret
		
	class Config:
		def __init__(self):
			self.items = []
		def __str__(self):
			ret = ""
			for item in self.items:
				ret += str(item)
			return ret
		
	logger = logging.getLogger(__name__)
		
	# Read configuration from file
	bus_config = bus.config
	config = Config()
	if os.path.exists(filename):
		cursect = None
		sect_re = RawConfigParser.SECTCRE
		opt_re = RawConfigParser.OPTCRE
		fp = open(filename, "r")
		while True:
			line = fp.readline()
			if not line:
				break
			mo = sect_re.match(line)
			if mo:
				cursect = Section(mo.group('header').strip())
				config.items.append(cursect)
			else:
				mo = opt_re.match(line)
				if mo:
					cursect.items.append(Option(mo.group("option").strip(), mo.group("value").strip()))
				else:
					comment = Comment(line)
					if cursect:
						cursect.items.append(comment)
					else:
						config.items.append(comment)
		fp.close()
		fp = None
	
	
	logger.debug("Update configuration...")
	
	# Update configuration
	for sect_name in sections:
		logger.debug("Find section '%s' in existing sections", sect_name)
		cur_sect = None
		for section in [it for it in config.items if it.type == "section"]:
			logger.debug("Compare '%s' with '%s'", sect_name, section.name)
			if section.name == sect_name:
				logger.debug("Found '%s' in existing sections", sect_name)
				cur_sect = section
				break
		# Section not found
		if cur_sect is None:
			# Create new section and append it in the end
			logger.debug("Section '%s' wasn't found in existing sections", sect_name)
			logger.debug("Create section '%s' and append it in the end", sect_name)
			cur_sect = Section(sect_name)
			config.items.append(cur_sect)
		if not bus_config.has_section(cur_sect.name):
			bus_config.add_section(cur_sect.name)
			
		for opt_name, value in sections[sect_name].items():
			logger.debug("Find option '%s' in section '%s'", opt_name, sect_name)
			cur_opt = None
			for option in [it for it in cur_sect.items if it.type == "option"]:
				logger.debug("Compare '%s' with '%s'", opt_name, option.key)
				if option.key == opt_name:
					logger.debug("Found option '%s' in existing options in section '%s'", 
							opt_name, sect_name)
					cur_opt = option
					break
			# Option not found
			if cur_opt is None:
				logger.debug("Option '%s' wasn't found in existing options of section '%s'", 
						opt_name, sect_name)
				logger.debug("Create option '%s' and append it in the end of section '%s'", 
						opt_name, sect_name)
				# Create option and append it in the end
				cur_opt = Option(opt_name, value if value != None else "")
				cur_sect.items.append(cur_opt)
			else:
				cur_opt.value = value
			bus_config.set(cur_sect.name, cur_opt.key, value)
	

	logger.debug("Write result configuration into file '%s'", filename)
	fp = None
	try:
		if os.path.exists(filename):
			os.chmod(filename, 0600)		
		fp = open(filename, "w+")
		fp.write(str(config))
		os.chmod(filename, 0400)
	finally:
		if fp:
			fp.close()
			
	
class _OptionWrapper(object):
	def __init__(self, *args):
		if isinstance(args[0], _SectionWrapper):
			self.config = args[0].config
			self.section = args[0].section
			self.option = args[1]
		elif len(args) == 3:
			self.config = args[0]
			self.section = args[1]
			self.option = args[2]
		else:
			raise AttributeError()
		
		self.name = "[%s]%s" % (self.section, self.option)
	
	def get(self): return self.config.get(self.section, self.option)
	
	def set(self, value): self.config.set(self.section, self.option, value)
			
	def remove(self, name): return self.config.remove_option(self.section, self.option)
	
	def set_required(self, value, ex_class=ConfigError):
		if value:
			self.set(value)
		if not self.get():
			raise ex_class("Configuration option %s is not defined" % (self.name))

			
def option_wrapper(*args):
	return _OptionWrapper(*args)
	
	
class _SectionWrapper(object):
	def __init__(self, config, section):
		self.config = config
		self.section = section
		self.name = self.section
		
	def set(self, option, value): self.config.set(self.section, option, value)
	
	def get(self, option): return self.config.get(self.section, option)
	
	def remove_option(self, option): return self.config.remove_option(self.section, option)
	
	def remove(self): return self.config.remove_section(self.section)
	
	def option_wrapper(self, option):
		return _OptionWrapper(self, option)
	
def section_wrapper(config, section):
	return _SectionWrapper(config, section)

def mount_private_d(mpoint, privated_image, blocks_count):
	from scalarizr.util import system, fstool, format_size
	from scalarizr.util.filetool import Rsync
	logger = logging.getLogger(__name__)
	
	logger.debug("Move private.d configuration %s to mounted filesystem (img: %s, size: %s)", 
			mpoint, privated_image, format_size(1024*(blocks_count-1)))
	mtab = fstool.Mtab()
	if mtab.contains(mpoint=mpoint): # if privated_image exists
		logger.debug("private.d already mounted to %s", mpoint)
		return
	
	if not os.path.exists(mpoint):
		os.makedirs(mpoint)
		
	mnt_opts = ('-t auto', '-o loop,rw')	
	if not os.path.exists(privated_image):	
		build_image_cmd = 'dd if=/dev/zero of=%s bs=1024 count=%s 2>&1' % (privated_image, blocks_count-1)
		retcode = system(build_image_cmd)[2]
		if retcode:
			logger.error('Cannot create image device')
		os.chmod(privated_image, 0600)
			
		logger.debug("Creating file system on image device")
		fstool.mkfs(privated_image)
		
	if os.listdir(mpoint):
		logger.debug("%s contains data. Need to copy it ot image before mounting", mpoint)
		# If mpoint not empty copy all data to the image
		try:
			tmp_mpoint = "/mnt/tmp-privated"
			os.makedirs(tmp_mpoint)
			logger.debug("Mounting %s to %s", privated_image, tmp_mpoint)
			fstool.mount(privated_image, tmp_mpoint, mnt_opts)
			logger.debug("Copy data from %s to %s", mpoint, tmp_mpoint)
			system(str(Rsync().archive().source(mpoint+"/" if mpoint[-1] != "/" else mpoint).dest(tmp_mpoint)))
			private_list = os.listdir(mpoint)
			for file in private_list:
				path = os.path.join(mpoint, file)
				if os.path.isdir(path):
					shutil.rmtree(path)
				else:
					os.remove(path)
		finally:
			try:
				fstool.umount(mpoint=tmp_mpoint)
			except fstool.FstoolError:
				pass
			try:
				os.removedirs(tmp_mpoint)
			except OSError:
				pass
		
	logger.debug("Mounting %s to %s", privated_image, mpoint)
	fstool.mount(privated_image, mpoint, mnt_opts)