
class BusEntries:
	BASE_PATH = "base_path"
	"""
	@cvar string: Application base path
	"""
	
	ETC_PATH = "etc_path"
	"""
	@cvar string: Application etc path 
	"""
	
	CONFIG = "config"
	"""
	@cvar ConfigParser.RawConfigParser: Scalarizr configuration 
	"""
	
	DB = "db"
	"""
	@cvar sqlalchemy.pool.SingletonThreadPool: Database connection pool
	"""
	
	MESSAGE_SERVICE = "message_service"
	"""
	@cvar scalarizr.messaging.MessageService: Default message service 
	"""
	
	QUERYENV_SERVICE = "queryenv_service"
	"""
	@cvar: QueryEnv service client
	"""
	
	PLATFORM = "platform"
	"""
	@cvar scalarizr.platform.Platform: Platform (ec2, rs, vps...)
	"""

from scalarizr.util import Observable, CryptoTool
class _Bus(Observable):
	_registry = {}
	
	def __init__(self):
		Observable.__init__(self)
	
	def __setitem__(self, name, value):
		self._registry[name] = value
	
	def __getitem__(self, name):
		return self._registry[name]
	
	
# Bus singleton 	
_bus = None
def Bus ():
	global _bus
	if (_bus is None):
		_bus = _Bus()
	return _bus


class ScalarizrError(BaseException):
	pass

import os.path
import sys
import sqlite3 as sqlite
import sqlalchemy.pool as pool
from ConfigParser import ConfigParser
from scalarizr.util import configtool
import logging

	
def _init():
	bus = Bus()
	bus[BusEntries.BASE_PATH] = os.path.realpath(os.path.dirname(__file__) + "/../../..")
	
	# Find scalarizr config file
	bus[BusEntries.ETC_PATH] = None
	etc_places = ("/etc/scalr", "/usr/local/etc/scalr", os.path.join(bus[BusEntries.BASE_PATH], "etc"))
	for etc_path in etc_places:
		config_filename = os.path.join(etc_path, "config.ini")
		if os.path.exists(config_filename) and os.path.isfile(config_filename):
			bus[BusEntries.ETC_PATH] = etc_path
	if bus[BusEntries.ETC_PATH] is None:
		raise ScalarizrError("Cannot find scalarizr `etc` path. " + 
				"Search amoung the list %s returned no results" % (":".join(etc_places)))
	
	# Configure logging
	logging.config.fileConfig(bus[BusEntries.ETC_PATH] + "logging.ini")
	logger = logging.getLogger(__name__)
	logger.debug("Initialize scalarizr...")
	
	# Load configuration
	config = ConfigParser()
	config.read(os.path.join(bus[BusEntries.ETC_PATH], "config.ini"))
	bus[BusEntries.CONFIG] = config

	# Inject behaviour configurations into global config
	for behaviour in config.get(configtool.SECT_GENERAL, configtool.OPT_BEHAVIOUR).split(","):
		for filename in configtool.get_behaviour_filename(behaviour, ret=configtool.RET_BOTH):
			if os.path.exists(filename):
				logger.debug("Read behaviour configuration file %s", filename)
				config.read(filename)
	
	# Configure database connection pool
	bus[BusEntries.DB] = pool.SingletonThreadPool(_db_connect)
	
	# Define scalarizr events
	bus.define_events(
		# Fires before scalarizr start 
		# (can be used by handers to subscribe events, published by other handlers)
		"init",
		
		# Fires when scalarizr is starting
		"start",
		
		# Fires when scalarizr is terminating
		"terminate"
	)	


def _db_connect():
	etc_path = Bus()[BusEntries.ETC_PATH]
	config = Bus()[BusEntries.CONFIG]
	file = os.path.join(etc_path, config.get(configtool.SECT_GENERAL, configtool.OPT_STORAGE_PATH))
	
	logger = logging.getLogger(__name__)
	logger.debug("Open SQLite database (file: %s)" % (file))
	
	conn = sqlite.Connection(file)
	conn.row_factory = sqlite.Row
	return conn
	
def init_service():
	logger = logging.getLogger(__name__)
	bus = Bus()
	config = bus[BusEntries.CONFIG]
	
	logger.debug("Initialize services...")
	
	# Initialize platform
	logger.debug("Initialize platform")
	from scalarizr.platform import PlatformFactory 
	pl_factory = PlatformFactory()
	bus[BusEntries.PLATFORM] = pl_factory.new_platform(
			config.get(configtool.SECT_GENERAL, configtool.OPT_PLATFORM))

	
	# Initialize QueryEnv
	logger.debug("Initialize QueryEnv client")
	from scalarizr.core.queryenv import QueryEnvService
	crypto_key = configtool.read_key(config.get(configtool.SECT_GENERAL, configtool.OPT_CRYPTO_KEY_PATH), 
			"Scalarizr crypto key")
	queryenv = QueryEnvService(
			config.get(configtool.SECT_GENERAL, configtool.OPT_QUERYENV_URL),
			config.get(configtool.SECT_GENERAL, configtool.OPT_SERVER_ID),
			crypto_key)
	bus[BusEntries.QUERYENV_SERVICE] = queryenv

	
	# Initialize messaging
	logger.debug("Initialize messaging")
	from scalarizr.messaging import MessageServiceFactory
	factory = MessageServiceFactory()
	adapter_name = config.get(configtool.SECT_MESSAGING, configtool.OPT_ADAPTER)
	try:
		service = factory.new_service(adapter_name, config.items(configtool.SECT_MESSAGING))
		bus[BusEntries.MESSAGE_SERVICE] = service
	except (BaseException, Exception):
		logger.error("Cannot create messaging service adapter '%s'" % (adapter_name))
		raise
		
	# Initialize handlers
	from scalarizr.core.handlers import MessageListener	
	consumer = service.get_consumer()
	consumer.add_message_listener(MessageListener())	

	bus.fire("init")
	
def init_script():
	logger = logging.getLogger(__name__)
	bus = Bus()
	config = bus[BusEntries.CONFIG]
	
	adapter = config.get("messaging", "adapter")
	
	# Make producer config from consumer
	logger.debug("Initialize messaging")
	producer_config = []
	for key, value in config.items("messaging"):
		if key.startswith(adapter + "_consumer"):
			producer_config.append((key.replace("consumer", "producer"), value))

	from scalarizr.messaging import MessageServiceFactory
	factory = MessageServiceFactory()
	msg_service = factory.new_service(adapter, producer_config)
	bus[BusEntries.MESSAGE_SERVICE] = msg_service

def _install (argv=None):
	# FIXME: rewrite _install
	"""
	if argv is None:
		argv = sys.argv
		
	global config, logger, base_path
	logger.info("Running installation process")
		
	for pair in argv[2:]:
		pair = pair.split("=", 1)
		if pair[0].startswith("--"):
			key = pair[0][2:]
			value = pair[1] if len(pair) > 1 else None

			section_option = key.split(".")
			section = section_option[0] if len(section_option) > 1 else "default"
			option = section_option[1] if len(section_option) > 1 else section_option[0]
			if config.has_option(section, option):
				config.set(section, option, value)
			elif section == "default" and option == "crypto_key":
				# Update crypto key
				f = open(base_path + "/" + config.get("default", "crypto_key_path"), "w+")
				f.write(value)
				f.close()
				
	# Save configuration
	filename = Bus()[BusEntries.BASE_PATH] + "/etc/config.ini"
	logger.debug("Save configuration into '%s'" % filename)
	f = open(filename, "w")
	config.write(f)
	f.close()
	"""
	pass
	

def main():
	logger = logging.getLogger(__name__)
	logger.info("Starting scalarizr...")
	
	
	# Run installation process
	if len(sys.argv) > 1 and sys.argv[1] == "--install":
		_install()
	
	# Initialize scalarizr service
	init_service()

	# Fire start
	bus = Bus()
	bus.fire("start")

	# @todo start messaging before fire 'start'
	# Start messaging server
	try:
		consumer = bus[BusEntries.MESSAGE_SERVICE].get_consumer()
		consumer.start()
	except KeyboardInterrupt:
		logger.info("Stopping scalarizr...")
		consumer.stop()
		
		# Fire terminate
		bus.fire("terminate")
		logger.info("Stopped")
	
	
try:
	_init()
except (Exception, BaseException), e:
	print >> sys.stderr, "error: %s" % (e)
	sys.exit(1)