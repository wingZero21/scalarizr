
class BusEntries:
	BASE_PATH = "base_path"
	"""
	@cvar string: Application base path
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


import os.path
import sys
import sqlite3 as sqlite
import sqlalchemy.pool as pool
from ConfigParser import ConfigParser
import logging
import logging.config
from scalarizr.core.behaviour import get_behaviour_ini_name

	
def _init():
	bus = Bus()
	bus[BusEntries.BASE_PATH] = os.path.realpath(os.path.dirname(__file__) + "/../../..")
	
	# Configure logging
	logging.config.fileConfig(bus[BusEntries.BASE_PATH] + "/etc/logging.ini")
	logger = logging.getLogger(__name__)
	logger.debug("Initialize scalarizr...")
	
	# Load configuration
	config = ConfigParser()
	config.read(bus[BusEntries.BASE_PATH] + "/etc/config.ini")
	bus[BusEntries.CONFIG] = config

	# Inject behaviour configurations into global config
	behaviour = config.get("default", "behaviour").split(",")
	for bh in behaviour:
		filename = "%s/etc/include/%s" % (bus[BusEntries.BASE_PATH], get_behaviour_ini_name(bh))
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
	bus = Bus()
	file = bus[BusEntries.BASE_PATH] + "/" + bus[BusEntries.CONFIG].get("default", "storage_path")

	logger = logging.getLogger(__name__)
	logger.debug("Open SQLite database (file: %s)" % (file))
	
	conn = sqlite.Connection(file)
	conn.row_factory = sqlite.Row
	return conn
	
def init_services():
	logger = logging.getLogger(__name__)
	bus = Bus()
	config = bus[BusEntries.CONFIG]
	
	logger.debug("Initialize services...")
	
	# Initialize platform
	logger.debug("Initialize platform")
	from scalarizr.platform import PlatformFactory 
	pl_factory = PlatformFactory()
	bus[BusEntries.PLATFORM] = pl_factory.new_platform(config.get("default", "platform"))

	
	# Initialize QueryEnv
	logger.debug("Initialize QueryEnv client")
	from scalarizr.core.queryenv import QueryEnvService
	f = open(bus[BusEntries.BASE_PATH] + "/" + config.get("default", "crypto_key_path"))
	key = f.read().strip()
	f.close()
	queryenv = QueryEnvService(config.get("default", "queryenv_url"),
			config.get("default", "server_id"), key)
	bus[BusEntries.QUERYENV_SERVICE] = queryenv

	
	# Initialize messaging
	logger.debug("Initialize messaging")
	from scalarizr.messaging import MessageServiceFactory
	factory = MessageServiceFactory()
	try:
		service = factory.new_service(config.get("messaging", "adapter"), config.items("messaging"))
		bus[BusEntries.MESSAGE_SERVICE] = service
	except Exception, e:
		logger.exception(e)
		sys.exit("Cannot create messaging service adapter '%s'" % (config.get("messaging", "adapter")))
		
	# Initialize handlers
	from scalarizr.core.handlers import MessageListener	
	consumer = service.get_consumer()
	consumer.add_message_listener(MessageListener())	

	bus.fire("init")
	
def init_scripts():
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
	

def main():
	logger = logging.getLogger(__name__)
	logger.info("Starting scalarizr...")
	
	
	# Run installation process
	if len(sys.argv) > 1 and sys.argv[1] == "--install":
		_install()
	
	# Initialize services
	init_services()

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
	
	
_init()
