
from scalarizr import behaviour
from scalarizr.bus import bus
from scalarizr.messaging import MessageServiceFactory, MessageService, MessageConsumer
from scalarizr.platform import PlatformFactory, UserDataOptions
from scalarizr.queryenv import QueryEnvService
from scalarizr.util import configtool, cryptotool

import os
import sys
import sqlite3 as sqlite
import sqlalchemy.pool as pool
from ConfigParser import ConfigParser
import logging
import logging.config
from optparse import OptionParser, OptionGroup
import binascii


class ScalarizrError(BaseException):
	pass

class NotInstalledError(BaseException):
	pass
	
def _init():
	bus.base_path = os.path.realpath(os.path.dirname(__file__) + "/../..")

	# Find scalarizr config file
	etc_places = (
		"/etc/scalr", 
		"/usr/etc/scalr", 
		"/usr/local/etc/scalr",
		os.path.join(bus.base_path, "etc-devel"), 
		os.path.join(bus.base_path, "etc")
	)
	for etc_path in etc_places:
		config_filename = os.path.join(etc_path, "config.ini")
		if os.path.exists(config_filename) and os.path.isfile(config_filename):
			bus.etc_path = etc_path
	if bus.etc_path is None:
		raise ScalarizrError("Cannot find scalarizr `etc` path. " + 
				"Search amoung the list %s returned no results" % (":".join(etc_places)))
	
	# Configure logging
	logging.config.fileConfig(os.path.join(bus.etc_path, "logging.ini"))
	logger = logging.getLogger(__name__)
	logger.debug("Initialize scalarizr...")
	

	# Load configuration
	config = ConfigParser()
	config.read(os.path.join(bus.etc_path, "config.ini"))
	bus.config = config

	# Inject behaviour configurations into global config
	bhs = config.get(configtool.SECT_GENERAL, configtool.OPT_BEHAVIOUR)
	for behaviour in configtool.split_array(bhs):
		behaviour = behaviour.strip()
		for filename in configtool.get_behaviour_filename(behaviour, ret=configtool.RET_BOTH):
			if os.path.exists(filename):
				logger.debug("Read behaviour configuration file %s", filename)
				config.read(filename)
	
	# Configure database connection pool
	bus.db = pool.SingletonThreadPool(_db_connect)
	
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
	config = bus.config
	file = os.path.join(bus.etc_path, config.get(configtool.SECT_GENERAL, configtool.OPT_STORAGE_PATH))
	
	logger = logging.getLogger(__name__)
	logger.debug("Open SQLite database (file: %s)" % (file))
	
	conn = sqlite.Connection(file)
	conn.row_factory = sqlite.Row
	return conn
	
def _init_services():
	
	logger = logging.getLogger(__name__)
	config = bus.config
	
	logger.info("Initialize services...")
	
	gen_sect = configtool.section_wrapper(config, configtool.SECT_GENERAL)
	messaging_sect = configtool.section_wrapper(config, configtool.SECT_MESSAGING)
	
	# Initialize platform
	logger.debug("Initialize platform")
	pl_name = gen_sect.get(configtool.OPT_PLATFORM)
	if pl_name:
		pl_factory = PlatformFactory()
		bus.platfrom = pl_factory.new_platform(pl_name)
	else:
		raise NotInstalledError("Platform not defined")

	platform = bus.platfrom
	optparser = bus.optparser
	
	# Set server id
	server_id_opt = gen_sect.option_wrapper(configtool.OPT_SERVER_ID)
	server_id_opt.set_required(optparser.values.server_id \
			or platform.get_user_data(UserDataOptions.SERVER_ID), 
			NotInstalledError)

	# Set queryenv url
	query_env_opt = gen_sect.option_wrapper(configtool.OPT_QUERYENV_URL)
	query_env_opt.set_required(optparser.values.queryenv_url \
			or platform.get_user_data(UserDataOptions.QUERYENV_URL), 
			NotInstalledError)

	# Set mesaging url
	# FIXME: disclosure of implementation	
	message_server_opt = messaging_sect.option_wrapper("p2p_producer_endpoint")
	message_server_opt.set_required(optparser.values.message_server_url \
			or platform.get_user_data(UserDataOptions.MESSAGE_SERVER_URL),
			NotInstalledError)
	
	# Set crypto key
	crypto_key_title = "Scalarizr crypto key"
	crypto_key_opt = gen_sect.option_wrapper(configtool.OPT_CRYPTO_KEY_PATH)
	crypto_key = optparser.values.crypto_key or platform.get_user_data(UserDataOptions.CRYPTO_KEY)
	if crypto_key:
		configtool.write_key(crypto_key_opt.get(), crypto_key, key_title=crypto_key_title)
	crypto_key = configtool.read_key(crypto_key_opt.get(), key_title=crypto_key_title)
	if not crypto_key:
		raise NotInstalledError("%s is empty" % (crypto_key_title))

	
	# Initialize QueryEnv
	logger.debug("Initialize QueryEnv client")
	queryenv = QueryEnvService(query_env_opt.get(), server_id_opt.get(), crypto_key)
	bus.queryenv_service = queryenv

	
	# Initialize messaging
	logger.debug("Initialize messaging")
	factory = MessageServiceFactory()
	adapter_name = messaging_sect.get(configtool.OPT_ADAPTER)
	try:
		service = factory.new_service(adapter_name, config.items(configtool.SECT_MESSAGING))
		bus.messaging_service = service
	except (BaseException, Exception):
		logger.error("Cannot create messaging service adapter '%s'" % (adapter_name))
		raise
		
	# Initialize handlers
	from scalarizr.handlers import MessageListener
	consumer = service.get_consumer()
	consumer.add_message_listener(MessageListener())	

	bus.fire("init")
	
def init_script():
	_init()
	
	logger = logging.getLogger(__name__)
	config = bus.config
	
	adapter = config.get(configtool.SECT_MESSAGING, configtool.OPT_ADAPTER)
	
	# Make producer config from consumer
	logger.debug("Initialize messaging")
	producer_config = []
	for key, value in config.items(configtool.SECT_MESSAGING):
		# FIXME: disclosure of implementation		
		if key.startswith(adapter + "_consumer"):
			producer_config.append((key.replace("consumer", "producer"), value))

	factory = MessageServiceFactory()
	msg_service = factory.new_service(adapter, producer_config)
	bus.messaging_service = msg_service

def _install_option(optparser, cli_opt_name, opt_title, opt_wrapper, ini_updates, validator=None):
	orig_value = opt_wrapper.get()
	while True:
		input = optparser.values.__dict__[cli_opt_name] \
				or (raw_input("Enter " + opt_title + (" ["+orig_value+"]" if orig_value else "") + ":") 
						if not optparser.values.no_prompt else None)
		if input:
			if validator and not validator(input):
				continue
			if not opt_wrapper.section in ini_updates:
				ini_updates[opt_wrapper.section] = dict()
			ini_updates[opt_wrapper.section][opt_wrapper.option] = input
		if input or orig_value:
			break
		elif optparser.values.no_prompt:
			# In automated mode raise error
			raise ScalarizrError("Option '%s' is missed" % (cli_opt_name))

def _install ():
	optparser = bus.optparser
	config = bus.config
	gen_sect = configtool.section_wrapper(config, configtool.SECT_GENERAL)
	messaging_sect = configtool.section_wrapper(config, configtool.SECT_MESSAGING)
	ini_updates = dict()

	# Crypto key
	crypto_key_path_opt = configtool.option_wrapper(gen_sect, configtool.OPT_CRYPTO_KEY_PATH)
	orig_crypto_key = configtool.read_key(crypto_key_path_opt.get())
	while True:
		input = optparser.values.crypto_key \
				or (raw_input("Enter crypto key" + (" ["+orig_crypto_key+"]" if orig_crypto_key else "") + ":")
						if not optparser.values.no_prompt else None)
		if input:
			try:
				binascii.a2b_base64(input)
			except binascii.Error, e:
				if optparser.values.crypto_key:
					# In automated mode raise error
					raise ScalarizrError("Cannot decode crypto key")
				else:
					# In interactive mode notify user, and go to enter key again 
					print >> sys.stderr, "error: Cannot decode crypto key. %s" % (e)
					continue
			configtool.write_key(crypto_key_path_opt.get(), input)
		if input or orig_crypto_key:
			break
	
	# Server id
	_install_option(optparser, "server_id", "server id", 
			configtool.option_wrapper(gen_sect, configtool.OPT_SERVER_ID), 
			ini_updates)
	
	# QueryEnv 
	_install_option(optparser, "queryenv_url", "QueryEnv url",
			configtool.option_wrapper(gen_sect, configtool.OPT_QUERYENV_URL), 
			ini_updates)
	
	# Message server url
	_install_option(optparser, "message_server_url", "message server url", 
			configtool.option_wrapper(messaging_sect, "p2p_producer_endpoint"), 
			ini_updates)
	
	# Platform
	_install_option(optparser, "platform", "platform", 
			configtool.option_wrapper(gen_sect, configtool.OPT_PLATFORM), 
			ini_updates, validator=_platform_validator)
	
	# Behaviour
	_install_option(optparser, "behaviour", "behaviour", 
			configtool.option_wrapper(gen_sect, configtool.OPT_BEHAVIOUR), 
			ini_updates, validator=_behaviour_validator)
	
	try:
		bhs = ini_updates[configtool.SECT_GENERAL][configtool.OPT_BEHAVIOUR]
	except KeyError:
		bhs = config.get(configtool.SECT_GENERAL, configtool.OPT_BEHAVIOUR)

	for bh in configtool.split_array(bhs):
		configurator = behaviour.get_configurator(bh)
		print "Configure %s behaviour" % (bh)
		kwargs = {}
		for opt in configurator.cli_options:
			kwargs[opt.dest] = optparser.values.__dict__[bh + "_" + opt.dest]
		configurator.configure(not optparser.values.no_prompt, **kwargs)
		
	configtool.update(os.path.join(bus.etc_path, "config.ini"), ini_updates)
	
	print "Done"

_KNOWN_PLATFORMS = ("ec2", "rs", "vps")
	
def _platform_validator(value):
	if not value in _KNOWN_PLATFORMS:
		print "invalid choice: '%s' (choose from %s)" % (value, ", ".join(_KNOWN_PLATFORMS))
		return False
	return True

_KNOWN_BEHAVIOURS = ("www", "app", "mysql")

def _behaviour_validator(value):
	for bh in configtool.split_array(value):
		if bh not in _KNOWN_BEHAVIOURS:
			print "invalid choice: '%s' (choose from %s)" % (bh, ", ".join(_KNOWN_BEHAVIOURS))
			return False
	return True
		

def main():
	try:
		logger = logging.getLogger(__name__)
	except (BaseException, Exception), e:
		print >> sys.stderr, "error: Cannot initiate logging. %s" % (e)
		sys.exit(1)
			
	try:
		_init()		
		
		optparser = bus.optparser
		optparser.add_option("-n", "--install", dest="install", action="store_true", default=False, 
				help="Run installation process")
		optparser.add_option("--no-prompt", dest="no_prompt", action="store_true", default=False,
				help="Do not prompt user during installation. Use only command line options")
		optparser.add_option("-k", "--gen-key", dest="gen_key", action="store_true", default=False,
				help="Generate crypto key")
		
		group = OptionGroup(optparser, "Installation and runtime override options")
		group.add_option("--server-id", dest="server_id", 
				help="unique server identificator in Scalr envirounment")
		group.add_option("--crypto-key", dest="crypto_key",
				help="Scalarizr base64 encoded crypto key")
		group.add_option("--platform", dest="platform", choices=_KNOWN_PLATFORMS,
				help="Cloud platform (choises: %s)" % 
				(", ".join(_KNOWN_PLATFORMS)))
		group.add_option("--behaviour", dest="behaviour", 
				help="Server behaviour (choises: %s). You can combine multiple using comma" %
				(", ".join(_KNOWN_BEHAVIOURS)))
		group.add_option("--queryenv-url", dest="queryenv_url",
				help="URL to Scalr QueryEnv service (default: https://scalr.net/queryenv)")
		group.add_option("--message-server-url", dest="message_server_url",
				help="URL to Scalr messaging server (default: https://scalr.net/messaging)")
		optparser.add_option_group(group)
		
		# Add options from behaviour configurators
		for bh_attr in [bh for bh in dir(behaviour.Behaviours) if not bh.startswith("__")]:
			bh = getattr(behaviour.Behaviours, bh_attr)
			configurator = behaviour.get_configurator(bh)
			if configurator.cli_options:			
				group = OptionGroup(optparser, "Installation options for '%s' behaviour" % (bh))
				for opt in configurator.cli_options:
					opt.dest = bh + "_" + opt.dest
				group.add_options(configurator.cli_options)
				optparser.add_option_group(group)
			
		optparser.parse_args()
	
		if optparser.values.gen_key:
			print cryptotool.keygen()
			sys.exit()

		# Run installation process
		if optparser.values.install:
			_install()
			sys.exit()
		
		# Initialize scalarizr service
		try:
			_init_services()
		except NotInstalledError, e:
			logger.error("Scalarizr is not properly installed. %s", e)
			print >> sys.stderr, "error: %s" % (e)
			print >> sys.stdout, "Execute instalation process first: 'scalarizr --install'"
			sys.exit()
	
		# Fire start
		bus.fire("start")
	
		# TODO: find a way to start messaging before fire 'start'. maybe in a separate thread, 
		# but program termination on Ctrl-C must be preserved
		
		# Start messaging server
		try:
			msg_service = bus.messaging_service
			consumer = msg_service.get_consumer()
			consumer.start()
		except KeyboardInterrupt:
			logger.info("Stopping scalarizr...")
			consumer.stop()
			
			# Fire terminate
			bus.fire("terminate")
			logger.info("Stopped")
	except (BaseException, Exception), e:
		if not (isinstance(e, SystemExit) or isinstance(e, KeyboardInterrupt)):
			logger.exception(e)
			print >> sys.stderr, "error: %s" % (e)
