
from scalarizr import behaviour
from scalarizr.bus import bus
from scalarizr.messaging import MessageServiceFactory, MessageService, MessageConsumer
from scalarizr.platform import PlatformFactory, UserDataOptions
from scalarizr.queryenv import QueryEnvService
from scalarizr.util import configtool, cryptotool, SqliteLocalObject, url_replace_hostname,\
	daemonize, system, disttool


import os
import sys
import re
import sqlite3 as sqlite
from ConfigParser import ConfigParser
import logging
import logging.config
from optparse import OptionParser, OptionGroup
import binascii
from scalarizr.messaging.p2p import P2pConfigOptions, P2pSender
from scalarizr.util.configtool import ConfigError
import threading
import urlparse
import socket
import signal
import shutil
import string
import traceback

try:
	import timemodule as time
except ImportError:
	import time


class ScalarizrError(BaseException):
	pass

class NotConfiguredError(BaseException):
	pass


__version__ = "0.5"	
EMBED_SNMPD = True
_running = False



def _init():
	optparser = bus.optparser
	bus.base_path = os.path.realpath(os.path.dirname(__file__) + "/../..")
	
	# Selected etc path and get configuration filename
	config_filename = None
	if optparser and optparser.values.conf_path:
		# Take config file from command-line options
		config_filename = os.path.abspath(optparser.values.conf_path)
	else:
		# Find configuration file among several places
		if not bus.etc_path:
			etc_places = (
				"/etc/scalr",
				"/etc/scalarizr", 
				"/usr/etc/scalarizr", 
				"/usr/local/etc/scalarizr",
				os.path.join(bus.base_path, "etc-devel"),
				os.path.join(bus.base_path, "etc")
			)
		else:
			etc_places = (bus.etc_path,)	
			
		# Find configuration file 
		for etc_path in etc_places:
			config_filename = os.path.join(etc_path, "config.ini")
			if os.path.exists(config_filename) and os.path.isfile(config_filename):
				break
		
		if config_filename is None:
			# File not found
			raise ScalarizrError("Cannot find scalarizr configuration file. " + 
					"Search amoung the list %s returned no results" % (":".join(etc_places)))

	if not os.path.exists(config_filename):
		raise ScalarizrError("Configuration file '%s' doesn't exists" % (config_filename))
	bus.etc_path = os.path.dirname(config_filename)

	# Load configuration
	config = ConfigParser()
	config.read(config_filename)
	bus.config = config

	# Configure database connection pool
	bus.db = SqliteLocalObject(_db_connect)

	
	# Configure logging
	if sys.version_info < (2,6):
		# Fix logging handler resolve
		from scalarizr.util.log import fix_python25_handler_resolve		
		fix_python25_handler_resolve()
	
	logging.config.fileConfig(os.path.join(bus.etc_path, "logging.ini"))
	logger = logging.getLogger(__name__)
	logger.info("Initialize scalarizr...")


	# Inject behaviour configurations into global config
	bhs = config.get(configtool.SECT_GENERAL, configtool.OPT_BEHAVIOUR)
	for behaviour in configtool.split_array(bhs):
		behaviour = behaviour.strip()
		for filename in configtool.get_behaviour_filename(behaviour, ret=configtool.RET_BOTH):
			if os.path.exists(filename):
				logger.debug("Read behaviour configuration file %s", filename)
				config.read(filename)
	

	
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
	
	logger.debug("Initialize services...")
	
	gen_sect = configtool.section_wrapper(config, configtool.SECT_GENERAL)
	messaging_sect = configtool.section_wrapper(config, configtool.SECT_MESSAGING)
	
	# Check that database exists (after rebundle for example)
	db_file = os.path.join(bus.etc_path, gen_sect.get(configtool.OPT_STORAGE_PATH))
	if not os.path.exists(db_file):
		db_script_file = os.path.join(bus.etc_path, "public.d/db.sql")
		logger.warning("Database doesn't exists, create new one from script '%s'", db_script_file)
		db = bus.db
		conn = db.get().get_connection()
		conn.executescript(open(db_script_file).read())
		conn.commit()		
	
	# Initialize platform
	logger.debug("Initialize platform")
	pl_name = gen_sect.get(configtool.OPT_PLATFORM)
	if pl_name:
		for filename in configtool.get_platform_filename(pl_name, ret=configtool.RET_BOTH):
			if os.path.exists(filename):
				logger.debug("Read platform configuration file %s", filename)
				config.read(filename)		
		pl_factory = PlatformFactory()
		bus.platform = pl_factory.new_platform(pl_name)
	else:
		raise NotConfiguredError("Platform not defined")

	platform = bus.platform
	optparser = bus.optparser
	
	# Set server id
	server_id_opt = gen_sect.option_wrapper(configtool.OPT_SERVER_ID)
	server_id_opt.set_required(optparser.values.server_id \
			or platform.get_user_data(UserDataOptions.SERVER_ID), 
			NotConfiguredError)
	
	# Set role name
	role_name_opt = gen_sect.option_wrapper(configtool.OPT_ROLE_NAME)
	role_name_opt.set_required(optparser.values.role_name \
			or platform.get_user_data(UserDataOptions.ROLE_NAME), 
			NotConfiguredError)

	# Set queryenv url
	query_env_opt = gen_sect.option_wrapper(configtool.OPT_QUERYENV_URL)
	query_env_opt.set_required(optparser.values.queryenv_url \
			or platform.get_user_data(UserDataOptions.QUERYENV_URL), 
			NotConfiguredError)

	# Set messaging producer url
	msg_p2p_producer_url_opt = configtool.option_wrapper(config, "messaging_p2p", 
			P2pConfigOptions.PRODUCER_URL)
	msg_p2p_producer_url_opt.set_required(optparser.values.msg_p2p_producer_url \
			or platform.get_user_data(UserDataOptions.MESSAGE_SERVER_URL), 
			NotConfiguredError)
	
	# Set crypto key
	crypto_key_title = "Scalarizr crypto key"
	crypto_key_opt = gen_sect.option_wrapper(configtool.OPT_CRYPTO_KEY_PATH)

	if not os.path.exists(os.path.join(bus.etc_path, ".hostinit")):
		# Override crypto key if server was'nt already initialized
		crypto_key = optparser.values.crypto_key or platform.get_user_data(UserDataOptions.CRYPTO_KEY)
		if crypto_key:
			configtool.write_key(crypto_key_opt.get(), crypto_key, key_title=crypto_key_title)
	try:
		crypto_key = binascii.a2b_base64(configtool.read_key(crypto_key_opt.get(), key_title=crypto_key_title))
	except ConfigError, e:
		logger.warn(str(e))
	if not crypto_key:
		raise NotConfiguredError("%s is empty" % (crypto_key_title))

	
	# Initialize QueryEnv
	logger.debug("Initialize QueryEnv client")
	queryenv = QueryEnvService(query_env_opt.get(), server_id_opt.get(), crypto_key)
	bus.queryenv_service = queryenv

	
	# Initialize messaging
	logger.debug("Initialize messaging")
	factory = MessageServiceFactory()
	adapter_name = messaging_sect.get(configtool.OPT_ADAPTER)
	try:
		kwargs = dict(config.items("messaging_" + adapter_name))
		kwargs[P2pConfigOptions.SERVER_ID] = gen_sect.get(configtool.OPT_SERVER_ID)
		kwargs[P2pConfigOptions.CRYPTO_KEY_PATH] = gen_sect.get(configtool.OPT_CRYPTO_KEY_PATH)
		kwargs[P2pConfigOptions.PRODUCER_SENDER] = P2pSender.DAEMON
		r = urlparse.urlparse(kwargs[P2pConfigOptions.CONSUMER_URL])
		if r.hostname == "localhost":
			# Replace localhost with public dns name in endpoint url
			kwargs[P2pConfigOptions.CONSUMER_URL] = url_replace_hostname(r, socket.gethostname())
		
		service = factory.new_service(adapter_name, **kwargs)
		bus.messaging_service = service
	except (BaseException, Exception):
		logger.error("Cannot create messaging service adapter '%s'" % (adapter_name))
		raise
		
		
	# Initialize SNMP server
	snmp_sect = configtool.section_wrapper(config, configtool.SECT_SNMP)	
	if EMBED_SNMPD:
		logger.debug("Initialize embed SNMP server")
		from scalarizr.snmp.agent import SnmpServer
		bus.snmp_server = SnmpServer(
			port=int(snmp_sect.get(configtool.OPT_PORT)),
			security_name=snmp_sect.get(configtool.OPT_SECURITY_NAME),
			community_name=platform.get_user_data(UserDataOptions.FARM_HASH) \
					or snmp_sect.get(configtool.OPT_COMMUNITY_NAME)  
		)
	else:
		logger.debug("Initialize snmpd")
		snmpd_conf = "/etc/snmp/snmpd.conf"
		if not os.path.exists(snmpd_conf):
			raise ScalarizrError("File %s doesn't exists. snmpd is not installed" % (snmpd_conf,))
		
		if not os.path.exists(snmpd_conf + ".orig"):
			shutil.copy(snmpd_conf, snmpd_conf + ".orig")
		else:
			shutil.copy(snmpd_conf, snmpd_conf + ".bak")
			
		inp = open(snmpd_conf, "r")
		lines = inp.readlines()
		inp.close()
		
		out = open(snmpd_conf, "w")
		ucdDiskIOMIB = ".1.3.6.1.4.1.2021.13.15"
		ucdDiskIOMIB_included = False		
		for line in lines:
			if re.match("^(com2sec.+)", line):
				# Modify community name
				community_name = platform.get_user_data(UserDataOptions.FARM_HASH) \
						or snmp_sect.get(configtool.OPT_COMMUNITY_NAME)
				line = line.replace(line.split()[3], community_name)
				
			elif re.match("^view\\s+systemview\\s+included", line):
				if line.split()[3] == ucdDiskIOMIB:
					ucdDiskIOMIB_included = True
				
			out.write(line)
			
		if not ucdDiskIOMIB_included:
			out.write("view systemview included " + ucdDiskIOMIB)
			
		out.close()
		
		# Add UCD-DISKIO-MIB
			
		
	# Initialize handlers
	from scalarizr.handlers import MessageListener
	consumer = service.get_consumer()
	consumer.add_message_listener(MessageListener())

	bus.fire("init")
	
def init_script():
	_init()
	
	config = bus.config
	logger = logging.getLogger(__name__)
	logger.debug("Initialize messaging")

	# Script producer url is scalarizr consumer url. 
	# Script can't handle any messages by himself. Leave consumer url blank
	adapter = config.get(configtool.SECT_MESSAGING, configtool.OPT_ADAPTER)	
	kwargs = dict(config.items("messaging_" + adapter))
	kwargs[P2pConfigOptions.SERVER_ID] = config.get(configtool.SECT_GENERAL, configtool.OPT_SERVER_ID)
	kwargs[P2pConfigOptions.CRYPTO_KEY_PATH] = config.get(configtool.SECT_GENERAL, configtool.OPT_CRYPTO_KEY_PATH)
	kwargs[P2pConfigOptions.PRODUCER_SENDER] = P2pSender.SCRIPT
	kwargs[P2pConfigOptions.PRODUCER_URL] = kwargs[P2pConfigOptions.CONSUMER_URL]
	r = urlparse.urlparse(kwargs[P2pConfigOptions.PRODUCER_URL])
	if r.hostname == "localhost":
		# Replace localhost with public dns name in endpoint url
		kwargs[P2pConfigOptions.PRODUCER_URL] = url_replace_hostname(r, socket.gethostname())
	
	factory = MessageServiceFactory()		
	msg_service = factory.new_service(adapter, **kwargs)
	bus.messaging_service = msg_service

def _configure_option(optparser, cli_opt_name, opt_title, opt_wrapper, ini_updates, 
		validator=None, allow_empty=False):
	orig_value = opt_wrapper.get()
	while True:
		input = optparser.values.__dict__[cli_opt_name] \
				or (raw_input("Enter " + opt_title + (" ["+orig_value+"]" if orig_value else "") + ":") 
						if not optparser.values.no_prompt else "")
		if input or allow_empty:
			if validator and not validator(input):
				continue
			if not opt_wrapper.section in ini_updates:
				ini_updates[opt_wrapper.section] = dict()
			ini_updates[opt_wrapper.section][opt_wrapper.option] = input
		if input or orig_value or allow_empty:
			break
		elif optparser.values.no_prompt:
			# In automated mode raise error
			raise ScalarizrError("Option '%s' is missed" % (cli_opt_name))

def _configure ():
	print "Configuring scalarizr..."
	
	optparser = bus.optparser
	config = bus.config
	gen_sect = configtool.section_wrapper(config, configtool.SECT_GENERAL)
	messaging_sect = configtool.section_wrapper(config, configtool.SECT_MESSAGING)
	ini_updates = dict()

	# Crypto key
	crypto_key_path_opt = configtool.option_wrapper(gen_sect, configtool.OPT_CRYPTO_KEY_PATH)
	try:
		orig_crypto_key = configtool.read_key(crypto_key_path_opt.get())
	except ConfigError, e:
		orig_crypto_key = None
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
	_configure_option(optparser, "server_id", "server id", 
			configtool.option_wrapper(gen_sect, configtool.OPT_SERVER_ID), 
			ini_updates)
	
	# Role name
	_configure_option(optparser, "role_name", "role name",
			configtool.option_wrapper(gen_sect, configtool.OPT_ROLE_NAME),
			ini_updates)
	
	# QueryEnv 
	_configure_option(optparser, "queryenv_url", "QueryEnv server URL",
			configtool.option_wrapper(gen_sect, configtool.OPT_QUERYENV_URL), 
			ini_updates)
	
	# Message server url
	_configure_option(optparser, "msg_p2p_producer_url", "Messaging server URL", 
			configtool.option_wrapper(config, "messaging_p2p", P2pConfigOptions.PRODUCER_URL), 
			ini_updates)
	
	# Platform
	_configure_option(optparser, "platform", "platform", 
			configtool.option_wrapper(gen_sect, configtool.OPT_PLATFORM), 
			ini_updates, validator=_platform_validator)
	
	# Behaviour
	_configure_option(optparser, "behaviour", "behaviour", 
			configtool.option_wrapper(gen_sect, configtool.OPT_BEHAVIOUR), 
			ini_updates, validator=_behaviour_validator, allow_empty=True)
	
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
	
	# Configure database
	print "Create database"
	conn = _db_connect()
	conn.executescript(open(os.path.join(bus.etc_path, "public.d/db.sql")).read())
	conn.commit()
	
	if optparser.values.run_import:
		print "Starting import process..."
		print "Don't terminate Scalarizr until Scalr will create the new role"
	else:
		print "Done"


_KNOWN_PLATFORMS = ("ec2", "rs", "vps")
	

def _platform_validator(value):
	if not value in _KNOWN_PLATFORMS:
		print "invalid choice: '%s' (choose from %s)" % (value, ", ".join(_KNOWN_PLATFORMS))
		return False
	return True

_KNOWN_BEHAVIOURS = ("www", "app", "mysql")

def _behaviour_validator(value):
	if value:
		for bh in configtool.split_array(value):
			if bh not in _KNOWN_BEHAVIOURS:
				print "invalid choice: '%s' (choose from %s)" % (bh, ", ".join(_KNOWN_BEHAVIOURS))
				return False
	return True

_snmp_pid = None
def _start_snmp_server():
	# Start SNMP server in a separate process
	pid = os.fork()
	if pid == 0:
		snmp_server = bus.snmp_server
		snmp_server.start()
		sys.exit()
	else:
		_snmp_pid = pid	

def _snmp_crash_handler(signum, frame):
	if _running:
		_start_snmp_server()

def _snmpd_health_check():
	logger = logging.getLogger(__name__)
	while True:
		if not os.path.exists("/var/run/snmpd.pid"):
			logger.warning("snmpd is not running. trying to start it")
			out, err, retcode = system("/etc/init.d/snmpd start")
			if retcode > 0 or out.lower().find("failed") != -1:
				logger.error("Canot start snmpd. %s", out)
		time.sleep(60)

def _shutdown():
	_running = False
	logger = logging.getLogger(__name__)
	logger.info("Stopping scalarizr...")
	
	if _snmp_pid:
		try:
			logging.debug("Stopping SNMP subprocess")
			os.kill(_snmp_pid, signal.SIGTERM)
		except OSError, e:
			logger.error("Cannot send SIGTERM to SNMP subprocess (pid: %d). %s", _snmp_pid, e)
	
	msg_service = bus.messaging_service
	consumer = msg_service.get_consumer()
	consumer.stop()
	# Fire terminate
	bus.fire("terminate")
	logger.info("Stopped")	

def main():
	try:
		logger = logging.getLogger(__name__)
	except (BaseException, Exception), e:
		print >> sys.stderr, "error: Cannot initiate logging. %s" % (e)
		sys.exit(1)
			
	try:
		optparser = bus.optparser = OptionParser()
		optparser.add_option("-c", "--conf-path", dest="conf_path",
				help="Configuration path")
		optparser.add_option("-z", dest="daemonize", action="store_true", default=False,
				help="Daemonize process")
		optparser.add_option("-n", "--configure", dest="configure", action="store_true", default=False, 
				help="Run installation process")
		optparser.add_option("-k", "--gen-key", dest="gen_key", action="store_true", default=False,
				help="Generate crypto key")
		
		group = OptionGroup(optparser, "Installation and runtime override options")
		
		group.add_option("--no-prompt", dest="no_prompt", action="store_true", default=False,
				help="Do not prompt user during installation. Use only command line options")
		group.add_option("--import", dest="run_import", action="store_true", default=False, 
				help="Start import process after configuring Scalarizr")
		group.add_option("--server-id", dest="server_id", 
				help="Unique server identificator in Scalr envirounment")
		group.add_option("--role-name", dest="role_name",
				help="Server role name")
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
		group.add_option("--msg-producer-url", dest="msg_p2p_producer_url",
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
	
		_init()		
	
		if optparser.values.gen_key:
			print cryptotool.keygen()
			sys.exit()

		# Run installation process
		if optparser.values.configure:
			_configure()
			if not optparser.values.run_import:
				sys.exit()
		
		# Initialize scalarizr service
		try:
			_init_services()
		except NotConfiguredError, e:
			logger.error("Scalarizr is not properly configured. %s", e)
			print >> sys.stderr, "error: %s" % (e)
			print >> sys.stdout, "Execute instalation process first: 'scalarizr --configure'"
			sys.exit(1)
		
		# Daemonize process
		if optparser.values.daemonize:
			daemonize()

		if EMBED_SNMPD:
			# Start SNMP server in a separate process			
			signal.signal(signal.SIGCHLD, _snmp_crash_handler)
			_start_snmp_server()
		else:
			# Start snmpd health check thread
			t = threading.Thread(target=_snmpd_health_check)
			t.daemon = True
			t.start()
			
		# Install  signal handlers	
		signal.signal(signal.SIGTERM, _shutdown)

		# Start messaging server
		msg_service = bus.messaging_service
		consumer = msg_service.get_consumer()
		msg_thread = threading.Thread(target=consumer.start)
		msg_thread.start()
	

		# Fire start
		_running = True
		bus.fire("start")
	
		try:
			while True:
				msg_thread.join(0.5)
		except KeyboardInterrupt:
			_shutdown()
			
	except (BaseException, Exception), e:
		if not (isinstance(e, SystemExit) or isinstance(e, KeyboardInterrupt)):
			traceback.print_exc(file=sys.stderr)
			logger.exception(e)
			sys.exit(1)
