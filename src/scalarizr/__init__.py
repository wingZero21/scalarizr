
from scalarizr.bus import bus
from scalarizr.config import CmdLineIni, ScalarizrCnf, ScalarizrState,\
	ScalarizrOptions
from scalarizr.messaging import MessageServiceFactory, MessageService, MessageConsumer
from scalarizr.messaging.p2p import P2pConfigOptions, P2pSender
from scalarizr.platform import PlatformFactory, UserDataOptions
from scalarizr.queryenv import QueryEnvService
from scalarizr.util import configtool, SqliteLocalObject, url_replace_hostname,\
	daemonize, system, disttool, fstool, initd, firstmatched, log
from scalarizr.util.configtool import ConfigError


import os
import sys
import re
import sqlite3 as sqlite
from ConfigParser import ConfigParser
import logging
import logging.config
from optparse import OptionParser, OptionGroup
import binascii
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
NET_SNMPD = False

_running = False
""" @var _running: True when scalarizr is running """

_snmp_pid = None
""" @var _snmp_pid: Embed snmpd process pid"""

def _init():
	optparser = bus.optparser
	bus.base_path = os.path.realpath(os.path.dirname(__file__) + "/../..")
	
	# Initialize configuration
	if not bus.etc_path:
		etc_places = [
			"/etc/scalr",
			"/etc/scalarizr", 
			"/usr/etc/scalarizr", 
			"/usr/local/etc/scalarizr",
			os.path.join(bus.base_path, "etc-devel"),
			os.path.join(bus.base_path, "etc")
		]
		if optparser and optparser.values.etc_path:
			# Insert command-line passed etc_path into begining
			etc_places.index(optparser.values.etc_path, 0)
			
		bus.etc_path = firstmatched(lambda p: os.access(p, os.F_OK), etc_places)
		if not bus.etc_path:
			raise ScalarizrError('Cannot find scalarizr configuration dir')
	bus.cnf = cnf = ScalarizrCnf(bus.etc_path)

	
	# Configure logging
	if sys.version_info < (2,6):
		# Fix logging handler resolve for python 2.5
		from scalarizr.util.log import fix_py25_handler_resolving		
		fix_py25_handler_resolving()
	
	logging.config.fileConfig(os.path.join(bus.etc_path, "logging.ini"))
	logger = logging.getLogger(__name__)
	
	# During server import user must see all scalarizr activity in his terminal
	# Add console handler if it doesn't configured in logging.ini	
	if optparser and optparser.values.import_server:
		if not any(isinstance(hdlr, logging.StreamHandler) \
				and (hdlr.stream == sys.stdout or hdlr.stream == sys.stderr) 
				for hdlr in logger.handlers):
			hdlr = logging.StreamHandler(sys.stdout)
			hdlr.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s"))
			logger.addHandler(hdlr)

	# Registering in init.d
	initd.explore("scalarizr", "/etc/init.d/scalarizr", tcp_port=8013)

	# Configure database connection pool
	bus.db = SqliteLocalObject(_db_connect)

	
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


DB_NAME = 'db.sqlite'
DB_SCRIPT = 'db.sql'

def _db_connect():
	cnf = bus.cnf
	file = cnf.private_path(DB_NAME)

	logger = logging.getLogger(__name__)
	logger.debug("Open SQLite database (file: %s)" % (file))
	
	conn = sqlite.connect(file, 5.0)
	#sqlite.Connection(file)
	conn.row_factory = sqlite.Row
	return conn
	
def _create_db():
	cnf = bus.cnf
	conn = _db_connect()
	conn.executescript(open(cnf.public_path(DB_SCRIPT)).read())
	conn.commit()	
	
	
def _mount_private_d():
	configtool.mount_private_d(bus.etc_path + "/private.d", "/mnt/privated.img", 10000)

	
def _init_services():
	
	logger = logging.getLogger(__name__)
	config = bus.config
	cnf = bus.cnf
	
	logger.debug("Initialize services")
	

	
	gen_sect = configtool.section_wrapper(config, configtool.SECT_GENERAL)
	messaging_sect = configtool.section_wrapper(config, configtool.SECT_MESSAGING)
	
	# Check that database exists (after rebundle for example)
	db_file = cnf.private_path(DB_NAME)
	if not os.path.exists(db_file) or not os.stat(db_file).st_size:
		logger.debug("Database doesn't exists, create new one from script")
		_create_db()		

	# Initialize platform
	logger.debug("Initialize platform")
	pl_name = gen_sect.get(configtool.OPT_PLATFORM)
	if pl_name:
		bus.platform = PlatformFactory().new_platform(pl_name)
	else:
		raise NotConfiguredError("Platform not defined")
	platform = bus.platform

	
	if cnf.state == ScalarizrState.UNKNOWN and platform.get_user_data():
		# Apply configuration from user-data
		o = ScalarizrOptions
		cnf.reconfigure(
			values={
				o.server_id.name : platform.get_user_data(UserDataOptions.SERVER_ID),
				o.crypto_key.name : platform.get_user_data(UserDataOptions.CRYPTO_KEY),
				o.role_name.name : platform.get_user_data(UserDataOptions.ROLE_NAME),
				o.queryenv_url.name : platform.get_user_data(UserDataOptions.QUERYENV_URL),
				o.message_producer_url.name : platform.get_user_data(UserDataOptions.MESSAGE_SERVER_URL),
				o.snmp_community_name.name : platform.get_user_data(UserDataOptions.FARM_HASH)
			},
			silent=True,
			yesall=True
		)
		cnf.bootstrap(force_reload=True)
		
		# Validate configuration
		errors = dict()
		def on_error(o, e, errors=errors):
			errors.append(e)
			logger.error('[%s] %s', o.name, e)
		logger.debug('Validating configuration')
		cnf.validate(on_error)		
	
		cnf.state = ScalarizrState.BOOTSTRAPPING
	
	
	# Set server id
	server_id_opt = gen_sect.option_wrapper(configtool.OPT_SERVER_ID)
	server_id_opt.set_required(None, NotConfiguredError)
	
	# Set role name
	role_name_opt = gen_sect.option_wrapper(configtool.OPT_ROLE_NAME)
	role_name_opt.set_required(None, NotConfiguredError)

	# Set queryenv url
	query_env_opt = gen_sect.option_wrapper(configtool.OPT_QUERYENV_URL)
	query_env_opt.set_required(None, NotConfiguredError)

	# Set messaging producer url
	msg_p2p_producer_url_opt = configtool.option_wrapper(config, "messaging_p2p", 
			P2pConfigOptions.PRODUCER_URL)
	msg_p2p_producer_url_opt.set_required(None, NotConfiguredError)
	
	# Set crypto key
	crypto_key_title = "Scalarizr crypto key"
	crypto_key = None
	try:
		crypto_key = binascii.a2b_base64(cnf.read_key('default', title=crypto_key_title))
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
	if NET_SNMPD:
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
	cnf = bus.cnf
	cnf.bootstrap()
	
	logger = logging.getLogger(__name__)
	logger.debug("Initialize script messaging")

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



def _start_snmp_server():
	# Start SNMP server in a separate process
	pid = os.fork()
	if pid == 0:
		snmp_server = bus.snmp_server
		snmp_server.start()
		sys.exit()
	else:
		globals()["_snmp_pid"] = pid

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

def onSIGTERM(*args):
	logger = logging.getLogger(__name__)
	logger.debug("Received SIGTERM")
	_shutdown()

def onSIGCHILD(*args):
	logger = logging.getLogger(__name__)
	logger.debug("Received SIGCHILD from SNMP process")
	if globals()["_running"]:
		_start_snmp_server()


def _shutdown(*args):
	logger = logging.getLogger(__name__)
	if globals()["_running"]:
		logger.info("Stopping scalarizr...")
		try:		
			if EMBED_SNMPD and _snmp_pid:
				try:
					logging.debug("Stopping SNMP subprocess")
					os.kill(_snmp_pid, signal.SIGTERM)
				except OSError, e:
					logger.error("Cannot kill SIGTERM to SNMP subprocess (pid: %d). %s", _snmp_pid, e)
			
			msg_service = bus.messaging_service
			consumer = msg_service.get_consumer()
			consumer.stop()
			consumer.shutdown()
			
			producer = msg_service.get_producer()
			producer.shutdown()
			
			snmp_server = bus.snmp_server
			snmp_server.stop()
			
			# Fire terminate
			bus.fire("terminate")
			logger.info("Stopped")
		finally:
			globals()["_running"] = False
	else:
		logger.warning("Scalarizr is not running. Nothing to stop")	

def do_validate(silent=False):
	errors = list()
	def on_error(o, e, errors=errors):
		errors.append(e)
		if not silent:
			print >> sys.stderr, 'error: [%s] %s' % (o.name, e)
		
	if not silent:
		print 'Validating scalarizr configuration'
	cnf = bus.cnf
	cnf.bootstrap()
	cnf.validate(on_error)
	return len(errors)

def do_configure():
	optparser = bus.optparser
	cnf = bus.cnf
	cnf.reconfigure(
		values=CmdLineIni.to_kvals(optparser.values.cnf), 
		silent=optparser.values.import_server, 
		yesall=optparser.values.yesall
	)


def do_keygen():
	from scalarizr.util import cryptotool
	print cryptotool.keygen()	


def main():
	try:
		logger = logging.getLogger(__name__)
	except (BaseException, Exception), e:
		print >> sys.stderr, "error: Cannot initiate logging. %s" % (e)
		sys.exit(1)
			
	try:
		optparser = bus.optparser = OptionParser()
		optparser.add_option('-v', '--version', dest='version', action='store_true',
				help='Show version information')
		optparser.add_option('-c', '--etc-path', dest='etc_path',
				help='Configuration directory path')
		optparser.add_option("-z", dest="daemonize", action="store_true", default=False,
				help='Daemonize process')
		optparser.add_option('-n', '--configure', dest='configure', action="store_true", default=False, 
				help="Configure Scalarizr in the interactive mode by default. " 
				+ "Use '-y -o' to configure Scalarizr non-interactively")
		optparser.add_option("-k", "--gen-key", dest="gen_key", action="store_true", default=False,
				help="Generate crypto key")
		optparser.add_option('-t', dest='validate_cnf', action='store_true', default=False,
				help='Validate configuration')
		optparser.add_option('-m', '--import', dest="import_server", action="store_true", default=False, 
				help="Import service into Scalr")
		optparser.add_option('-y', dest="yesall", action="store_true", default=False,
				help='Answer "yes" to all questions')
		optparser.add_option('-o', dest='cnf', action='append',
				help='Runtime .ini option key=value')
		

		optparser.parse_args()

		
		# Daemonize process
		if optparser.values.daemonize:
			daemonize()
	
		logger.info("Initialize Scalarizr...")
		_init()
		cnf = bus.cnf
	
		if optparser.values.version:
			print 'Scalarizr %s' % __version__
			sys.exit()
		if optparser.values.gen_key:
			do_keygen()
			sys.exit()
		elif optparser.values.validate_cnf:
			num_errors = do_validate()
			sys.exit(int(not num_errors or 1))

		_mount_private_d()

		if optparser.values.configure:
			do_configure()
			sys.exit()
		elif optparser.values.import_server:
			print "Starting import process..."
			print "Don't terminate Scalarizr until Scalr will create the new role"
			do_configure()
			cnf.state = ScalarizrState.IMPORTING
		
		cnf.bootstrap(CmdLineIni.to_ini_sections(optparser.values.cnf))
		
		# Initialize scalarizr service
		try:
			_init_services()
		except NotConfiguredError, e:
			logger.error("Scalarizr is not properly configured. %s", e)
			print >> sys.stderr, "error: %s" % (e)
			print >> sys.stdout, "Execute instalation process first: 'scalarizr --configure'"
			sys.exit(1)
		

		if EMBED_SNMPD:
			# Start SNMP server in a separate process			
			#signal.signal(signal.SIGCHLD, onSIGCHILD)
			#_start_snmp_server()
			pass
		elif NET_SNMPD:
			# Start snmpd health check thread
			t = threading.Thread(target=_snmpd_health_check)
			t.setDaemon(True)
			t.start()
			
		# Install  signal handlers	
		signal.signal(signal.SIGTERM, onSIGTERM)

		# Start messaging server
		msg_service = bus.messaging_service
		consumer = msg_service.get_consumer()
		msg_thread = threading.Thread(target=consumer.start, name="Message consumer")
		snmp_thread = threading.Thread(target=bus.snmp_server.start, name="SNMP server")
		logger.info('Starting Scalarizr')
		msg_thread.start()
		snmp_thread.start()

		# Fire start
		globals()["_running"] = True
		bus.fire("start")
	
		try:
			while _running:
				msg_thread.join(0.2)
				#if not msg_thread.isAlive():
				#	raise ScalarizrError("%s thread unexpectedly terminated" % msg_thread.name)
		except KeyboardInterrupt:
			pass
		finally:
			if _running:
				_shutdown()
			
	except (BaseException, Exception), e:
		if not (isinstance(e, SystemExit) or isinstance(e, KeyboardInterrupt)):
			traceback.print_exc(file=sys.stderr)
			logger.exception(e)
			sys.exit(1)
