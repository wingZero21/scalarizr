
from scalarizr.bus import bus
from scalarizr.config import CmdLineIni, ScalarizrCnf, ScalarizrState, ScalarizrOptions
from scalarizr.handlers import MessageListener
from scalarizr.messaging import MessageServiceFactory, MessageService, MessageConsumer
from scalarizr.messaging.p2p import P2pConfigOptions
from scalarizr.platform import PlatformFactory, UserDataOptions
from scalarizr.queryenv import QueryEnvService
from scalarizr.util import SqliteLocalObject, daemonize, system, disttool, fstool, initdv2, firstmatched,\
	filetool, format_size
from scalarizr.snmp.agent import SnmpServer
from scalarizr.util import configtool

import os, sys, re, shutil
import sqlite3 as sqlite
import logging
import logging.config
from ConfigParser import ConfigParser
from optparse import OptionParser, OptionGroup
import binascii, string, traceback
import threading, socket, signal
import urlparse


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

class ScalarizrInitScript(initdv2.ParametrizedInitScript):
	def __init__(self):
		initdv2.ParametrizedInitScript.__init__(self, 'scalarizr', "/etc/init.d/scalarizr", socks=[initdv2.SockParam(8013)])

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
	bus.cnf = ScalarizrCnf(bus.etc_path)

	
	# Configure logging
	if sys.version_info < (2,6):
		# Fix logging handler resolve for python 2.5
		from scalarizr.util.log import fix_python25_handler_resolve		
		fix_python25_handler_resolve()
	
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
	initdv2.explore("scalarizr", ScalarizrInitScript)

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
	logger = logging.getLogger(__name__)	
	cnf = bus.cnf
	file = cnf.private_path(DB_NAME)

	logger.debug("Open SQLite database (file: %s)" % (file))
	
	conn = sqlite.connect(file, 5.0)
	#sqlite.Connection(file)
	conn.row_factory = sqlite.Row
	return conn

def _init_db():
	logger = logging.getLogger(__name__)	
	cnf = bus.cnf

	# Check that database exists (after rebundle for example)	
	db_file = cnf.private_path(DB_NAME)
	if not os.path.exists(db_file) or not os.stat(db_file).st_size:
		logger.debug("Database doesn't exists, create new one from script")
		_create_db()
	
def _create_db():
	cnf = bus.cnf
	conn = _db_connect()
	conn.executescript(open(cnf.public_path(DB_SCRIPT)).read())
	conn.commit()	
	
	
def _mount_private_d(mpoint, privated_image, blocks_count):
	logger = logging.getLogger(__name__)
	
	logger.debug("Move private.d configuration %s to mounted filesystem (img: %s, size: %s)", 
			mpoint, privated_image, format_size(1024*blocks_count))
	mtab = fstool.Mtab()
	if mtab.contains(mpoint=mpoint): # if privated_image exists
		logger.debug("private.d already mounted to %s", mpoint)
		return
	
	if not os.path.exists(mpoint):
		os.makedirs(mpoint)
		
	mnt_opts = ('-t auto', '-o loop,rw')	
	if not os.path.exists(privated_image):	
		build_image_cmd = 'dd if=/dev/zero of=%s bs=1024 count=%s 2>&1' % (privated_image, blocks_count)
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
			system(str(filetool.Rsync().archive().source(mpoint+"/" if mpoint[-1] != "/" else mpoint).dest(tmp_mpoint)))
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


def _init_platform():
	logger = logging.getLogger(__name__)
	cnf = bus.cnf; ini = cnf.rawini
	
	# Initialize platform
	logger.debug("Initialize platform")
	name = ini.get('general', 'platform')
	if name:
		bus.platform = PlatformFactory().new_platform(name)
	else:
		raise ScalarizrError("Platform not defined")

def _init_services():
	logger = logging.getLogger(__name__)
	cnf = bus.cnf; ini = cnf.rawini

	server_id = ini.get('general', 'server_id')
	queryenv_url = ini.get('general', 'queryenv_url')
	crypto_key = cnf.read_key(cnf.DEFAULT_KEY)
	messaging_adp = ini.get('messaging', 'adapter')
	snmp_port = ini.get('snmp', 'port')
	snmp_security_name = ini.get('snmp', 'security_name')
	snmp_community_name = ini.get('snmp', 'community_name')
	

	logger.debug("Initialize QueryEnv client")
	queryenv = QueryEnvService(queryenv_url, server_id, crypto_key)
	bus.queryenv_service = queryenv

	
	logger.debug("Initialize messaging")
	factory = MessageServiceFactory()
	try:
		params = dict(ini.items("messaging_" + messaging_adp))
		params[P2pConfigOptions.SERVER_ID] = server_id
		params[P2pConfigOptions.CRYPTO_KEY_PATH] = cnf.key_path(cnf.DEFAULT_KEY)
		
		msg_service = factory.new_service(messaging_adp, **params)
		bus.messaging_service = msg_service
	except (BaseException, Exception):
		raise ScalarizrError("Cannot create messaging service adapter '%s'" % (messaging_adp))

	logger.debug('Initialize message handlers')
	consumer = msg_service.get_consumer()
	consumer.listeners.append(MessageListener())

	
	logger.debug('Initialize embed SNMP server')
	bus.snmp_server = SnmpServer(
		port=int(snmp_port), 
		security_name=snmp_security_name, 
		community_name=snmp_community_name
	)

	bus.fire("init")


def _apply_user_data(cnf):
	logger = logging.getLogger(__name__)
	platform = bus.platform
	g = platform.get_user_data
	
	logger.debug('Apply scalarizr user-data to configuration')
	cnf.update_ini('config.ini', dict(
		general={
			'server_id' : g(UserDataOptions.SERVER_ID),
			'role_name' : g(UserDataOptions.ROLE_NAME),
			'queryenv_url' : g(UserDataOptions.QUERYENV_URL),
		},
		messaging_p2p={
			'producer_url' : g(UserDataOptions.MESSAGE_SERVER_URL),
		},
		snmp={
			'community_name' : g(UserDataOptions.FARM_HASH)
		}
	))
	cnf.write_key(cnf.DEFAULT_KEY, g(UserDataOptions.CRYPTO_KEY))

	
def init_script():
	_init()
	
	config = bus.config
	cnf = bus.cnf
	cnf.bootstrap()
	
	logger = logging.getLogger(__name__)
	logger.debug("Initialize messaging")

	# Script producer url is scalarizr consumer url. 
	# Script can't handle any messages by himself. Leave consumer url blank
	adapter = config.get(configtool.SECT_MESSAGING, configtool.OPT_ADAPTER)	
	kwargs = dict(config.items("messaging_" + adapter))
	kwargs[P2pConfigOptions.SERVER_ID] = config.get(configtool.SECT_GENERAL, configtool.OPT_SERVER_ID)
	kwargs[P2pConfigOptions.CRYPTO_KEY_PATH] = config.get(configtool.SECT_GENERAL, configtool.OPT_CRYPTO_KEY_PATH)
	kwargs[P2pConfigOptions.PRODUCER_URL] = kwargs[P2pConfigOptions.CONSUMER_URL]
	
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
			
			# Kill Scalr message consumer
			msg_service = bus.messaging_service
			consumer = msg_service.get_consumer()
			consumer.stop()
			consumer.shutdown()
			
			# Kill Scalr message producer
			producer = msg_service.get_producer()
			producer.shutdown()
			
			# Kill Cross-scalarizr message consumer
			int_msg_service = bus.int_messaging_service
			if int_msg_service and int_msg_service.consumer:
				int_msg_service.consumer.stop()
				int_msg_service.consumer.shutdown()
			
			# Fire terminate
			bus.fire("terminate")
			logger.info("Stopped")
		finally:
			globals()["_running"] = False
	else:
		logger.warning("Scalarizr is not running. Nothing to stop")	

def do_validate_cnf():
	errors = list()
	def on_error(o, e, errors=errors):
		errors.append(e)
		print >> sys.stderr, 'error: [%s] %s' % (o.name, e)
		
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
				help='Generate crypto key')
		optparser.add_option('-t', dest='validate_cnf', action='store_true', default=False,
				help='Validate configuration')
		optparser.add_option('-m', '--import', dest="import_server", action="store_true", default=False, 
				help='Import service into Scalr')
		optparser.add_option('-y', dest="yesall", action="store_true", default=False,
				help='Answer "yes" to all questions')
		optparser.add_option('-o', dest='cnf', action='append',
				help='Runtime .ini option key=value')
		optparser.parse_args()

		
		# Daemonize process
		if optparser.values.daemonize:
			daemonize()
	
		logger.info('Initialize Scalarizr...')
		_init()
	
		if optparser.values.version:
			# Report scalarizr version
			print 'Scalarizr %s' % __version__
			sys.exit()
			
		elif optparser.values.gen_key:
			# Generate key-pair
			do_keygen()
			sys.exit()

		# Starting scalarizr daemon initialization
		cnf = bus.cnf
		cnf.on('apply_user_data', _apply_user_data)
		
		# Move private configuration to loop device
		privated_img_path = '/mnt/privated.img'
		if cnf.state == ScalarizrState.UNKNOWN and os.path.exists(privated_img_path):
			os.remove(privated_img_path)
		_mount_private_d(cnf.private_path(), privated_img_path, 10000)
		
		if optparser.values.configure:
			do_configure()
			sys.exit()
			
		elif optparser.values.import_server:
			print "Starting import process..."
			print "Don't terminate Scalarizr until Scalr will create the new role"
			cnf.state = ScalarizrState.IMPORTING
			# Load Command-line configuration options and auto-configure Scalarizr
			cnf.reconfigure(values=CmdLineIni.to_kvals(optparser.values.cnf), silent=True, yesall=True)
		
		# Load INI files configuration
		cnf.bootstrap(force_reload=True)
		
		# Initialize local database
		_init_db()
		
		# Initialize platform module
		_init_platform()
		
		# At first scalarizr startup platform user-data should be applied
		if cnf.state == ScalarizrState.UNKNOWN:
			cnf.state = ScalarizrState.BOOTSTRAPPING
			cnf.fire('apply_user_data', cnf)
			
		# Apply Command-line passed configuration options
		cnf.update(CmdLineIni.to_ini_sections(optparser.values.cnf))
		
		# Validate configuration
		num_errors = do_validate_cnf()
		if num_errors or optparser.values.validate_cnf:
			sys.exit(int(not num_errors or 1))		
		
		_init_services()
		_start_snmp_server()
		
		# Install  signal handlers	
		signal.signal(signal.SIGTERM, onSIGTERM)

		# Start messaging server
		msg_service = bus.messaging_service
		consumer = msg_service.get_consumer()
		msg_thread = threading.Thread(target=consumer.start, name="Message consumer")
		logger.info('Starting Scalarizr')
		msg_thread.start()

		# Fire start
		globals()["_running"] = True
		bus.fire("start")
	
		try:
			while _running:
				msg_thread.join(0.2)
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
