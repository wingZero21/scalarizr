
# Core
from scalarizr import config 
from scalarizr.bus import bus
from scalarizr.config import CmdLineIni, ScalarizrCnf, ScalarizrState, ScalarizrOptions
from scalarizr.handlers import MessageListener
from scalarizr.messaging import MessageServiceFactory, MessageService, MessageConsumer
from scalarizr.messaging.p2p import P2pConfigOptions
from scalarizr.platform import PlatformFactory, UserDataOptions
from scalarizr.queryenv import QueryEnvService
from scalarizr.storage import Storage


# Utils
from scalarizr.util import initdv2, fstool, filetool, log, PeriodicalExecutor
from scalarizr.util import SqliteLocalObject, daemonize, system2, disttool, firstmatched, format_size

# Stdlibs
import logging
import logging.config
import os, sys, re, shutil, time
import binascii, string, traceback
import sqlite3 as sqlite
import threading, socket, signal
from ConfigParser import ConfigParser
from optparse import OptionParser, OptionGroup
from urlparse import urlparse, urlunparse
from scalarizr.storage.util.loop import listloop
from scalarizr.util.filetool import write_file, read_file


class ScalarizrError(BaseException):
	pass

class NotConfiguredError(BaseException):
	pass


__version__ = "0.7.14"	

EMBED_SNMPD = True
NET_SNMPD = False

SNMP_RESTART_DELAY = 5 # Seconds

PID_FILE = '/var/run/scalarizr.pid' 

_running = False
'''
True when scalarizr daemon should be running
'''

_pid = None
'''
Scalarizr main process PID
'''

_snmp_pid = None
'''
Embed SNMP server process PID
'''

_snmp_scheduled_start_time = None
'''
Next time when SNMP process should be forked
'''

_logging_configured = False


class ScalarizrInitScript(initdv2.ParametrizedInitScript):
	def __init__(self):
		initdv2.ParametrizedInitScript.__init__(self, 
			'scalarizr', 
			'/etc/init.d/scalarizr', 
			socks=[initdv2.SockParam(8013)]
		)


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
			os.path.join(bus.base_path, 'etc')
		]
		if optparser and optparser.values.etc_path:
			# Insert command-line passed etc_path into begining
			etc_places.index(optparser.values.etc_path, 0)
			
		bus.etc_path = firstmatched(lambda p: os.access(p, os.F_OK), etc_places)
		if not bus.etc_path:
			raise ScalarizrError('Cannot find scalarizr configuration dir. Search path: %s' % ':'.join(etc_places))
	bus.cnf = ScalarizrCnf(bus.etc_path)
	
	# Find shared resources dir
	if not bus.share_path:
		share_places = [
			'/usr/share/scalr',
			'/usr/local/share/scalr',
			os.path.join(bus.base_path, 'share')
		]
		bus.share_path = firstmatched(lambda p: os.access(p, os.F_OK), share_places)
		if not bus.share_path:
			raise ScalarizrError('Cannot find scalarizr share dir. Search path: %s' % ':'.join(share_places))

	
	# Configure logging
	if sys.version_info < (2,6):
		# Fix logging handler resolve for python 2.5
		from scalarizr.util.log import fix_py25_handler_resolving		
		fix_py25_handler_resolving()
	
	logging.config.fileConfig(os.path.join(bus.etc_path, "logging.ini"))
	logger = logging.getLogger(__name__)
	globals()['_logging_configured'] = True
	
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

	# Create periodical executor for background tasks (cleanup, rotate, gc, etc...)
	bus.periodical_executor = PeriodicalExecutor()


DB_NAME = 'db.sqlite'
DB_SCRIPT = 'db.sql'

def _db_connect(file=None):
	logger = logging.getLogger(__name__)
	cnf = bus.cnf
	file = file or cnf.private_path(DB_NAME)
	logger.debug("Open SQLite database (file: %s)" % (file))	
	
	conn = sqlite.connect(file, 5.0)
	conn.row_factory = sqlite.Row
	return conn

def _init_db(file=None):
	logger = logging.getLogger(__name__)	
	cnf = bus.cnf

	# Check that database exists (after rebundle for example)	
	db_file = file or cnf.private_path(DB_NAME)
	if not os.path.exists(db_file) or not os.stat(db_file).st_size:
		logger.debug("Database doesn't exists, create new one from script")
		_create_db(file)
	
def _create_db(db_file=None, script_file=None):
	conn = _db_connect(db_file)
	conn.executescript(open(script_file or os.path.join(bus.share_path, DB_SCRIPT)).read())
	conn.commit()	
	

def _mount_private_d(mpoint, privated_image, blocks_count):
	logger = logging.getLogger(__name__)
	
	logger.debug("Move private.d configuration %s to mounted filesystem (img: %s, size: %s)", 
			mpoint, privated_image, format_size(1024*(blocks_count-1)))
	mtab = fstool.Mtab()
	if mtab.contains(mpoint=mpoint): # if privated_image exists
		logger.debug("private.d already mounted to %s", mpoint)
		return
	loopdevs = listloop()
	if privated_image in loopdevs.values():
		loopdevs = dict(zip(loopdevs.values(), loopdevs.keys()))
		loop = loopdevs[privated_image]
		logger.debug('%s already associated with %s. mounting', privated_image, loop)		
		fstool.mount(loop, mpoint)
		return
	
	if not os.path.exists(mpoint):
		os.makedirs(mpoint)
		
	mnt_opts = ('-t', 'auto', '-o', 'loop,rw')	
	if not os.path.exists(privated_image):	
		build_image_cmd = 'dd if=/dev/zero of=%s bs=1024 count=%s 2>&1' % (privated_image, blocks_count-1)
		retcode = system2(build_image_cmd, shell=True)[2]
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
			system2(str(filetool.Rsync().archive().source(mpoint+"/" if mpoint[-1] != "/" else mpoint).dest(tmp_mpoint)), shell=True)
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
	messaging_adp = ini.get('messaging', 'adapter')

	# Set base URL
	pr = urlparse(queryenv_url)
	bus.scalr_url = urlunparse((pr.scheme, pr.netloc, '', '', '', ''))
	logger.debug("Got scalr url: '%s'" % bus.scalr_url)

	logger.debug("Initialize QueryEnv client")
	queryenv = QueryEnvService(queryenv_url, server_id, cnf.key_path(cnf.DEFAULT_KEY))
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
	
	logger.debug('Schedule SNMP process')
	globals()['_snmp_scheduled_start_time'] = time.time()		

	Storage.maintain_volume_table = True

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
			'security_name' : 'notConfigUser',			
			'community_name' : g(UserDataOptions.FARM_HASH)
		}
	))
	cnf.write_key(cnf.DEFAULT_KEY, g(UserDataOptions.CRYPTO_KEY))

def _detect_scalr_version():
	pl = bus.platform
	cnf = bus.cnf
	if pl and cnf.state != ScalarizrState.IMPORTING:
		return (2, 2) if pl.get_user_data('cloud_storage_path') else (2, 1)
	return (2, 0)	

	
def init_script():
	_init()
	
	cnf = bus.cnf
	cnf.bootstrap()
	ini = cnf.rawini
	
	logger = logging.getLogger(__name__)
	logger.debug("Initialize script messaging")

	# Script producer url is scalarizr consumer url. 
	# Script can't handle any messages by himself. Leave consumer url blank
	adapter = ini.get(config.SECT_MESSAGING, config.OPT_ADAPTER)	
	kwargs = dict(ini.items("messaging_" + adapter))
	kwargs[P2pConfigOptions.SERVER_ID] = ini.get(config.SECT_GENERAL, config.OPT_SERVER_ID)
	kwargs[P2pConfigOptions.CRYPTO_KEY_PATH] = cnf.key_path(cnf.DEFAULT_KEY)
	kwargs[P2pConfigOptions.PRODUCER_URL] = kwargs[P2pConfigOptions.CONSUMER_URL]
	
	factory = MessageServiceFactory()		
	msg_service = factory.new_service(adapter, **kwargs)
	bus.messaging_service = msg_service


def _start_snmp_server():
	logger = logging.getLogger(__name__)
	# Start SNMP server in a separate process
	pid = os.fork()
	if pid == 0:
		from scalarizr.snmp.agent import SnmpServer
		globals()['_pid'] = 0
		cnf = bus.cnf; ini = cnf.rawini		
		snmp_server = SnmpServer(
			port=int(ini.get(config.SECT_SNMP, config.OPT_PORT)),
			security_name=ini.get(config.SECT_SNMP, config.OPT_SECURITY_NAME),
			community_name=ini.get(config.SECT_SNMP, config.OPT_COMMUNITY_NAME)
		)
		bus.snmp_server = snmp_server
		
		try:
			snmp_server.start()
			logger.info('[pid: %d] SNMP process terminated', os.getpid())
			sys.exit(0)
		except SystemExit:
			raise
		except (BaseException, Exception), e:
			logger.exception(e)
			sys.exit(1)
	else:
		globals()["_snmp_pid"] = pid


def onSIGTERM(*args):
	logger = logging.getLogger(__name__)
	logger.debug('Received SIGTERM')
		
	pid = os.getpid()
	if pid == _pid:
		# Main process
		logger.debug('Shutdown main process (pid: %d)', pid)
		_shutdown()
	else:
		# SNMP process
		logger.debug('Shutdown SNMP server process (pid: %d)', pid)
		snmp = bus.snmp_server
		snmp.stop()

def onSIGCHILD(*args):
	logger = logging.getLogger(__name__)
	#logger.debug("Received SIGCHILD")
	
	if globals()["_running"] and _snmp_pid:
		try:
			# Restart SNMP process if it terminates unexpectedly
			pid, sts = os.waitpid(_snmp_pid, os.WNOHANG)
			'''
			logger.debug(
				'Child terminated (pid: %d, status: %s, WIFEXITED: %s, '
				'WEXITSTATUS: %s, WIFSIGNALED: %s, WTERMSIG: %s)', 
				pid, sts, os.WIFEXITED(sts), 
				os.WEXITSTATUS(sts), os.WIFSIGNALED(sts), os.WTERMSIG(sts)
			)
			'''
			if pid == _snmp_pid and not (os.WIFEXITED(sts) and os.WEXITSTATUS(sts) == 0):
				logger.warning(
					'SNMP process [pid: %d] died unexpectedly. Restarting it', 
					_snmp_pid
				)
				globals()['_snmp_scheduled_start_time'] = time.time() + SNMP_RESTART_DELAY
				globals()['_snmp_pid'] = None
		except OSError:
			pass	
	

def _shutdown(*args):
	logger = logging.getLogger(__name__)
	globals()["_running"] = False
		
	try:
		logger.info("[pid: %d] Stopping scalarizr", os.getpid())
		
		if _snmp_pid:
			logger.debug('Send SIGTERM to SNMP process (pid: %d)', _snmp_pid)
			try:
				os.kill(_snmp_pid, signal.SIGTERM)
			except (Exception, BaseException), e:
				logger.debug("Can't kill SNMP process: %s" % e)
				
		# Shutdown messaging
		msg_service = bus.messaging_service
		msg_service.get_consumer().shutdown()
		msg_service.get_producer().shutdown()
		
		# Shutdown internal messaging
		int_msg_service = bus.int_messaging_service
		if int_msg_service:
			int_msg_service.get_consumer().shutdown()
		
		ex = bus.periodical_executor
		ex.shutdown()
		
		# Fire terminate
		bus.fire("terminate")
		
	except:
		pass
	finally:
		if os.path.exists(PID_FILE):
			os.remove(PID_FILE)
	
	logger.info('[pid: %d] Scalarizr terminated', os.getpid())



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
	
		logger.debug("Initialize scalarizr...")
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
		globals()['_pid'] = pid = os.getpid()		
		logger.info('[pid: %d] Starting scalarizr %s', pid, __version__)
		
		# Check for another running scalarzir 
		if os.path.exists(PID_FILE):
			try:
				another_pid = int(read_file(PID_FILE).strip())
			except ValueError:
				pass
			else:
				if pid != another_pid and os.path.exists('/proc/%s/status' % (another_pid,)):
					logger.error('Cannot start scalarizr: Another process (pid: %s) already running', another_pid)
					sys.exit(1)
					
		# Write PID
		write_file(PID_FILE, str(pid))
					
		cnf = bus.cnf
		cnf.on('apply_user_data', _apply_user_data)
		
		# Move private configuration to loop device
		#privated_img_path = '/mnt/privated.img'
		#_mount_private_d(cnf.private_path(), privated_img_path, 10000)
		

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
		
		# At first startup cleanup private configuration
		if cnf.state in (ScalarizrState.UNKNOWN, ScalarizrState.REBUNDLING):
			cnf.state = ScalarizrState.BOOTSTRAPPING
			priv_path = cnf.private_path()
			for file in os.listdir(priv_path):
				if file in ('.user-data', '.update'):
					continue
				path = os.path.join(priv_path, file)
				os.remove(path) if (os.path.isfile(path) or os.path.islink(path)) else shutil.rmtree(path)
		
		# Initialize local database
		_init_db()
		
		# Initialize platform module
		_init_platform()
		
		# At first startup platform user-data should be applied
		if cnf.state == ScalarizrState.BOOTSTRAPPING:
			cnf.fire('apply_user_data', cnf)			
		
		# At first scalarizr startup platform user-data should be applied
		if cnf.state in (ScalarizrState.UNKNOWN, ScalarizrState.REBUNDLING):
			cnf.state = ScalarizrState.BOOTSTRAPPING
			cnf.fire('apply_user_data', cnf)
		
		# Check Scalr version
		if not bus.scalr_version:
			version_file = cnf.private_path('.scalr-version')
			if os.path.exists(version_file):
				bus.scalr_version = tuple(read_file(version_file).strip().split('.'))
			else:
				bus.scalr_version = _detect_scalr_version()
				write_file(version_file, '.'.join(map(str, bus.scalr_version)))
			
		# Apply Command-line passed configuration options
		cnf.update(CmdLineIni.to_ini_sections(optparser.values.cnf))
		
		# Validate configuration
		num_errors = do_validate_cnf()
		if num_errors or optparser.values.validate_cnf:
			sys.exit(int(not num_errors or 1))		
		
		# Initialize scalarizr services
		_init_services()
		
		# Install signal handlers
		signal.signal(signal.SIGCHLD, onSIGCHILD)	
		signal.signal(signal.SIGTERM, onSIGTERM)

		# Create message server thread
		msg_service = bus.messaging_service
		consumer = msg_service.get_consumer()
		msg_thread = threading.Thread(target=consumer.start, name="Message server")

		# Start message server
		msg_thread.start()
		
		# Start periodical executor
		ex = bus.periodical_executor
		ex.start()
		
		# Fire start
		globals()["_running"] = True
		bus.fire("start")
	
		try:
			while _running:
				msg_thread.join(0.2)
				if not _snmp_pid and time.time() >= _snmp_scheduled_start_time:
					_start_snmp_server()
		except KeyboardInterrupt:
			pass
		finally:
			if _running and os.getpid() == _pid:
				_shutdown()
			
	except (BaseException, Exception), e:
		if isinstance(e, SystemExit):
			raise
		elif isinstance(e, KeyboardInterrupt):
			pass
		else:
			if _logging_configured:
				logger.exception(e)
			else:
				print >> sys.stderr, 'error: %s' % e
			sys.exit(1)
