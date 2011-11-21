'''
Created on Mar 3, 2010

@author: marat
'''

# Core
import scalarizr.handlers
from scalarizr.bus import bus
from scalarizr import config
from scalarizr.config import ScalarizrState
from scalarizr.messaging import Messages, MetaOptions, MessageServiceFactory
from scalarizr.messaging.p2p import P2pConfigOptions
from scalarizr.util import system2, port_in_use
from scalarizr.util.iptables import RuleSpec, IpTables, P_TCP, P_UDP

# Libs
from scalarizr.util import cryptotool

# Stdlibs
import logging, os, sys, threading



_lifecycle = None
def get_handlers():
	if not _lifecycle:
		globals()["_lifecycle"] = LifeCycleHandler()
	return [_lifecycle]

class LifeCycleHandler(scalarizr.handlers.Handler):
	_logger = None
	_bus = None
	_msg_service = None
	_producer = None
	_platform = None
	_cnf = None
	
	_new_crypto_key = None
	
	FLAG_REBOOT = "reboot"
	FLAG_HALT = "halt"
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		
		bus.define_events(
			# Fires before HostInit message is sent
			# @param msg 
			"before_host_init",
			
			# Fires after HostInit message is sent
			"host_init",
			
			# Fires when HostInitResponse received
			# @param msg
			"host_init_response",
			
			# Fires before HostUp message is sent
			# @param msg
			"before_host_up",
			
			# Fires after HostUp message is sent
			"host_up",
			
			# Fires before RebootStart message is sent
			# @param msg
			"before_reboot_start",
			
			# Fires after RebootStart message is sent
			"reboot_start",
			
			# Fires before RebootFinish message is sent
			# @param msg
			"before_reboot_finish",
			
			# Fires after RebootFinish message is sent
			"reboot_finish",
			
			# Fires before Restart message is sent
			# @param msg: Restart message
			"before_restart",
			
			# Fires after Restart message is sent
			"restart",
			
			# Fires before Hello message is sent
			# @param msg
			"before_hello",
			
			# Fires after Hello message is sent
			"hello",
			
			# Fires after HostDown message is sent
			# @param msg
			"before_host_down",
			
			# Fires after HostDown message is sent
			"host_down",
			
			# 
			# Service events
			#
			
			# Fires when behaviour is configured
			# @param service_name: Service name. Ex: mysql
			"service_configured"
		)
		bus.on(
			init=self.on_init, 
			start=self.on_start, 
			reload=self.on_reload, 
			shutdown=self.on_shutdown
		)
		self.on_reload()
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.INT_SERVER_REBOOT \
			or message.name == Messages.INT_SERVER_HALT	\
			or message.name == Messages.HOST_INIT_RESPONSE \
			or message.name == Messages.SCALARIZR_UPDATE_AVAILABLE

	
	def on_init(self):
		bus.on("host_init_response", self.on_host_init_response)
		self._producer.on("before_send", self.on_before_message_send)
		bus.on(before_reboot_finish=self._insert_iptables_rules)	
		if self._cnf.state in (ScalarizrState.BOOTSTRAPPING, ScalarizrState.IMPORTING):
			self._insert_iptables_rules()
		
		# Add internal messages to scripting skip list
		try:
			map(scalarizr.handlers.script_executor.skip_events.add, (
				Messages.INT_SERVER_REBOOT, 
				Messages.INT_SERVER_HALT, 
				Messages.HOST_INIT_RESPONSE
			))
		except AttributeError:
			pass

		# Mount all filesystems
		system2(('mount', '-a'), raise_exc=False)


	def on_start(self):
		optparser = bus.optparser
		
		if self._flag_exists(self.FLAG_REBOOT):
			self._logger.info("Scalarizr resumed after reboot")
			self._clear_flag(self.FLAG_REBOOT)			
			self._start_after_reboot()
			
		elif self._flag_exists(self.FLAG_HALT):
			self._logger.info("Scalarizr resumed after stop")
			self._clear_flag(self.FLAG_HALT)
			self._start_after_stop()
			
		elif optparser.values.import_server:
			self._logger.info('Server will be imported into Scalr')				
			self._start_import()
				
		elif self._cnf.state == ScalarizrState.IMPORTING:
			self._logger.info('Server import resumed. Awaiting Rebundle message')
			
		elif self._cnf.state == ScalarizrState.BOOTSTRAPPING:
			self._logger.info("Starting initialization")
			self._start_init()
			
		else:
			self._logger.info("Normal start")


	def _start_after_reboot(self):
		msg = self.new_message(Messages.REBOOT_FINISH, broadcast=True)
		bus.fire("before_reboot_finish", msg)
		self.send_message(msg)
		bus.fire("reboot_finish")		

	def _start_after_stop(self):
		msg = self.new_message(Messages.RESTART)
		bus.fire("before_restart", msg)
		self.send_message(msg)
		bus.fire("restart")
	
	def _start_init(self):
		# Regenerage key
		new_crypto_key = cryptotool.keygen()
		
		# Prepare HostInit
		msg = self.new_message(Messages.HOST_INIT, dict(
			crypto_key = new_crypto_key,
			snmp_port = self._cnf.rawini.get(config.SECT_SNMP, config.OPT_PORT),
			snmp_community_name = self._cnf.rawini.get(config.SECT_SNMP, config.OPT_COMMUNITY_NAME)
		), broadcast=True)
		bus.fire("before_host_init", msg)
		self.send_message(msg)
		
		# Update key file
		self._cnf.write_key(self._cnf.DEFAULT_KEY, new_crypto_key)		

		bus.cnf.state = ScalarizrState.INITIALIZING
		bus.fire("host_init")

		

	
	def _start_import(self):
		# Send Hello 
		msg = self.new_message(Messages.HELLO, 
			{"architecture" : self._platform.get_architecture()}, 
			broadcast=True # It's not really broadcast but need to contain broadcast message data 
		)		
		bus.fire("before_hello", msg)
		self.send_message(msg)
		bus.fire("hello")

	def on_reload(self):
		self._msg_service = bus.messaging_service
		self._producer = self._msg_service.get_producer()
		self._cnf = bus.cnf
		self._platform = bus.platform
		
		if self._cnf.state == ScalarizrState.RUNNING and self._cnf.key_exists(self._cnf.FARM_KEY):
			self._start_int_messaging()

	def on_shutdown(self):
		self._logger.debug('Calling %s.on_shutdown', __name__)
		# Shutdown internal messaging
		int_msg_service = bus.int_messaging_service
		if int_msg_service:
			self._logger.debug('Shutdowning internal messaging')			
			int_msg_service.get_consumer().shutdown()
		bus.int_messaging_service = None

	def on_host_init_response(self, message):
		farm_crypto_key = message.body.get('farm_crypto_key', '')
		if farm_crypto_key:
			self._cnf.write_key(self._cnf.FARM_KEY, farm_crypto_key)
			if not port_in_use(8012):
				''' This cond was added to avoid 'Address already in use' 
				when scalarizr reinitialized with `szradm --reinit` '''
				self._start_int_messaging()
		else:
			self._logger.warning("`farm_crypto_key` doesn't received in HostInitResponse. " 
					+ "Cross-scalarizr messaging not initialized")

	def _start_int_messaging(self):
		srv = IntMessagingService()
		bus.int_messaging_service = srv
		t = threading.Thread(name='IntMessageConsumer', target=srv.get_consumer().start)
		t.start()


	def _insert_iptables_rules(self, *args, **kwds):
		self._logger.debug('Adding iptables rules for scalarizr ports')		
		iptables = IpTables()
		if iptables.usable():		
			rules = []
			
			# Scalarizr ports
			rules.append(RuleSpec(dport=8012, jump='ACCEPT', protocol=P_TCP))
			rules.append(RuleSpec(dport=8013, jump='ACCEPT', protocol=P_TCP))
			rules.append(RuleSpec(dport=8014, jump='ACCEPT', protocol=P_UDP))
			
			for rule in rules:
				iptables.insert_rule(None, rule_spec = rule)


	def on_IntServerReboot(self, message):
		# Scalarizr must detect that it was resumed after reboot
		self._set_flag(self.FLAG_REBOOT)
		# Send message 
		msg = self.new_message(Messages.REBOOT_START, broadcast=True)
		bus.fire("before_reboot_start", msg)
		self.send_message(msg)
		bus.fire("reboot_start")
		
	
	def on_IntServerHalt(self, message):
		self._set_flag(self.FLAG_HALT)
		msg = self.new_message(Messages.HOST_DOWN, broadcast=True)
		bus.fire("before_host_down", msg)
		self.send_message(msg)		
		bus.fire("host_down")

	def on_HostInitResponse(self, message):
		bus.fire("host_init_response", message)
		if bus.scalr_version >= (2, 2, 3):
			self.send_message(Messages.BEFORE_HOST_UP, broadcast=True, wait_ack=True)
		msg = self.new_message(Messages.HOST_UP, broadcast=True)
		bus.fire("before_host_up", msg)
		self.send_message(msg)
		bus.cnf.state = ScalarizrState.RUNNING
		bus.fire("host_up")


	def on_ScalarizrUpdateAvailable(self, message):
		self._update_package()


	def _update_package(self):
		up_script = self._cnf.rawini.get(config.SECT_GENERAL, config.OPT_SCRIPTS_PATH) + '/update'
		#cmd = [sys.executable, up_script]
		#p = Popen(cmd, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=False, close_fds=True)
		#p.communicate()
		system2([sys.executable, up_script], close_fds=True)
		self._set_flag('update')

	def on_before_message_send(self, queue, message):
		"""
		Add scalarizr version to meta
		"""
		message.meta[MetaOptions.SZR_VERSION] = scalarizr.__version__
		
		
	def _get_flag_filename(self, name):
		return self._cnf.private_path('.%s' % name)
	
	def _set_flag(self, name):
		file = self._get_flag_filename(name)
		try:
			self._logger.debug("Touch file '%s'", file)
			open(file, "w+").close()
		except IOError, e:
			self._logger.error("Cannot touch file '%s'. IOError: %s", file, str(e))		
	
	def _flag_exists(self, name):
		return os.path.exists(self._get_flag_filename(name))
	
	def _clear_flag(self, name):
		os.remove(self._get_flag_filename(name))	
	

class IntMessagingService(object):
	'''
	Usage:
	s = bus.int_messaging_service
	p = s.new_producer('10.152.12.38')
	m = s.new_message('Cassandra_IntCreateDataBundle')
	p.send(Queues.CONTROL, m)
	'''
	_msg_service = None
	
	def __init__(self):
		cnf = bus.cnf
		f = MessageServiceFactory()
		self._msg_service = f.new_service("p2p", **{
			P2pConfigOptions.SERVER_ID : cnf.rawini.get(config.SECT_GENERAL, config.OPT_SERVER_ID),
			P2pConfigOptions.CRYPTO_KEY_PATH : cnf.key_path(cnf.FARM_KEY),
			P2pConfigOptions.CONSUMER_URL : 'http://0.0.0.0:8012'
		})
	
	def get_consumer(self):
		return self._msg_service.get_consumer()
	
	'''
	def start_consumer(self):
		if not self.consumer:
			self.consumer = self.msg_service.get_consumer()
			# IMPORTANT! to fix message double handling
			self.consumer.create_handler_thread = False
			# Start consumer thread
			t = threading.Thread(name="IntMessagingConsumer", target=self.consumer.start)
			t.start()
	'''
	
	def new_producer(self, host):
		return self._msg_service.new_producer(endpoint="http://%s:8012" % host)
	
	def new_message(self, *args, **kwargs):
		return self._msg_service.new_message(*args, **kwargs)