'''
Created on Sep 30, 2011

@author: Dmytro Korsakov
'''
import os
import time
import logging


from scalarizr.config import BuiltinBehaviours, ScalarizrState
from scalarizr.services import BaseConfig, BaseService, lazy
from scalarizr.libs.metaconf import Configuration
from scalarizr.util import disttool, cryptotool, system2, \
				PopenError, wait_until, initdv2, software
from scalarizr.util.filetool import rchown
import pymongo


try:
	MONGOD = software.whereis('mongod')[0]
	MONGO_CLI = software.whereis('mongo')[0]
	MONGO_DUMP = software.whereis('mongodump')[0]
	MONGOS = software.whereis('mongos')[0]	
except IndexError:
	#raise Exception('Cannot locate mongo executables.')
	pass

ROUTER_DEFAULT_PORT = 27017
ARBITER_DEFAULT_PORT = 27020
REPLICA_DEFAULT_PORT = 27018
CONFIG_SERVER_DEFAULT_PORT = 27019

SERVICE_NAME = BuiltinBehaviours.MONGODB
STORAGE_PATH = "/mnt/mongodb-storage"

LOG_PATH_DEFAULT = '/var/log/mongodb/mongodb.log'
DB_PATH_DEFAULT = '/var/lib/mongodb'
LOCK_FILE = 'mongod.lock'
DEFAULT_USER = 'mongodb'
SCALR_USER = 'scalr'
STORAGE_DATA_DIR = os.path.join(STORAGE_PATH, 'data')

CONFIG_PATH_DEFAULT = UBUNTU_CONFIG_PATH = CENTOS_CONFIG_PATH = '/etc/mongodb.conf'

ARBITER_DATA_DIR = '/tmp/arbiter'
ARBITER_LOG_PATH = '/var/log/mongodb/mongodb.arbiter.log'
ARBITER_CONF_PATH = '/etc/mongodb.arbiter.conf'

CONFIG_SERVER_DATA_DIR = os.path.join(STORAGE_PATH, 'config_server')
CONFIG_SERVER_CONF_PATH = '/etc/mongodb.configsrv.conf'
CONFIG_SERVER_LOG_PATH = '/var/log/mongodb/mongodb.configsrv.log'

ROUTER_LOG_PATH = '/var/log/mongodb/mongodb.router.log'

MAX_START_TIMEOUT = 180
MAX_STOP_TIMEOUT = 65


				
class MongoDBDefaultInitScript(initdv2.ParametrizedInitScript):
	socket_file = None
	
	@lazy
	def __new__(cls, *args, **kws):
		obj = super(MongoDBDefaultInitScript, cls).__new__(cls, *args, **kws)
		cls.__init__(obj)
		return obj
			
	def __init__(self):
		initd_script = None
		if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
			initd_script = ('/usr/sbin/service', 'mongodb')
		else:
			initd_script = '/etc/init.d/mongodb'
		initdv2.ParametrizedInitScript.__init__(self, name=SERVICE_NAME, 
				initd_script=initd_script)
		
	def status(self):
		'''
		By default Ubuntu automatically starts mongodb process on 27017 
		which is exactly the port number used by our router process.
		'''
		p = MongoCLI(port=ROUTER_DEFAULT_PORT)
		return initdv2.Status.RUNNING if p.has_connection else initdv2.Status.NOT_RUNNING

	def stop(self, reason=None):
		initdv2.ParametrizedInitScript.stop(self)
		wait_until(lambda: self.status() == initdv2.Status.NOT_RUNNING, timeout=MAX_STOP_TIMEOUT)
	
	def restart(self, reason=None):
		initdv2.ParametrizedInitScript.restart(self)
	
	def reload(self, reason=None):
		initdv2.ParametrizedInitScript.restart(self)
		
	def start(self):
		initdv2.ParametrizedInitScript.start(self)
		wait_until(lambda: self.status() == initdv2.Status.RUNNING, sleep=1, timeout=MAX_START_TIMEOUT, 
				error_text="In %s seconds after start Redis state still isn't 'Running'" % MAX_START_TIMEOUT)

		
initdv2.explore(SERVICE_NAME, MongoDBDefaultInitScript)
	
		
class MongoDB(BaseService):
	_arbiter = None
	_instance = None
	_config_server = None
	keyfile = None
	login = None
	password = None

	
	def __init__(self, keyfile=None):
		self.keyfile = keyfile
		self._objects = {}
		self._logger = logging.getLogger(__name__)
		self.default_init_script = initdv2.lookup(SERVICE_NAME)

								
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(MongoDB, cls).__new__(
								cls, *args, **kwargs)
		return cls._instance


	def authenticate(self, login=SCALR_USER, password=None):
		self.login = login
		self.password = password
		self.cli.auth(login, password)
		
		
	@property
	def is_replication_master(self):
		return self.cli.is_master()['ismaster']

	
	def prepare(self, rs_name):
		'''
		Somehow shardsvr in Mongo 2.01 messes with our port option
		And since it is not required we decided to skip it for good.
		'''
		#self.config.shardsvr = True
		'''
		option nojournal is True on all 64bit platforms by default
		'''
		#self.config.nojournal = False
		self.config.replSet = rs_name
		self.config.dbpath = self.working_dir.create(STORAGE_DATA_DIR)
		self.config.logpath = LOG_PATH_DEFAULT
		self.config.port = REPLICA_DEFAULT_PORT
		self.config.logappend = True
		self.working_dir.unlock()
		

	def _prepare_arbiter(self, rs_name):
		if not os.path.exists(ARBITER_DATA_DIR):
			os.makedirs(ARBITER_DATA_DIR)
		rchown(DEFAULT_USER, ARBITER_DATA_DIR)	
			
		self.arbiter_conf.dbpath = ARBITER_DATA_DIR
		self.arbiter_conf.replSet = rs_name
		self.arbiter_conf.shardsvr = True
		self.arbiter_conf.port = ARBITER_DEFAULT_PORT
		self.arbiter_conf.logpath = ARBITER_LOG_PATH


	def _prepare_config_server(self):
		self._logger.debug('Preparing config server')
		if not os.path.exists(CONFIG_SERVER_DATA_DIR):
			os.makedirs(CONFIG_SERVER_DATA_DIR)
		rchown(DEFAULT_USER, CONFIG_SERVER_DATA_DIR)
		'''
		configsvr changes the default port and turns on the diaglog, 
		a log that keeps every action the config database performs 
		in a replayable format, just in case.
		For mongo 1.8+ use --port 27019 and --journal (instead of --diaglog). 
		Journaling gives mostly the same effect as the diaglog with better performance.
		P.S. Assume that mongodb roles Scalr will be build on x64 platform only
		Wchich means journal option by default will be on.
		'''
		self.config_server_conf.configsvr = True
		self.config_server_conf.port = CONFIG_SERVER_DEFAULT_PORT
		self.config_server_conf.logpath = CONFIG_SERVER_LOG_PATH


	def initiate_rs(self):
		'''
		@return (host:port)
		'''
		ret = self.cli.initiate_rs()
		if ret and ret['ok'] == '0':
			raise BaseException('Could not initialize replica set: %s' % ret['errmsg'])
		
		wait_until(lambda: self.is_replication_master, sleep=5, logger=self._logger,
					timeout=120, start_text='Wait until node becomes replication primary')		
		
		return ret['me'].split(':') if ret else None
	
	
	def start_arbiter(self):
		self._prepare_arbiter(self.config.replSet)
		self.arbiter.start()
	
	
	def stop_arbiter(self):
		self.arbiter.stop(reason='Stopping arbiter')
		
	
	def start_config_server(self):
		self._prepare_config_server()
		self._logger.debug('Starting config server')
		self.config_server.start()
		
		
	def stop_config_server(self):
		self.config_server.stop()
		
		
	def start_router(self):
		self.default_init_script.stop('Stopping default mongod service')
		Mongos.set_keyfile(self.keyfile.path)
		Mongos.start()
		
	
	def stop_router(self):
		Mongos.stop()
	
	
	def register_slave(self, ip, port=None):
		ret = self.cli.add_replica(ip, port)
		if ret['ok'] == '0':
			self._logger.error('Could not add replica %s to set: %s' % (ip, ret['errmsg']))
		
			
	def register_arbiter(self,ip,port=None):
		ret = self.cli.add_arbiter(ip, port or ARBITER_DEFAULT_PORT)
		if ret['ok'] == '0':
			self._logger.error('Could not add arbiter %s to set: %s' % (ip, ret['errmsg']))
			
			
	def unregister_slave(self,ip,port=None):
		ret = self.cli.remove_slave(ip, port=None)
		if ret['ok'] == '0':
			self._logger.error('Could not remove replica %s from set: %s' % (ip, ret['errmsg']))
	
	"""
	def wait_for_sync(self):
		wait_until(lambda: self.status == 1 or self.status == 2, timeout=3600, sleep=2)
	"""
	
	
	@property
	def status(self):
		'''
		rs.status['myState']
		
		0	 Starting up, phase 1 (parsing configuration)
		1	 Primary
		2	 Secondary
		3	 Recovering (initial syncing, post-rollback, stale members)
		4	 Fatal error
		5	 Starting up, phase 2 (forking threads)
		6	 Unknown state (member has never been reached)
		7	 Arbiter
		8	 Down
		9	 Rollback
		'''
		ret = self.cli.get_rs_status()
		if 'errmsg' in ret:
			self._logger.error('Could not get status of replica set' % (ret['errmsg']))
		else:
			return int(ret['myState']) if 'myState' in ret else None
				
				
	@property
	def replicas(self):
		ret = self.cli.is_master()
		return ret['hosts'] if 'hosts' in ret else []
	
	
	@property
	def arbiters(self):
		ret = self.cli.is_master()
		return ret['arbiters'] if 'arbiters' in ret else []
	
	
	@property
	def primary_host(self):
		ret = self.cli.is_master()
		return ret['primary'] if 'primary' in ret else None
	
	
	@property
	def dbpath(self):
		return self.config.dbpath
	
	
	@property
	def arbiter(self):
		if not self._arbiter:
			self._arbiter = Mongod(ARBITER_CONF_PATH, self.keyfile.path, ARBITER_DATA_DIR, ARBITER_DEFAULT_PORT)
		return self._arbiter
	
	
	@property
	def config_server(self):
		if not self._config_server:
			self._config_server = Mongod(CONFIG_SERVER_CONF_PATH, self.keyfile.path, CONFIG_SERVER_DATA_DIR, \
										 CONFIG_SERVER_DEFAULT_PORT)
		return self._config_server


	def _get_mongod(self):
		return self._get('mongod', Mongod.find, self.config, self.keyfile.path, self.cli)
	
	def _set_mongod(self, obj):
		self._set('mongod', obj)


	def _get_cli(self):
		return self._get('cli', MongoCLI.find, REPLICA_DEFAULT_PORT, self.login, self.password)
	
	def _set_cli(self, obj):
		self._set('cli', obj)


	def _get_working_directory(self):
		return self._get('working_directory', WorkingDirectory.find, self.config)
		
	def _set_working_directory(self, obj):
		self._set('working_directory', obj)
	
	
	def _get_config(self):
		return self._get('mongodb_conf', MongoDBConfig.find)
	
	def _set_config(self, obj):
		self._set('mongodb_conf', obj)
		
		
	def _get_arbiter_conf(self):
		return self._get('arbiter_config', ArbiterConf.find, '/etc')
	
	def _set_arbiter_conf(self, obj):
		self._set('arbiter_config', obj)
		
		
	def _get_cfg_srv_conf(self):
		return self._get('cfg_srv_config', ConfigServerConf.find, '/etc')
	
	def _set_cfg_srv_conf(self, obj):
		self._set('cfg_srv_config', obj)
		
		
	def _get_router_cli(self):
		return self._get('router_cli', MongoCLI.find, ROUTER_DEFAULT_PORT)
	
	def _set_router_cli(self, obj):
		self._set('router_cli', obj)
	
									
	cli = property(_get_cli, _set_cli)
	router_cli = property(_get_router_cli, _set_router_cli)
	mongod = property(_get_mongod, _set_mongod)
	working_dir = property(_get_working_directory, _set_working_directory)	
	config = property(_get_config, _set_config)
	arbiter_conf = property(_get_arbiter_conf, _set_arbiter_conf)
	config_server_conf = property(_get_cfg_srv_conf, _set_cfg_srv_conf)

	
class MongoDump(object):
	
	host = None
	port = None
	
	def __init__(self, host=None, port=None):
		self._logger = logging.getLogger(__name__)
		self.host = host
		self.port = port
	
	def create(self, dbname, dst):
		self._logger.debug('Dumping database %s to %s' % (dbname, dst))
		args = [MONGO_DUMP, '-d', dbname, '-o', dst]
		if self.host:
			args += ('-h', self.host if not self.port else "%s:%s" % (self.host, self.port))
		return system2(args)

		
class KeyFile(object):
	
	def __init__(self, path):
		self.path = path
			
	@classmethod
	def find(cls, path):
		return cls(path)

	def exists(self):
		return os.path.exists(self.path)

	def __repr__(self):
		return open(self.path).read().strip() if os.path.exists(self.path) else None


class WorkingDirectory(object):
	
	path = None
	user = DEFAULT_USER
	
	def __init__(self, path=None):
		self._logger = logging.getLogger(__name__)  
		self.path = path
		
	@classmethod
	def find(cls, mongo_conf):
		dir = mongo_conf.dbpath
		if not dir:
			dir = DB_PATH_DEFAULT
		return cls(dir)	

	def is_initialized(self, path): 
		if os.path.exists(path):
			fnames = os.listdir(path)
			return 'local.0' in fnames
		return False
	
	def is_locked(self):
		return os.path.exists(self.lock_path)
	
	def unlock(self):
		if self.is_locked():
			self._logger.warning('Lock was found in database directory %s. Last time Mongodb was not shut down properly.' % self.path)
			os.remove(self.lock_path)	
			
	def create(self, dst):
		if not os.path.exists(dst):
			self._logger.debug('Creating directory structure for mongodb files: %s' % dst)
			os.makedirs(dst)
			
		self._logger.debug("changing directory owner to %s" % self.user)	
		rchown(self.user, dst)			
		self.path = dst

		return dst

	@property
	def lock_path(self):
		return os.path.join(self.path, LOCK_FILE)


class MongoDBConfig(BaseConfig):
	
	config_type = 'mongodb'
	config_name = 'mongodb.conf'
	
	@classmethod
	def find(cls, config_dir=None):
		conf_path = UBUNTU_CONFIG_PATH if disttool.is_ubuntu() else CENTOS_CONFIG_PATH
		return cls(os.path.join(config_dir, cls.config_name) if config_dir else conf_path)
	
	def set(self, option, value):
		if not self.data:
			self.data = Configuration(self.config_type)
			if os.path.exists(self.path):
				self.data.read(self.path)
		if value :
			self.data.set(option,value, force=True)
		else:
			self.data.remove(option)
		if self.autosave:
			self.save_data()
			self.data = None

	def set_bool_option(self, option, value):
		try:
			assert value in (None, True, False)
		except AssertionError:
			raise ValueError('%s must be a boolean (got %s instead)' % (option, type(value)))
		value_to_set = None if value is None else str(value).lower()
		self.set(option, value_to_set)

	def get_bool_option(self, option):
		value = self.get(option)
		try:
			assert not value or value == 'true' or value == 'false'
		except AssertionError:
			raise ValueError('%s must be true or false (got %s instead)' % (option,  type(value)))
		return True if value == 'true' else False

	def _get_logpath(self):
		return self.get('logpath')
	
	def _set_logpath(self, path):
		self.set('logpath', path)    
	
	def _get_replSet(self):
		return self.get('replSet')
	
	def _set_replSet(self, name):
		self.set('replSet', name)    
	
	def _get_port(self):
		return self.get_numeric_option('port')
	
	def _set_port(self, number):
		self.set_numeric_option('port', number)    
	
	def _get_logappend(self):
		return self.get_bool_option('logappend')
	
	def _set_logappend(self, on=True):
		self.set_bool_option('logappend', on)    

	def _get_dbpath(self):
		return self.get('dbpath')
	
	def _set_dbpath(self, path):
		self.set_path_type_option('dbpath', path)
	
	def _get_nojournal(self):
		return self.get_bool_option('nojournal')
	
	def _set_nojournal(self, on=False):
		self.set_bool_option('nojournal', on)      
	
	def _get_nohttpinterface(self):
		return self.get_bool_option('nohttpinterface')
	
	def _set_nohttpinterface(self, on=False):
		self.set_bool_option('nohttpinterface', on) 

	def _get_rest(self):
		return self.get_bool_option('rest')
	
	def _set_rest(self, on=False):
		self.set_bool_option('rest', on)
		
	def _set_shardsvr(self, value):
		self.set_bool_option('shardsvr', value)

	def _get_shardsvr(self):
		return self.get_bool_option('shardsvr')
	
	shardsvr = property(_get_shardsvr, _set_shardsvr)
	rest = property(_get_rest, _set_rest)
	nohttpinterface = property(_get_nohttpinterface, _set_nohttpinterface)
	nojournal = property(_get_nojournal, _set_nojournal)
	dbpath = property(_get_dbpath, _set_dbpath)
	logappend = property(_get_logappend, _set_logappend)
	port = property(_get_port, _set_port)
	replSet = property(_get_replSet, _set_replSet)
	logpath = property(_get_logpath, _set_logpath)
	
	
class ArbiterConf(MongoDBConfig):
	config_name = 'mongodb.arbiter.conf'

	@classmethod
	def find(cls, config_dir='/etc'):
		return cls(os.path.join(config_dir, cls.config_name))
	
	
class ConfigServerConf(MongoDBConfig):
	config_name = 'mongodb.configsrv.conf'

	@classmethod
	def find(cls, config_dir='/etc'):
		return cls(os.path.join(config_dir, cls.config_name))
					
	def _set_configsvr(self, value):
		self.set_bool_option('configsvr', value)

	def _get_configsvr(self):
		return self.get_bool_option('configsvr')
	
	configsvr = property(_get_configsvr, _set_configsvr)

	
class Mongod(object):	
	def __init__(self, configpath=None, keyfile=None, dbpath=None, port=None, cli=None):
		self._logger = logging.getLogger(__name__)
		self.configpath = configpath
		self.dbpath = dbpath
		self.keyfile = keyfile
		self.cli = cli or MongoCLI(port=port)
		self.port = port
		self.sock = initdv2.SockParam(self.port or REPLICA_DEFAULT_PORT)
		
	@classmethod
	def find(cls, mongo_conf=None, keyfile=None, cli=None):
		config_path = mongo_conf.path or CONFIG_PATH_DEFAULT
		return cls(configpath=config_path, keyfile=keyfile, cli=cli)

	@property
	def args(self):
		s = ['--fork']
		if self.configpath:
			s.append('--config=%s' % self.configpath)
		if self.dbpath:
			s.append('--dbpath=%s' % self.dbpath)
		if self.port:
			s.append('--port=%s' % self.port)
		if self.keyfile and os.path.exists(self.keyfile):
			rchown(DEFAULT_USER, self.keyfile)	
			s.append('--keyFile=%s' % self.keyfile)
		return s
	
	def start(self):
		try:
			if not self.is_running:
				system2(['sudo', '-u', DEFAULT_USER, MONGOD,] + self.args)
				'''
				mongod process takes some time before it actualy starts accepting connections
				it can easily be as long as 160 seconds on a Large instance
				'''
				wait_until(lambda: self.is_running, timeout=MAX_START_TIMEOUT)
				wait_until(lambda: self.cli.has_connection, timeout=MAX_START_TIMEOUT)
				
		except PopenError, e:
			self._logger.error('Unable to start mongod process: %s' % e)

	def stop(self, reason=None):
		if self.is_running:
			self.cli.shutdown_server()
			wait_until(lambda: not self.is_running, timeout=MAX_STOP_TIMEOUT)
	
	def restart(self, reason=None):
		if not self.is_running:
			self.stop(reason)
			self.start()	
	
	@property
	def is_running(self):
		try:
			initdv2.wait_sock(self.sock)
			return True
		except:
			return False


class Mongos(object):
	sock = initdv2.SockParam(ROUTER_DEFAULT_PORT)
	keyfile = None
	
	@classmethod
	def set_keyfile(cls, keyfile = None):
		cls.keyfile = keyfile
	
	@classmethod
	def start(cls):
		if not cls.is_running():
			args = [MONGOS, '--fork', '--logpath', ROUTER_LOG_PATH,
									'--configdb', 'mongo-0-0:%s' % CONFIG_SERVER_DEFAULT_PORT]
			if cls.keyfile and os.path.exists(cls.keyfile):
				rchown(DEFAULT_USER, cls.keyfile)	
				args.append('--keyFile=%s' % cls.keyfile)
			system2(args)
			cli = MongoCLI(port=ROUTER_DEFAULT_PORT)
			wait_until(lambda: cls.is_running, timeout=MAX_START_TIMEOUT)
			wait_until(lambda: cli.has_connection, timeout=MAX_START_TIMEOUT)

	@classmethod
	def stop(cls):
		if cls.is_running():
			cli = MongoCLI(port=ROUTER_DEFAULT_PORT)
			cli.shutdown_server()
			wait_until(lambda: not cls.is_running, timeout=MAX_STOP_TIMEOUT)

	@classmethod
	def is_running(cls):
		try:
			initdv2.wait_sock(cls.sock)
			return True
		except:
			return False


class MongoCLI(object):
	
	authenticated = False
	
	def __init__(self, port=REPLICA_DEFAULT_PORT, login=SCALR_USER, password=None):
		self.port = port
		self._logger = logging.getLogger(__name__)
		self.login = None
		self.password = None
		self.sock = initdv2.SockParam(port)

	@classmethod
	def find(cls, port=REPLICA_DEFAULT_PORT, login=SCALR_USER, password=None):
		return cls(port=port, login=login, password=password)
	
	
	def auth(self,login,password):
		self.login = login
		self.password = password
		
		
	@property
	def connection(self):
		if not hasattr(self, '_con'):
			self._con = pymongo.Connection('localhost', self.port)
		if not self.authenticated and self.login and self.password and self.is_port_listening:
			self._con.admin.authenticate(self.login, self.password)
			self.authenticated = True
		return self._con


	@property	
	def has_connection(self):
		'''
		MongoDB shell version: 2.0.0
		connecting to: test
		Thu Oct 20 13:17:21 Error: couldn't connect to server 127.0.0.1 shell/mongo.js:84
		exception: connect failed
		'''
		try:
			self.connection.db.test.find_one()
		except pymongo.errors.AutoReconnect, e:
			if "Connection refused" in str(e):
				return False
		except BaseException, e:
			self._logger.debug(e)
		return True


	@property
	def is_port_listening(self):
		try:
			initdv2.wait_sock(self.sock)
			return True
		except:
			return False
	
	
	def list_databases(self):
		return self.connection.database_names()

	
	def initiate_rs(self):
		'''
	    initializes replica set
	    '''
		try:
			res = self.connection.admin.command('replSetInitiate')
		except pymongo.errors.OperationFailure, e:
			self._logger.warning(e)
			res = None
		return res	
	
	def add_shard(self, rs_name, rs_members):
		host_str = '%s/%s' % (rs_name, ','.join(rs_members))
		return self.connection.admin.command('addshard', host_str)

	
	def add_replica(self, ip, port=None, arbiter=False):
		port = port or REPLICA_DEFAULT_PORT
		new_member = {}
		new_member['host'] = "%s:%s" % (ip, port)
		cfg = self.get_rs_config()
		cfg['version'] = cfg['version'] + 1
		new_member['_id'] = max([m['_id'] for m  in cfg['members']]) + 1
		cfg['members'].append(new_member)
		if arbiter:
			cfg['arbiterOnly'] = True
		return self.rs_reconfig(cfg)
		
	
	def is_master(self):
		return self.connection.admin.command('isMaster')

	
	def get_rs_status(self):
		return self.connection.admin.command('replSetGetStatus')

	
	def get_rs_config(self):
		rs_count = self.connection.local.system.replset.count()
		assert rs_count, "No replica set found" 
		return self.connection.local.system.replset.find_one()


	def rs_reconfig(self, config, force=False):
		return self.connection.admin.command("replSetReconfig", config, force=force)		


	def add_arbiter(self,ip, port=None):
		return self.add_replica(ip, port, arbiter=True)


	def remove_slave(self, ip, port=None):
		port = port or REPLICA_DEFAULT_PORT
		host_to_del = "%s:%s" % (ip, port)
		cfg = self.get_rs_config()
		cfg['version'] = cfg['version'] + 1
		for member in cfg['members']:
			if member['host'] == host_to_del:
				cfg['members'].remove(member)
				self.rs_reconfig(cfg)
				break
		else:
			raise Exception("Host %s not found in replica set")


	def shutdown_server(self):
		return self.connection.admin.command('shutdown')

	
	def sync(self):
		return self.connection.admin.command('fsync')

	
	def stop_balancer(self):
		'''
		// connect to mongos (not a config server!)
		
		> use config
		> db.settings.update( { _id: "balancer" }, { $set : { stopped: true } } , true ); 
		>
		> // wait for any migrations that were in progress to finish
		> // "state" field is zero if no migrations in progress
		> while( db.locks.findOne({_id: "balancer"}).state ) { print("waiting..."); sleep(1000); }
		
		http://www.mongodb.org/display/DOCS/Backing+Up+Sharded+Cluster
		'''
		self._logger.info('Stopping balancer')
		self.connection.config.settings.update({'_id': 'balancer'}, {'stopped' : True}, True)
		self._logger.debug("Waiting until balancer finishes it's round")
		while self.connection.locks.find_one({'_id': "balancer"})['state']:
			time.sleep(1)

	
	def start_balancer(self):
		'''
		>use config
		>db.settings.update( { _id: "balancer" }, { $set : { stopped: false } } , true );
		'''
		self._logger.info('Starting balancer')
		self.connection.config.settings.update({'_id': 'balancer'}, {'stopped' : False}, True)
		

	def create_or_update_admin_user(self, username, password):
		self.connection.admin.add_user(username, password)


		
'''
> rs.initiate()
{
    "info2" : "no configuration explicitly specified -- making one",
    "me" : "ec2-107-22-29-228.compute-1.amazonaws.com:27017",
    "info" : "Config now saved locally.  Should come online in about a minute.",
    "ok" : 1
}


> rs.initiate()
{
	"info2" : "no configuration explicitly specified -- making one",
	"me" : "mongo-0-0:27018",
	"errmsg" : "couldn't initiate : new file allocation failure",
	"ok" : 0
}


> rs.initiate()
{
	"assertion" : "Can't take a write lock while out of disk space",
	"assertionCode" : 14031,
	"errmsg" : "db assertion failure",
	"ok" : 0
}



> rs.config()
{
    "_id" : "trololo",
    "version" : 1,
    "members" : [
        {
            "_id" : 0,
            "host" : "ec2-107-22-29-228.compute-1.amazonaws.com:27017"
        }
    ]
}

PRIMARY> db.isMaster();
{
    "setName" : "trololo",
    "ismaster" : true,
    "secondary" : false,
    "hosts" : [
        "ec2-107-22-29-228.compute-1.amazonaws.com:27017"
    ],
    "primary" : "ec2-107-22-29-228.compute-1.amazonaws.com:27017",
    "me" : "ec2-107-22-29-228.compute-1.amazonaws.com:27017",
    "maxBsonObjectSize" : 16777216,
    "ok" : 1
}

PRIMARY> rs.status()
{
    "set" : "trololo",
    "date" : ISODate("2011-10-18T12:34:11Z"),
    "myState" : 1,
    "members" : [
        {
            "_id" : 0,
            "name" : "ec2-107-22-29-228.compute-1.amazonaws.com:27017",
            "health" : 1,
            "state" : 1,
            "stateStr" : "PRIMARY",
            "optime" : {
                "t" : 1318940313000,
                "i" : 1
            },
            "optimeDate" : ISODate("2011-10-18T12:18:33Z"),
            "self" : true
        }
    ],
    "ok" : 1
}

PRIMARY> rs.add("127.0.0.1:27018")
{
    "assertion" : "can't use localhost in repl set member names except when using it for all members",
    "assertionCode" : 13393,
    "errmsg" : "db assertion failure",
    "ok" : 0
}
    
 PRIMARY> rs.add("ec2-107-22-29-228.compute-1.amazonaws.com:27018")
{ "ok" : 1 }  


SECONDARY> rs.status()
{
    "set" : "trololo",
    "date" : ISODate("2011-10-18T13:33:09Z"),
    "myState" : 2,
    "syncingTo" : "ec2-107-22-29-228.compute-1.amazonaws.com:27017",
    "members" : [
        {
            "_id" : 0,
            "name" : "ec2-107-22-29-228.compute-1.amazonaws.com:27017",
            "health" : 1,
            "state" : 1,
            "stateStr" : "PRIMARY",
            "uptime" : 2975,
            "optime" : {
                "t" : 1318941802000,
                "i" : 1
            },
            "optimeDate" : ISODate("2011-10-18T12:43:22Z"),
            "lastHeartbeat" : ISODate("2011-10-18T13:33:07Z"),
            "pingMs" : 0
        },
        {
            "_id" : 1,
            "name" : "ec2-107-22-29-228.compute-1.amazonaws.com:27018",
            "health" : 1,
            "state" : 2,
            "stateStr" : "SECONDARY",
            "optime" : {
                "t" : 1318941802000,
                "i" : 1
            },
            "optimeDate" : ISODate("2011-10-18T12:43:22Z"),
            "self" : true
        }
    ],
    "ok" : 1
}

SECONDARY> db.isMaster()
{
    "setName" : "trololo",
    "ismaster" : false,
    "secondary" : true,
    "hosts" : [
        "ec2-107-22-29-228.compute-1.amazonaws.com:27018",
        "ec2-107-22-29-228.compute-1.amazonaws.com:27017"
    ],
    "primary" : "ec2-107-22-29-228.compute-1.amazonaws.com:27017",
    "me" : "ec2-107-22-29-228.compute-1.amazonaws.com:27018",
    "maxBsonObjectSize" : 16777216,
    "ok" : 1
}
SECONDARY> 

#running without replicaSet
> db.isMaster()
{ "ismaster" : true, "maxBsonObjectSize" : 16777216, "ok" : 1 }

 
SECONDARY> rs.config()
{
    "_id" : "trololo",
    "version" : 2,
    "members" : [
        {
            "_id" : 0,
            "host" : "ec2-107-22-29-228.compute-1.amazonaws.com:27017"
        },
        {
            "_id" : 1,
            "host" : "ec2-107-22-29-228.compute-1.amazonaws.com:27018"
        }
    ]
}
SECONDARY>    

> rs.status()
{ "errmsg" : "not running with --replSet", "ok" : 0 } 
'''
