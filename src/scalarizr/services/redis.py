'''
Created on Aug 10, 2011

@author: Dmytro Korsakov
'''

import os
import signal
import logging
import shutil

from scalarizr.bus import bus
from scalarizr.util import initdv2, system2, PopenError, wait_until
from scalarizr.services import lazy, BaseConfig, BaseService, ServiceError
from scalarizr.util import disttool, cryptotool, firstmatched
from scalarizr.util.filetool import rchown
from scalarizr.libs.metaconf import Configuration, NoPathError


SERVICE_NAME = CNF_SECTION = DEFAULT_USER = 'redis'

UBUNTU_BIN_PATH 	 = '/usr/bin/redis-server'	
CENTOS_BIN_PATH 	 = '/usr/sbin/redis-server'	
UBUNTU_CONFIG_DIR = '/etc/redis'
CENTOS_CONFIG_DIR = '/etc/redis'

OPT_REPLICATION_MASTER  = "replication_master"

REDIS_CLI_PATH = '/usr/bin/redis-cli'	
DEFAULT_DIR_PATH = '/var/lib/redis'
REDIS_USER = 'redis'	
DB_FILENAME = 'dump.rdb'
AOF_FILENAME = 'appendonly.aof'

AOF_TYPE = 'aof'
SNAP_TYPE = 'snapshotting'

MAX_START_TIMEOUT = 30
DEFAULT_PORT	= 6379


LOG = logging.getLogger(__name__)


class RedisInitScript(initdv2.ParametrizedInitScript):
	socket_file = None
	
	@lazy
	def __new__(cls, *args, **kws):
		obj = super(RedisInitScript, cls).__new__(cls, *args, **kws)
		cls.__init__(obj)
		return obj
			
	def __init__(self):
		initd_script = None
		if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
			initd_script = ('/usr/sbin/service', 'redis-server')
		else:
			initd_script = firstmatched(os.path.exists, ('/etc/init.d/redis', '/etc/init.d/redis-server'))
		initdv2.ParametrizedInitScript.__init__(self, name=SERVICE_NAME, 
				initd_script=initd_script)
	
	@property
	def _processes(self):
		args = ('ps', '-G', 'redis', '-o', 'command', '--no-headers')
		bin_path = UBUNTU_BIN_PATH if disttool.is_ubuntu() else CENTOS_BIN_PATH	
		conf_dir = UBUNTU_CONFIG_DIR if disttool.is_ubuntu() else CENTOS_CONFIG_DIR
		conf_path = os.path.join(conf_dir, 'redis.conf')
		try:	
			p = [x for x in system2(args, silent=True)[0].split('\n') if x and bin_path + ' ' + conf_path in x]
		except PopenError,e:
			p = []
		return p
	
	def status(self):
		return initdv2.Status.RUNNING if self._processes else initdv2.Status.NOT_RUNNING

	def stop(self, reason=None):
		initdv2.ParametrizedInitScript.stop(self)
		wait_until(lambda: not self._processes, timeout=10, sleep=1)
	
	def restart(self, reason=None):
		self.stop()
		self.start()
	
	def reload(self, reason=None):
		initdv2.ParametrizedInitScript.restart(self)
		
	def start(self):
		initdv2.ParametrizedInitScript.start(self)
		wait_until(lambda: self._processes, timeout=10, sleep=1)
		redis_conf = RedisConf.find()
		password = redis_conf.requirepass
		cli = RedisCLI(password)
		wait_until(lambda: cli.test_connection(), timeout=10, sleep=1)
	
initdv2.explore(SERVICE_NAME, RedisInitScript)
	
	
class Redisd(object):

	redis_conf = None
	bin_path = UBUNTU_BIN_PATH if disttool.is_ubuntu() else CENTOS_BIN_PATH	
	
	def __init__(self, config_path=None):
		self.config_path = config_path
		self.redis_conf = RedisConf(config_path)
		self.cli = RedisCLI(self.redis_conf.requirepass, self.redis_conf.port)
	
	
	def find(self):
		pass
	
	
	@property
	def args(self):
		return [self.config_path,]


	def start(self):
		try:
			if not self.running:
				LOG.debug('Starting %s' % self.bin_path)
				system2(['sudo', '-u', DEFAULT_USER, self.bin_path,] + self.args)
				wait_until(lambda: self.running, timeout=MAX_START_TIMEOUT)
				wait_until(lambda: self.cli.test_connection, timeout=MAX_START_TIMEOUT)
				LOG.debug('%s process has been started.' % SERVICE_NAME)
				
		except PopenError, e:
			LOG.error('Unable to start redis process: %s' % e)
	
	
	def stop(self):
		os.kill(int(self.pid), signal.SIGTERM)
	
	
	def restart(self):
		if self.running:
			self.stop()
		self.start()
	
	
	def reload(self):
		self.restart()
	
	
	@property
	def running(self):
		try:
			out = system2(('ps', '-G', 'redis', '-o', 'command', '--no-headers'))[0]
		except:
			out = None
		return False if not out else self.bin_path in out and self.config_path in out	
	
	
	@property
	def pid(self):
		pid = open(self.redis_conf.pidfile).read().strip()
		return pid


class RedisInstances(object):

	creds = None
	instances = None
	ports = None
	master = None
	persistence_type = None
	
	
	def __init__(self, master=False, persistence_type=SNAP_TYPE, ports=[], passwords=[]):
		self.master = master
		self.persistence_type = persistence_type
		self.ports = ports 
		self.instances = []
		self.creds = dict()
		self.init_processes(ports, passwords)
		
	def __iter__(self):
		return iter(self.instances)
			
	def get_instances(self, port=None, password=None):
		for instance in self.instances:
			if instance.port == port or instance.password == password:
				return instance
		raise ServiceError('Redis instance with password %s or port %s not found' % (port, password))	
		
	def init_processes(self, ports=[], passwords=[]):
		creds = dict(zip(ports or [], passwords or []))
		for port,password in creds.items():
			ports = [instance.port for instance in self.instances]
			if port not in ports:
				redis_process = Redis(self.master, self.persistence_type, port, password)
				self.instances.append(redis_process)
				self.creds[port] = password
				
	
	def start_all(self):	
		for redis in self.instances:
			redis.service.start()
	
	
	def stop_all(self, reason = None):
		for redis in self.instances:
			if redis.service.running:
				redis.service.stop(reason)
	
	
	def save_all(self):
		for redis in self.instances:
			if redis.service.running:
					redis.redis_cli.save()
					redis.service.stop('rebooting')
		
		
	def init_as_masters(self, mpoint):
		for redis in self.instances:
			redis.init_master(mpoint)
	
	
	def init_as_slaves(self, mpoint, primary_ip):
		for redis in self.instances:
			redis.init_slave(mpoint, primary_ip, redis.port)

	
	def wait_for_sync(self,link_timeout=None,sync_timeout=None):
		#consider using threads
		for redis in self.instances:
			redis.wait_for_sync(link_timeout,sync_timeout)
	
	
class Redis(BaseService):
	_instance = None
	service = None
	port = None
	password = None

	def __init__(self, master=False, persistence_type=SNAP_TYPE, port=DEFAULT_PORT, password = None):
		self._objects = {}
		self.service = initdv2.lookup(SERVICE_NAME)
		self.is_replication_master = master
		self.persistence_type = persistence_type		
		self.port = port		
		self.password = password or self.generate_password()		

	def init_master(self, mpoint):
		self.service.stop('Configuring master. Moving Redis db files')	
		self.init_service(mpoint, self.password)
		self.redis_conf.masterauth = None
		self.redis_conf.slaveof = None
		self.service.start()
		self.is_replication_master = True
		
	def init_slave(self, mpoint, primary_ip, primary_port):
		self.service.stop('Configuring slave')
		self.init_service(mpoint, self.password)
		self.change_primary(primary_ip, primary_port, self.password)
		self.service.start()
		self.is_replication_master = False
	
	def wait_for_sync(self,link_timeout=None,sync_timeout=None):
		LOG.info('Waiting for link with master')
		wait_until(lambda: self.redis_cli.master_link_status == 'up', sleep=3, timeout=link_timeout)
		LOG.info('Waiting for sync with master to complete')
		wait_until(lambda: not self.redis_cli.master_sync_in_progress, sleep=10, timeout=sync_timeout)
		LOG.info('Sync with master completed')
		
	def change_primary(self, primary_ip, primary_port):
		'''
		Currently redis slaves cannot use existing data to catch master
		Instead they create another db file while performing full sync
		Wchich may potentially cause free space problem on redis storage
		And broke whole initializing process.
		So scalarizr removing all existing data on initializing slave 
		to free as much storage space as possible.
		'''
		self.working_directory.empty()
		self.redis_conf.masterauth = self.password
		self.redis_conf.slaveof = (primary_ip, primary_port)
		
	def init_service(self, mpoint):
		move_files = not self.working_directory.is_initialized(mpoint)
		self.working_directory.move_to(mpoint, move_files)
		self.redis_conf.requirepass = self.password
		self.redis_conf.dir = mpoint
		self.redis_conf.bind = None
		self.redis_conf.port = self.port
		self.redis_conf.dbfilename = 'dump.%s.rdb' % self.port
		self.redis_conf.appendfilename = 'appendonly.%s.aof' % self.port 
		if self.persistence_type == SNAP_TYPE:
			self.redis_conf.appendonly = False
		elif self.persistence_type == AOF_TYPE:
			self.redis_conf.appendonly = True
			self.redis_conf.save = {}
		
	@property	
	def current_password(self):
		return self.redis_conf.requirepass
	
	@property
	def db_path(self):
		fname = self.redis_conf.dbfilename if not self.redis_conf.appendonly else self.redis_conf.appendfilename
		return os.path.join(self.redis_conf.dir, fname)
	
	def generate_password(self, length=20):
		return cryptotool.pwgen(length)	
	
	def _get_redis_conf(self):
		return self._get('redis_conf', RedisConf.find)
	
	def _set_redis_conf(self, obj):
		self._set('redis_conf', obj)	
	
	def _get_redis_cli(self):
		return self._get('redis_cli', RedisCLI.find, self.redis_conf)
	
	def _set_redis_cli(self, obj):
		self._set('redis_cli', obj)
	
	def _get_working_directory(self):
		return self._get('working_directory', WorkingDirectory.find, self.redis_conf)
		
	def _set_working_directory(self, obj):
		self._set('working_directory', obj)
		
	working_directory = property(_get_working_directory, _set_working_directory)
	redis_conf = property(_get_redis_conf, _set_redis_conf)
	redis_cli = property(_get_redis_cli, _set_redis_cli)
	
	
class WorkingDirectory(object):

	default_db_fname = DB_FILENAME
	
	def __init__(self, db_path=None, user = "redis"):
		self.db_path = db_path
		self.user = user
		
		
	@classmethod
	def find(cls, redis_conf):
		dir = redis_conf.dir
		if not dir:
			dir = DEFAULT_DIR_PATH
			
		db_fname = redis_conf.appendfilename if redis_conf.appendonly else redis_conf.dbfilename 
		if not db_fname:
			db_fname = cls.default_db_fname
		return cls(os.path.join(dir,db_fname))	
	

	def move_to(self, dst, move_files=True):
		new_db_path = os.path.join(dst, os.path.basename(self.db_path))
		
		if not os.path.exists(dst):
			LOG.debug('Creating directory structure for redis db files: %s' % dst)
			os.makedirs(dst)
		
		if move_files and os.path.exists(os.path.dirname(self.db_path)) and os.path.isfile(self.db_path):
			LOG.debug("copying db file %s into %s" % (os.path.dirname(self.db_path), dst))
			shutil.copyfile(self.db_path, new_db_path)
			
		LOG.debug("changing directory owner to %s" % self.user)	
		rchown(self.user, dst)			
		self.db_path = new_db_path
		return new_db_path


	def is_initialized(self, path):
		# are the redis db files already in place? 
		if os.path.exists(path):
			fnames = os.listdir(path)
			return os.path.basename(self.db_path) in fnames
		return False
	
	
	def empty(self):
		LOG.info('Emptying redis database dir %s' % os.path.dirname(self.db_path))
		try:
			for fname in os.listdir(os.path.dirname(self.db_path)):
				if fname.endswith('.rdb') or fname.startswith('appendonly'):
					path = os.path.join(os.path.dirname(self.db_path), fname)
					if os.path.isfile(path):
						LOG.debug('Deleting redis db file %s' % path)
						os.remove(path)
					elif os.path.islink(path):
						LOG.debug('Deleting link to redis db file %s' % path)
						os.unlink(path)						
		except OSError, e:
			LOG.error('Cannot empty %s: %s' % (os.path.dirname(self.db_path), e))
			
	
	
class BaseRedisConfig(BaseConfig):
	config_type = 'redis'
		
	def set(self, option, value, append=False):
		if not self.data:
			self.data = Configuration(self.config_type)
			if os.path.exists(self.path):
				self.data.read(self.path)
		if value:
			if append:
				self.data.add(option, value)
			else:
				self.data.set(option,value, force=True)
		else: 
			self.data.comment(option)
		if self.autosave:
			self.save_data()
			self.data = None
	
	def set_sequential_option(self, option, seq):
		is_typle = type(seq) is tuple
		try:
			assert seq is None or is_typle
		except AssertionError:
			raise ValueError('%s must be a sequence (got %s instead)' % (option, seq))	
		self.set(option, ' '.join(map(str,seq)) if is_typle else None)

	def get_sequential_option(self, option):
		raw = self.get(option)
		return raw.split() if raw else ()
				
	def get_list(self, option):
		if not self.data:
			self.data =  Configuration(self.config_type)
			if os.path.exists(self.path):
				self.data.read(self.path)	
		try:
			value = self.data.get_list(option)	
		except NoPathError:
			try:
				value = getattr(self, option+'_default')
			except AttributeError:
				value = ()
		if self.autosave:
			self.data = None
		return value
	
	def get_dict_option(self, option):
		raw = self.get_list(option)
		d = {}
		for raw_value in raw:
			k,v = raw_value.split()
			if k and v:
				d[k] = v
		return d
		
	def set_dict_option(self, option, d):
		try:
			assert d is None or type(d)==dict
			#cleaning up
			#TODO: make clean process smarter using indexes
			for i in self.get_list(option):
				self.set(option+'[0]', None)
				
			#adding multiple entries
			for k,v in d.items():
				val = ' '.join(map(str,'%s %s'%(k,v)))
				self.set(option, val, append=True)
		except ValueError:
			raise ValueError('%s must be a sequence (got %s instead)' % (option, d))			
	
	
class RedisConf(BaseRedisConfig):
	
	config_name = 'redis.conf'
	
	@classmethod
	def find(cls, config_dir=None, port=DEFAULT_PORT):
		default_conf_dir = UBUNTU_CONFIG_DIR if disttool.is_ubuntu() else CENTOS_CONFIG_DIR
		conf_name = 'redis.%s.conf' % port
		conf_path = os.path.join(default_conf_dir, conf_name)
		return cls(os.path.join(config_dir, conf_name) if config_dir else conf_path)
	
	def _get_dir(self):
		return self.get('dir')
	
	def _set_dir(self, path):
		self.set_path_type_option('dir', path)
	
	def _get_bind(self):
		return self.get_sequential_option('bind')
	
	def _set_bind(self, list_ips):
		self.set_sequential_option('bind', list_ips)
				
	def _get_slaveof(self):
		return self.get_sequential_option('slaveof')
	
	def _set_slaveof(self, conn_data):
		'''
		@tuple conndata: (ip,) or (ip, port)
		'''
		self.set_sequential_option('slaveof', conn_data)		
	
	def _get_masterauth(self):
		return self.get('masterauth')
	
	def _set_masterauth(self, passwd):
		self.set('masterauth', passwd)		
	
	def _get_requirepass(self):
		return self.get('requirepass')
	
	def _set_requirepass(self, passwd):
		self.set('requirepass', passwd)	
					
	def _get_appendonly(self):
		return True if self.get('appendonly') == 'yes' else False
	
	def _set_appendonly(self, on):
		assert on == True or on == False
		self.set('appendonly', 'yes' if on else 'no')	
	
	def _get_dbfilename(self):
		return self.get('dbfilename')
	
	def _set_dbfilename(self, fname):
		self.set('dbfilename', fname)	
	
	def _get_save(self):
		return self.get_dict_option('save')
	
	def _set_save(self, save_dict):
		self.set_dict_option('save', save_dict)
												
	def _get_pidfile(self):
		return self.get('pidfile')
	
	def _set_pidfile(self, pid):
		self.set('pidfile', pid)
														
	def _get_port(self):
		return self.get('port')
	
	def _set_port(self, number):
		self.set('port', number)
																
	def _get_logfile(self):
		return self.get('logfile')
	
	def _set_logfile(self, path):
		self.set('logfile', path)
																	
	def _get_appendfilename(self):
		return self.get('appendfilename')
	
	def _set_appendfilename(self, path):
		self.set('appendfilename', path)
		

	appendfilename = property(_get_appendfilename, _set_appendfilename) 						
	pidfile = property(_get_pidfile, _set_pidfile)
	port = property(_get_port, _set_port)
	logfile = property(_get_logfile, _set_logfile)
	dir = property(_get_dir, _set_dir)
	save = property(_get_save, _set_save)
	bind = property(_get_bind, _set_bind)
	slaveof = property(_get_slaveof, _set_slaveof)
	masterauth = property(_get_masterauth, _set_masterauth)
	requirepass	 = property(_get_requirepass, _set_requirepass)
	appendonly	 = property(_get_appendonly, _set_appendonly)
	dbfilename	 = property(_get_dbfilename, _set_dbfilename)
	dbfilename_default = DB_FILENAME
	appendfilename_default = AOF_FILENAME


class RedisCLI(object):
	
	port = None
	password = None
	path = REDIS_CLI_PATH
	
	
	class no_keyerror_dict(dict):
		def __getitem__(self, key):
			try:
				return dict.__getitem__(self, key)
			except KeyError:
				return None
	
	
	def __init__(self, password=None, port=DEFAULT_PORT):
		self.port = port
		self.password = password
		
		if not os.path.exists(self.path):
			raise OSError('redis-cli not found')
	
	
	@classmethod	
	def find(cls, redis_conf):
		return cls(redis_conf.requirepass)
		
		
	def execute(self, query):
		if not self.password:
			full_query = query
		else:
			full_query = 'AUTH %s\n%s' % (self.password, query)
		try:
			out = system2([self.path, '-p', self.port], stdin=full_query,silent=True)[0]
			
			#fix for redis 2.4 AUTH
			if 'Client sent AUTH, but no password is set' in out:
				out = system2([self.path], stdin=query,silent=True)[0]
				
			if out.startswith('ERR') or out.startswith('LOADING'):
				raise PopenError(out)
			
			elif out.startswith('OK\n'):
				out = out[3:]
			if out.endswith('\n'):
				out = out[:-1]
			return out	
		except PopenError, e:
			if 'LOADING' in str(e):
				LOG.debug('Unable to execute query %s: Redis is loading the dataset in memory' % query)
			else:
				LOG.error('Unable to execute query %s with redis-cli: %s' % (query, e))
			raise	
		
		
	def test_connection(self):
		try:
			self.execute('select (1)')
		except PopenError, e:
			if 'LOADING' in str(e):
				return False
		return True
			
	
	@property
	def info(self):
		info = self.execute('info')
		LOG.debug('Redis INFO: %s' % info)
		d = self.no_keyerror_dict()
		if info:
			for i in info.strip().split('\n'):
				raw = i[:-1] if i.endswith('\r') else i
				if raw:
					kv = raw.split(':')
					if len(kv)==2:
						key, val = kv
						if key:
							d[key] = val
		return d
	
	
	@property
	def aof_enabled(self):
		return True if self.info['aof_enabled']=='1' else False	
	
	
	@property
	def bgrewriteaof_in_progress(self):
		return True if self.info['bgrewriteaof_in_progress']=='1' else False	
	
	
	@property
	def bgsave_in_progress(self):
		return True if self.info['bgsave_in_progress']=='1' else False
	
	
	@property
	def changes_since_last_save(self):
		return int(self.info['changes_since_last_save'])
		
		
	@property
	def connected_slaves(self):
		return int(self.info['connected_slaves'])
		
		
	@property
	def last_save_time(self):
		return int(self.info['last_save_time'])
		
		
	@property
	def redis_version(self):
		return self.info['redis_version']
			
			
	@property
	def role(self):
		return self.info['role']


	@property
	def master_host(self):
		info = self.info
		if info['role']=='slave':
			return info['master_host']
		return None
		
		
	@property
	def master_port(self):
		info = self.info
		if info['role']=='slave':
			return int(info['master_port'])
		return None
		
		
	@property
	def master_link_status(self):
		info = self.info
		if info['role']=='slave':
			return info['master_link_status']
		return None
	
	
	def bgsave(self, wait_until_complete=True):
		if not self.bgsave_in_progress:
			self.execute('bgsave')
		if wait_until_complete:
			wait_until(lambda: not self.bgsave_in_progress, sleep=5, timeout=900)
			
			
	def bgrewriteaof(self, wait_until_complete=True):
		if not self.bgrewriteaof_in_progress:
			self.execute('bgrewriteaof')
		if wait_until_complete:
			wait_until(lambda: not self.bgrewriteaof_in_progress, sleep=5, timeout=900)
					
					
	def save(self):
		self.bgrewriteaof() if self.aof_enabled else self.bgsave()
		
		
	@property
	def master_last_io_seconds_ago(self):
		info = self.info
		if info['role']=='slave':
			return int(info['master_last_io_seconds_ago'])
		return None
		
		
	@property
	def master_sync_in_progress(self):
		info = self.info
		if info['role']=='slave':
			return True if info['master_sync_in_progress']=='1' else False
		return False
		
