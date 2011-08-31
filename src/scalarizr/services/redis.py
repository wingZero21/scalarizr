'''
Created on Aug 10, 2011

@author: Dmytro Korsakov
'''

import os
import logging
import shutil

from scalarizr.bus import bus
from scalarizr.util import initdv2, system2, PopenError, wait_until
from scalarizr.services import lazy, BaseConfig, BaseService
from scalarizr.util import disttool, cryptotool
from scalarizr.util.filetool import rchown
from scalarizr.libs.metaconf import Configuration


SERVICE_NAME = CNF_SECTION = 'redis'
BIN_PATH 	 = '/usr/bin/redis-server'	
OPT_REPLICATION_MASTER  = "replication_master"
CONFIG_PATH = '/etc/redis/redis.conf'
REDIS_CLI_PATH = '/usr/bin/redis-cli'	
REDIS_USER = 'redis'	
	
SNAP_TYPE = 'snapshotting'
AOF_TYPE = 'aof'
				
class RedisInitScript(initdv2.ParametrizedInitScript):
	socket_file = None
	
	@lazy
	def __new__(cls, *args, **kws):
		obj = super(RedisInitScript, cls).__new__(cls, *args, **kws)
		cls.__init__(obj)
		return obj
			
	def __init__(self):
		initdv2.ParametrizedInitScript.__init__(self, name=SERVICE_NAME, 
				initd_script='/etc/init.d/redis-server')
	
	@property
	def _processes(self):
		args = ('ps', '-G', 'redis', '-o', 'command', '--no-headers')	
		try:	
			p = [x for x in system2(args, silent=True)[0].split('\n') if x and BIN_PATH + ' ' + CONFIG_PATH in x]
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
	
	
initdv2.explore(SERVICE_NAME, RedisInitScript)
	
	
class Redis(BaseService):
	_instance = None
	service = None

	def __init__(self, master=False, persistence_type=SNAP_TYPE):
		self._objects = {}
		self.service = initdv2.lookup(SERVICE_NAME)
		self._logger = logging.getLogger(__name__)
		self.is_replication_master = master
		self.persistence_type = persistence_type
								
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(Redis, cls).__new__(
								cls, *args, **kwargs)
		return cls._instance				

	def init_master(self, mpoint, password=None):
		self.service.stop('Configuring master. Moving Redis db files')
		move_files = not self.working_directory.is_initialized(mpoint)
		self.working_directory.move_to(mpoint, move_files)		
		self.init_service(mpoint)
		self.redis_conf.masterauth = None
		self.redis_conf.slaveof = None
		self.redis_conf.requirepass = password or self.generate_password()
		self.service.start()
		self.is_replication_master = True
		
	def init_slave(self, mpoint, primary_ip, primary_port, password):
		rchown(REDIS_USER, mpoint)
		self.service.stop('Configuring slave')
		self.init_service(mpoint)
		self.redis_conf.requirepass = None
		self.change_primary(primary_ip, primary_port, password)
		self.service.start()
		self.is_replication_master = False
		
	def change_primary(self, primary_ip, primary_port, password):
		self.working_directory.empty()
		self.redis_conf.masterauth = password
		self.redis_conf.slaveof = (primary_ip, primary_port)
		
	def init_service(self, mpoint):
		self.redis_conf.bind = None
		self.redis_conf.dir = mpoint
		if self.persistence_type == SNAP_TYPE:
			pass
		elif self.persistence_type == AOF_TYPE:
			pass
		
	@property	
	def password(self):
		return self.redis_conf.requirepass if self.is_replication_master else self.redis_conf.masterauth
	
	def generate_password(self, length=20):
		return cryptotool.pwgen(length)	
	
	def _get_redis_conf(self):
		return self._get('redis_conf', RedisConf.find)
	
	def _set_redis_conf(self, obj):
		self._set('redis_conf', obj)
	
	def _get_working_directory(self):
		return self._get('working_directory', WorkingDirectory.find, self.redis_conf)
		
	def _set_working_directory(self, obj):
		self._set('working_directory', obj)
		
	working_directory = property(_get_working_directory, _set_working_directory)
	redis_conf = property(_get_redis_conf, _set_redis_conf)
	
	
class WorkingDirectory(object):
	
	default_centos_path = default_ubuntu_path = '/var/lib/redis'
	
	def __init__(self, path=None, user = "redis"):
		self.path = path
		self.user = user
		self._logger = logging.getLogger(__name__)
		
	@classmethod
	def find(cls, redis_conf):
		path = redis_conf.dir
		if not path:
			path = cls.default_ubuntu_path if disttool.is_ubuntu() else cls.default_centos_path
		return cls(path)

	def move_to(self, dst, move_files=True):
		if not os.path.exists(dst):
			self._logger.debug('Creating directory structure for redis db files: %s' % dst)
			os.makedirs(dst)
		
		if move_files and os.path.exists(self.path) and os.listdir(self.path):
			self._logger.debug("copying db files from %s into %s" % (self.path, dst))
			data = os.listdir(self.path)
			for fname in data:
				if fname in ('dump.rdb','appendonly.log'):
					shutil.copyfile(os.path.join(self.path, fname), os.path.join(dst, fname))
		self._logger.debug("changing directory owner to %s" % self.user)	
		rchown(self.user, dst)			
		self.path = dst
		return dst

	def is_initialized(self, path):
		# are the redis db files already in place? 
		if os.path.exists(path):
			fnames = os.listdir(path)
			return 'dump.rdb' in fnames or 'appendonly.log' in fnames
		return False
	
	def empty(self):
		self._logger.info('Emptying redis database dir %s' % self.path)
		try:
			for fname in os.listdir(self.path):
				if fname.endswith('.rdb'):
					path = os.path.join(self.path, fname)
					if os.path.isfile(path):
						self._logger.debug('Deleting redis db file %s' % path)
						os.remove(path)
					elif os.path.islink(path):
						self._logger.debug('Deleting link to redis db file %s' % path)
						os.unlink(path)						
		except OSError, e:
			self._logger.error('Cannot empty %s: %s' % (self.path, e))
				
	
	
class BaseRedisConfig(BaseConfig):
	config_type = 'redis'
	
	def set_sequential_option(self, option, seq):
		try:
			assert not seq or type(seq)==tuple
			self.set(option, ' '.join(map(str,seq)) if seq else None)
		except ValueError:
			raise ValueError('%s must be a sequence (got %s instead)' % (option, seq))	

	def get_sequential_option(self, option):
		raw = self.get(option)
		return raw.split() if raw else ()
	
	
class RedisConf(BaseRedisConfig):
	
	config_name = 'redis.conf'
	
	@classmethod
	def find(cls, config_dir=None):
		return cls(os.path.join(config_dir, cls.config_name) if config_dir else CONFIG_PATH)
		
	def set(self, option, value):
		if not self.data:
			self.data = Configuration(self.config_type)
			if os.path.exists(self.path):
				self.data.read(self.path)
		if value:
			self.data.set(option,value, force=True)
		else: 
			self.data.comment(option)
		if self.autosave:
			self.save()
			self.data = None
				
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
				
	dir = property(_get_dir, _set_dir)
	bind = property(_get_bind, _set_bind)
	slaveof = property(_get_slaveof, _set_slaveof)
	masterauth = property(_get_masterauth, _set_masterauth)
	requirepass	 = property(_get_requirepass, _set_requirepass)
		
		
class RedisCLI(object):
	path = REDIS_CLI_PATH
	
	def __init__(self, password=None):
		if not os.path.exists(self.path):
			raise OSError('redis-cli not found')
		
	def execute(self, query):
		if self.password:
				query = 'AUTH %s\n%s' % (self.passwword, query)
		try:
			out = system2([self.path], stdin=query,silent=True)[0]
			if out.startswith('ERR'):
				raise PopenError(out)
			elif out.startswith('OK\n'):
				out = out[3:]
			if out.endswith('\n'):
				out = out[:-1]
			return out	
		except PopenError, e:
			self._logger.error('Unable to execute query %s with redis-cli: %s' % (query, e))
			raise	
	
	@property
	def info(self):
		info = self.execute('info')
		d = {}
		if info:
			for i in info.strip().split('\n'):
				key, val = i[:-1].split(':')
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
		
