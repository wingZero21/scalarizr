'''
Created on Aug 29, 2011

@author: marat
'''

from scalarizr import config
from scalarizr import handlers
from scalarizr import messaging
from scalarizr import storage
from scalarizr import util
from scalarizr.util import filetool
from scalarizr.bus import bus
from scalarizr.services import cloudfoundry


import logging
import os
import shutil

def get_handlers():
	return (MainHandler(), CloudControllerHandler(), MysqlHandler())
	#return (CloudFoundryHandler(), )


LOG = logging.getLogger(__name__)
SERVICE_NAME = 'cloudfoundry'
DEFAULTS = {
	'home': '/root/cloudfoundry/vcap',
	'datadir': '/var/vcap/data',
	'services': ''
}

'''
class CloudFoundryHandler(handlers.Handler, handlers.FarmSecurityMixin):
	
	def __init__(self):
		handlers.FarmSecurityMixin.__init__(self, [4222, 9022, 12345])
		bus.on(init=self.on_init)


	def on_init(self, *args, **kwds):
		LOG.debug('Called on_init')
		bus.on(
			reload=self.on_reload,
			start=self.on_start,
			host_init_response=self.on_host_init_response,
			before_host_up=self.on_before_host_up,
			before_reboot_start=self.on_before_reboot_start
		)
		self.on_reload()		


	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		LOG.debug('Accept %s [y/n]?', message.name)
		result = message.name in	(
				messaging.Messages.HOST_INIT, 
				messaging.Messages.HOST_DOWN,  
				messaging.Messages.HOST_UP,
				messaging.Messages.BEFORE_HOST_TERMINATE)
		LOG.debug('Yes of corsa!' if result else 'Nonono!')
		return result


	def on_reload(self):
		LOG.debug('Called on_reload')
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		self._ini = self._cnf.rawini

		# Apply configuration
		for key, value in DEFAULTS.iteritems():
			if self._ini.has_option(SERVICE_NAME, key):
				value = self._ini.get(SERVICE_NAME, key)
			setattr(self, key, value)

		# Read components, services and behaviours 
		class list_ex(list):
			def __setattr__(self, key, value):
				self.__dict__[key] = value
		
		self.components, self.services, self.bhs = [], [], list_ex()

		behaviour_str = self._ini.get('general', 'behaviour')
		for prop in dir(config.BuiltinBehaviours):
			if prop.startswith('CF'):
				bh = getattr(config.BuiltinBehaviours, prop)
				cmp = bh[3:]
				setattr(self.bhs, cmp, bh)
				if bh in behaviour_str:
					self.bhs.append(bh)
					self.components.append(cmp)

		# Init storage for cloud_controller
		self.volume_path = self._cnf.private_path('storage/cloudfoundry.json')
		self._volume_config = None				
		if self.is_scalarizr_running and self.is_cloud_controller:
			self._volume = storage.Storage.create(self.volume_config)

		# Init CloudFoundry manager
		self.cf = cloudfoundry.CloudFoundry(self.home)
		

	def _set_volume_config(self, cnf):
		volume_dir = os.path.dirname(self.volume_path)
		if not os.path.exists(volume_dir):
			os.makedirs(volume_dir)
		storage.Storage.backup_config(cnf, self.volume_path)

	
	def _get_volume_config(self):
		if not self._volume_config:
			self._volume_config = storage.Storage.restore_config(self.volume_path)
		return self._volume_config


	volume_config = property(_get_volume_config, _set_volume_config)


	@property
	def is_cloud_controller(self):
		return self.bhs.cloud_controller in self.bhs

	def is_service(self):
		return self.bhs.service in self.bhs

	@property
	def is_scalarizr_running(self):
		return self._cnf.state == config.ScalarizrState.RUNNING


	@property
	def local_ip(self):
		return self._platform.get_private_ip()


	def from_cloud_controller(self, msg):
		return self.bhs.cloud_controller in msg.behaviour
	

	def its_me(self, msg):
		return msg.remote_ip == self._platform.get_public_ip()

	@property
	def svss(self):
		return self.components + self.services

		
	def _start_services(self):
		self.cf.start(*self.svss)
		
		
	def _stop_services(self):
		self.cf.stop(*self.svss)
		
		
	def _setup_external_uri(self):
		if self.is_cloud_controller:
			roles = self._queryenv.list_roles(behaviour='www')
			if roles:
				self._os_hosts(roles[0].hosts[0].internal_ip, 
						self.cf.components['cloud_controller'].config['external_uri'])
	
		
	def _plug_storage(self, vol=None, mpoint=None):
		vol = vol or self.volume_config
		mpoint = mpoint or self.datadir
		if not hasattr(vol, 'id'):
			vol = storage.Storage.create(vol)

		try:
			if not os.path.exists(mpoint):
				os.makedirs(mpoint)
			if not vol.mounted():
				vol.mount(mpoint)
		except storage.StorageError, e:
			# XXX: Crapy. We need to introduce error codes from fstool 
			if 'you must specify the filesystem type' in str(e):
				vol.mkfs()
				vol.mount(mpoint)
			else:
				raise
		return vol


	def _os_hosts(self, ipaddr, hostname):
		hosts_file = '/etc/hosts'
		hosts_bck_file = hosts_file + '.bck'
		if not os.path.exists(hosts_bck_file):
			shutil.copy(hosts_file, hosts_bck_file)
			
		lines = open(hosts_file).readlines()
		newline = '%s %s\n' % (ipaddr, hostname)
		
		fp = open(hosts_file, 'w')
		updated = False
		for line in lines:
			if hostname in line:
				line = newline
				updated = True
			fp.write(line)
		if not updated:
			fp.write(newline)
		fp.close()
		
			
	def _locate_cloud_controller(self):
		util.wait_until(self._do_locate_cloud_controller, timeout=600, logger=LOG, 
					start_text='Locating cloud_controller server', 
					error_text='Cannot locate cloud_controller server')

		
	def _do_locate_cloud_controller(self):
		host = None
		if self.is_cloud_controller:
			host = self.local_ip
		else:
			roles = self._queryenv.list_roles(behaviour=self.bhs.cloud_controller)
			if roles:
				host = roles[0].hosts[0].internal_ip
		if host:
			self.cf.cloud_controller = host
		return bool(host)

	###
	### Event & message handlers
	###


	def on_start(self):
		LOG.debug('Called on_start')
		if self.is_scalarizr_running:
			self._setup_external_uri()
			self._plug_storage()
			self._start_services()
	

	def on_host_init_response(self, msg):
		LOG.debug('Called on_host_init_response')
		if self.is_cloud_controller:
			ini = msg.body.get(self.bhs.cloud_controller, {})
			self.volume_config = ini.pop('volume_config', 
										dict(type='loop',file='/mnt/cfdata.loop',size=50))
		if self.is_service:
			ini = msg.body.get(self.bhs.service, {})
			if 'mysql' in ini:
				
				pass
	
	def on_before_host_up(self, msg):
		LOG.debug('Called on_before_host_up')
		hostup = dict()
		
		if self.is_cloud_controller:
			# Initialize storage			
			LOG.info('Initializing vcap data storage')
			tmp_mpoint = '/mnt/tmp.vcap.data'
			try:
				self.volume = self._plug_storage(mpoint=tmp_mpoint)
				if not self.cf.valid_datadir(tmp_mpoint):
					LOG.debug('Syncing data from %s to storage', self.datadir)
					rsync = filetool.Rsync().archive().delete().\
								source(self.datadir + '/').dest(tmp_mpoint)
					rsync.execute()
					
				LOG.debug('Mounting storage to %s', self.datadir)
				self.volume.umount()
				self.volume.mount(self.datadir)
			finally:
				if os.path.exists(tmp_mpoint):
					os.removedirs(tmp_mpoint)		
			self.volume_config = self.volume.config()
		
		self._setup_external_uri()
		self._locate_cloud_controller()

		LOG.debug('Setting local route')
		cmps = self.cf.components
		cmps['dea'].local_route = \
		cmps['cloud_controller'].local_route = \
		cmps['health_manager'].local_route = self.local_ip

		if self.is_cloud_controller:
			self.cf.init_db()
			
		self._start_services()
			
		self._cnf.update_ini(SERVICE_NAME, hostup)
		msg.body[SERVICE_NAME] = hostup

		
	def on_HostUp(self, msg):
		LOG.debug('Called on_HostUp')
		if self.from_cloud_controller(msg) and not self.its_me(msg):
			self.cf.cloud_controller = msg.local_ip
		
		
	def on_before_reboot_start(self, msg):
		LOG.debug('Called on_before_reboot_start')
		self._stop_services()
		

	def on_BeforeHostTerminate(self, msg):
		LOG.debug('Called on_BeforeHostTerminate')
		if self.its_me(msg):
			self._stop_services()

'''




_cnf = _ini = _bhs = _queryenv = _platform = None
_cf = _components = _services = None
_home = _datadir = None


def is_cloud_controller():
	return _bhs.cloud_controller in _bhs

def is_service():
	return _bhs.service in _bhs

def is_scalarizr_running():
	return _cnf.state == config.ScalarizrState.RUNNING

def local_ip():
	return _platform.get_private_ip()

def from_cloud_controller(msg):
	return _bhs.cloud_controller in msg.behaviour

def its_me(msg):
	return msg.remote_ip == _platform.get_public_ip()

def svss():
	return _components + _services

class list_ex(list):
	def __setattr__(self, key, value):
		self.__dict__[key] = value

class MainHandler(handlers.Handler, handlers.FarmSecurityMixin):
	
	def __init__(self):
		handlers.FarmSecurityMixin.__init__(self, [4222, 9022, 12345])
		bus.on(init=self.on_init)
	
	def _init_globals(self):
		global _cnf, _ini, _bhs, _queryenv, _platform
		global _cf, _components, _services
		
		_queryenv = bus.queryenv_service
		_platform = bus.platform 

		# Apply configuration
		_cnf = bus.cnf		
		_ini = _cnf.rawini		
		for key, value in DEFAULTS.iteritems():
			if _ini.has_option(SERVICE_NAME, key):
				value = _ini.get(SERVICE_NAME, key)
			globals()['_' + key] = value
	

		_services = filter(None, _services.split(','))			
		_components, _bhs = [], list_ex()
		_cf = cloudfoundry.CloudFoundry(_home)				
				
		behaviour_str = _ini.get('general', 'behaviour')
		for prop in dir(config.BuiltinBehaviours):
			if prop.startswith('CF'):
				bh = getattr(config.BuiltinBehaviours, prop)
				cmp = bh[3:]
				setattr(_bhs, cmp, bh)
				if bh in behaviour_str:
					_bhs.append(bh)
					_components.append(cmp)
	


		
	def _start_services(self):
		_cf.start(*svss())
		
		
	def _stop_services(self):
		_cf.stop(*svss())
	
	
	def _locate_cloud_controller(self):
		util.wait_until(self._do_locate_cloud_controller, timeout=600, logger=LOG, 
					start_text='Locating cloud_controller server', 
					error_text='Cannot locate cloud_controller server')

		
	def _do_locate_cloud_controller(self):
		host = None
		if is_cloud_controller():
			host = local_ip()
		else:
			roles = _queryenv.list_roles(behaviour=_bhs.cloud_controller)
			if roles:
				host = roles[0].hosts[0].internal_ip
		if host:
			_cf.cloud_controller = host
		return bool(host)
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		LOG.debug('Accept %s [y/n]?', message.name)
		result = message.name in	(
				messaging.Messages.HOST_INIT, 
				messaging.Messages.HOST_DOWN,  
				messaging.Messages.HOST_UP,
				messaging.Messages.BEFORE_HOST_TERMINATE)
		LOG.debug('Yes of corsa!' if result else 'Nonono!')
		return result
	
	
	def on_init(self, *args, **kwds):
		LOG.debug('Called on_init')
		bus.on(
			reload=self.on_reload,
			start=self.on_start,
			before_host_up=self.on_before_host_up,
			before_reboot_start=self.on_before_reboot_start
		)
		self._init_globals()

	
	def on_reload(self, *args, **kwds):
		LOG.debug('Called on_reload')
		self._init_globals()

	def on_start(self):
		LOG.debug('Called on_start')
		if is_scalarizr_running():
			self._start_services()

	
	def on_before_host_up(self, msg):
		LOG.debug('Called on_before_host_up')
		
		self._locate_cloud_controller()

		LOG.debug('Setting local route')
		cmps = _cf.components
		cmps['dea'].local_route = \
		cmps['cloud_controller'].local_route = \
		cmps['health_manager'].local_route = local_ip()
			
		self._start_services()


	def on_HostUp(self, msg):
		LOG.debug('Called on_HostUp')
		if from_cloud_controller(msg) and not its_me(msg):
			_cf.cloud_controller = msg.local_ip


	def on_before_reboot_start(self, msg):
		LOG.debug('Called on_before_reboot_start')
		self._stop_services()
		

	def on_BeforeHostTerminate(self, msg):
		LOG.debug('Called on_BeforeHostTerminate')
		if self.its_me(msg):
			self._stop_services()


class CloudControllerHandler(handlers.Handler):
	LOG = logging.getLogger(__name__ + '.cloud_controller')
	
	def __init__(self):
		bus.on(init=self.on_init)
	
	def _set_volume_config(self, cnf):
		volume_dir = os.path.dirname(self.volume_path)
		if not os.path.exists(volume_dir):
			os.makedirs(volume_dir)
		if cnf:
			storage.Storage.backup_config(cnf, self.volume_path)
		self._volume_config = cnf			
	
	def _get_volume_config(self):
		if not self._volume_config:
			self._volume_config = storage.Storage.restore_config(self.volume_path)
		return self._volume_config


	volume_config = property(_get_volume_config, _set_volume_config)
	
	def _init_objects(self):
		self.volume_path = _cnf.private_path('storage/cloudfoundry.json')		
		self.volume_config = None
		self.volume = None

	
	def _os_hosts(self, ipaddr, hostname):
		hosts_file = '/etc/hosts'
		hosts_bck_file = hosts_file + '.bck'
		if not os.path.exists(hosts_bck_file):
			shutil.copy(hosts_file, hosts_bck_file)
			
		lines = open(hosts_file).readlines()
		newline = '%s %s\n' % (ipaddr, hostname)
		
		fp = open(hosts_file, 'w')
		updated = False
		for line in lines:
			if hostname in line:
				line = newline
				updated = True
			fp.write(line)
		if not updated:
			fp.write(newline)
		fp.close()
	
	def _setup_external_uri(self):
		roles = _queryenv.list_roles(behaviour='www')
		if roles:
			self._os_hosts(
				roles[0].hosts[0].internal_ip, 
				_cf.components['cloud_controller'].config['external_uri']
			)
	
	def _plug_storage(self, vol=None, mpoint=None):
		vol = vol or self.volume_config
		mpoint = mpoint or _datadir
		if not hasattr(vol, 'id'):
			vol = storage.Storage.create(vol)

		try:
			if not os.path.exists(mpoint):
				os.makedirs(mpoint)
			if not vol.mounted():
				vol.mount(mpoint)
		except storage.StorageError, e:
			''' XXX: Crapy. We need to introduce error codes from fstool ''' 
			if 'you must specify the filesystem type' in str(e):
				vol.mkfs()
				vol.mount(mpoint)
			else:
				raise
		return vol
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		LOG.debug('Accept %s [y/n]?', message.name)
		result = is_cloud_controller() and message.name in (messaging.Messages.BEFORE_HOST_UP) 
		LOG.debug('Yes of corsa!' if result else 'Nonono!')
		return result
		
	
	def on_init(self):
		if is_cloud_controller():
			bus.on(
				start=self.on_start,
				host_init_response=self.on_host_init_response,
				reload=self.on_reload
			)
			self._init_objects()


	def on_reload(self):
		self._init_objects()


	def on_start(self):
		if is_scalarizr_running():
			self._setup_external_uri()
			self._plug_storage()
			
	
	def on_host_init_response(self, msg):
		self.LOG.debug('Called on_host_init_response')
		ini = msg.body.get(_bhs.cloud_controller, {})
		self.volume_config = ini.pop('volume_config', 
									dict(type='loop',file='/mnt/cfdata.loop',size=50))

	
	def on_BeforeHostUp(self, msg):
		# Initialize storage			
		LOG.info('Initializing vcap data storage')
		tmp_mpoint = '/mnt/tmp.vcap.data'
		try:
			self.volume = self._plug_storage(mpoint=tmp_mpoint)
			if not self.cf.valid_datadir(tmp_mpoint):
				LOG.debug('Syncing data from %s to storage', _datadir)
				rsync = filetool.Rsync().archive().delete().\
							source(_datadir + '/').dest(tmp_mpoint)
				rsync.execute()
				
			LOG.debug('Mounting storage to %s', _datadir)
			self.volume.umount()
			self.volume.mount(_datadir)
		finally:
			if os.path.exists(tmp_mpoint):
				os.removedirs(tmp_mpoint)		
		self.volume_config = self.volume.config()

		self._setup_external_uri()		
		_cf.init_db()


class MysqlHandler(handlers.Handler):
	def on_BeforeHostUp(self, msg):
		# patch configs
		pass
