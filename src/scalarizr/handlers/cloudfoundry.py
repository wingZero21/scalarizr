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


def get_handlers():
	return (CloudFoundryHandler(), )


LOG = logging.getLogger(__name__)
SERVICE_NAME = 'cloudfoundry'
BEHAVIOURS = [getattr(config.BuiltinBehaviours, bh) 
			for bh in dir(config.BuiltinBehaviours) 
			if bh.startswith('CF_')] 

DEFAULTS = {
	'home': '/root/cloudfoundry',
	'datadir': '/var/vcap/data'
}

class CloudFoundryHandler(handlers.Handler, handlers.FarmSecurityMixin):
	
	def __init__(self):
		handlers.FarmSecurityMixin.__init__(self, [4222, 9022, 12345])
		bus.on(init=self.on_init)
		self.on_reload()

	def on_init(self, *args, **kwds):
		LOG.debug('Called on_init')
		bus.on(
			reload=self.on_reload,
			start=self.on_start,
			host_init_response=self.on_host_init_response,
			before_host_up=self.on_before_host_up,
			before_reboot_start=self.on_before_reboot_start
		)


	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return set(BEHAVIOURS).intersection(set(behaviour)) and (
					message.name == messaging.Messages.HOST_INIT
				or	message.name == messaging.Messages.HOST_DOWN
				or  message.name == messaging.Messages.HOST_UP
				or	message.name == messaging.Messages.BEFORE_HOST_TERMINATE)


	def on_HostUp(self, msg):
		LOG.debug('Called on_HostUp')
		if msg.remote_ip != self._platform.get_public_ip() \
				and config.BuiltinBehaviours.CF_CLOUD_CONTROLLER in msg.behaviour:
			cchost = msg.local_ip or msg.remote_ip
			self.cf.cloud_controller = cchost
		

	def on_BeforeHostTerminate(self, msg):
		LOG.debug('Called on_BeforeHostTerminate')
		# Apply configuration defaults:
		if msg.remote_ip == self._platform.get_public_ip():
			self._stop_services()


	def on_reload(self):
		LOG.debug('Called on_reload')
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		self._cnf = bus.cnf
		self._ini = self._cnf.rawini
		self._volume_config = None
		self.volume_path = self._cnf.private_path('storage/cloudfoundry.json')
		if self.szr_running:
			self._init_volume()

		# Apply configuration
		for key, value in DEFAULTS.iteritems():
			if self._ini.has_option(SERVICE_NAME, key):
				value = self._ini.get(SERVICE_NAME, key)
			setattr(self, key, value)
		
		self.cf = cloudfoundry.CloudFoundry(self.home)
		self.components = [bh[3:] for bh in config.split(self._ini.get('general', 'behaviour')) 
						if bh.startswith('cf_')]
		self.services = []
		

	def _set_volume_config(self, cnf):
		storage.Storage.backup_config(cnf, self.volume_path)

	
	def _get_volume_config(self):
		if not self._volume_config:
			self._volume_config = storage.Storage.restore_config(self.volume_path)
		return self._volume_config


	volume_config = property(_get_volume_config, _set_volume_config)


	@property
	def szr_running(self):
		return self._cnf.state == config.ScalarizrState.RUNNING


	###
	### Event & message handlers
	###


	def on_start(self):
		LOG.debug('Called on_start')
		if self.szr_running:
			self._plug_storage()
			self._start_services()
	

	def on_host_init_response(self, msg):
		LOG.debug('Called on_host_init_response')
		if SERVICE_NAME in msg.body:
			ini = msg.body[SERVICE_NAME].copy()
			self.volume_config = ini.pop('volume_config', 
								dict(type='loop', 
									file='/mnt/cfdata.loop', 
									size=50))
			self._cnf.update_ini(SERVICE_NAME, ini)
		#else:
		#	raise handlers.HandlerError("Property '%s' in 'HostInitResponse' message is undefined", 
		#							SERVICE_NAME)
	
	
	def on_before_host_up(self, msg):
		LOG.debug('Called on_before_host_up')
		hostup = dict()
		
		# Initialize storage
		LOG.info('Initializing vcap data storage')
		tmp_mpoint = '/mnt/tmp.vcap.data'
		self.volume = self._plug_storage()
		if not self.cf.valid_datadir(tmp_mpoint):
			LOG.debug('Syncing data from %s to storage', self.datadir)
			rsync = filetool.Rsync().archive().delete().\
						source(self.datadir, tmp_mpoint)
			rsync.execute()
			
		LOG.debug('Mount storage to %s', self.datadir)
		self.volume.umount()
		self.volume.mount(self.datadir)
		
		self._locate_cloud_controller()

		LOG.debug('Setting local route')
		cmps = self.cf.components
		cmps['dea'].local_route = \
		cmps['cloud_controller'].local_route = \
		cmps['health_manager'].local_route = self._platform.get_private_ip()

		self.cf.init_db()
		self._start_services()
			
		self._cnf.update_ini(SERVICE_NAME, hostup)
		msg.body[SERVICE_NAME] = hostup
		
		
	def on_before_reboot_start(self, msg):
		LOG.debug('Called on_before_reboot_start')
		self._stop_services()
		
		
	def _start_services(self):
		svss = self.components + self.services
		self.cf.start(*svss)
		
		
	def _stop_services(self):
		svss = self.components + self.services
		self.cf.stop(*svss)
		
		
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
			''' XXX: Crapy. We need to introduce error codes from fstool ''' 
			if 'you must specify the filesystem type' in str(e):
				vol.mkfs()
				vol.mount(mpoint)
			else:
				raise
		return vol

		
	def _locate_cloud_controller(self):
		util.wait_until(self.__locate_cloud_controller, timeout=600, logger=LOG, 
					start_text='Locating cloud_controller server', 
					error_text='Cannot locate cloud_controller server')

		
	def __locate_cloud_controller(self):
		cchost = None
		if 'cloud_controller' in self.components:
			cchost = self._platform.get_private_ip()
		else:
			roles = self._queryenv.list_roles(behaviour=config.BuiltinBehaviours.CF_CLOUD_CONTROLLER)
			if roles:
				cchost = roles[0].hosts[0].internal_ip
		if cchost:
			self.cf.cloud_controller = cchost
		return bool(cchost)

