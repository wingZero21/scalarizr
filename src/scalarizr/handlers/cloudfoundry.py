'''
Created on Aug 29, 2011

@author: marat
'''

from scalarizr import config
from scalarizr import handlers
from scalarizr import messaging
from scalarizr import util
from scalarizr.bus import bus
from scalarizr.services import cloudfoundry

import logging


def get_handlers():
	return (CloudFoundryHandler(), )


LOG = logging.getLogger(__name__)
SERVICE_NAME = 'cloudfoundry'
BEHAVIOURS = [getattr(config.BuiltinBehaviours, bh) 
			for bh in dir(config.BuiltinBehaviours) 
			if bh.startswith('CF_')] 


class CloudFoundryHandler(handlers.Handler, handlers.FarmSecurityMixin):
	
	def __init__(self):
		handlers.FarmSecurityMixin.__init__(self, [4222, 12345])
		self.on_reload()


	def on_init(self, *args, **kwds):
		bus.on(
			reload=self.on_reload,
			start=self.on_start,
			host_init_response=self.on_host_init_response,
			before_host_up=self.on_before_host_up
		)


	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return set(BEHAVIOURS).intersection(set(behaviour)) and (
					message.name == messaging.Messages.HOST_INIT
				or	message.name == messaging.Messages.HOST_DOWN
				or  message.name == messaging.Messages.HOST_UP)


	def on_HostUp(self, msg):
		if msg.remote_ip != self.platform.get_public_ip() \
				and config.BuiltinBehaviours.CF_CLOUD_CONTROLLER in msg.behaviour:
			cchost = msg.local_ip or msg.remote_ip
			self.cloudfoundry.cloud_controller = cchost
		

	def on_reload(self):
		self.queryenv = bus.queryenv_service
		self.platform = bus.platform
		self.cnf = bus.cnf
		self.ini = self.cnf.rawini
		self.cloudfoundry = cloudfoundry.CloudFoundry(self.ini.get(SERVICE_NAME, 'vcap_home'))
		self.components = [bh[3:] for bh in config.split(self.ini.get('general', 'behaviour')) 
						if bh.startswith('cf_')]
		self.services = []


	def on_start(self):
		if self.cnf.state == config.ScalarizrState.RUNNING:
			self._start_services()


	def on_host_init_response(self, msg):
		if SERVICE_NAME in msg.body:
			ini = msg.body[SERVICE_NAME].copy()
			self.cnf.update_ini(SERVICE_NAME, ini)
		else:
			raise handlers.HandlerError("Property '%s' in 'HostInitResponse' message is undefined", 
									SERVICE_NAME)
	
	
	def on_before_host_up(self, msg):
		hostup = dict()
		self._locate_cloud_controller()
		if 'router' in self.components:
			self._configure_router()
		self._start_services()		
			
		self.ini.update_ini(SERVICE_NAME, hostup)
		msg.body[SERVICE_NAME] = hostup
		
		
	def _start_services(self):
		for cmp in self.components + self.services:
			LOG.info('Starting %s', cmp)
			self.cloudfoundry.start(cmp)
		# @todo check that all of them finally running

	
	def _stop_services(self):
		for cmp in self.components + self.services:
			LOG.info('Stopping %s', cmp)
			self.cloudfoundry.stop(cmp)


	def _locate_cloud_controller(self):
		util.wait_until(self.__locate_cloud_controller, logger=LOG, 
					start_text='Locating cloud_controller server', 
					error_text='Cannot locate cloud_controller server')

		
	def __locate_cloud_controller(self):
		cchost = None
		if 'cloud_controller' in self.components:
			cchost = self.platform.get_private_ip()
		else:
			roles = self.queryenv.list_roles(behaviour=config.BuiltinBehaviours.CF_CLOUD_CONTROLLER)
			if roles.hosts:
				cchost = roles.hosts[0].internal_ip
		if cchost:
			self.cloudfoundry.cloud_controller = cchost
		return bool(cchost)


	def _configure_router(self):
		roles = self.queryenv.list_roles(behaviour=config.BuiltinBehaviours.WWW)
		if roles:
			pass