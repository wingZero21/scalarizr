'''
Created on Aug 29, 2011

@author: marat
'''

from scalarizr import config
from scalarizr import handlers
from scalarizr import messaging
from scalarizr.bus import bus
from scalarizr.services import cloudfoundry



def get_handlers():
	return (CloudFoundryHandler(), )


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
				or	message.name == messaging.Messages.HOST_DOWN)


	def on_reload(self):
		self.platform = bus.platform
		self.cnf = bus.cnf
		self.ini = self.cnf.rawini
		self.cloudfoundry = cloudfoundry.CloudFoundry(self.ini.get(SERVICE_NAME, 'vcap_home'))
		self.components = [bh[3:] for bh in config.split(self.ini.get('general', 'behaviour')) 
						if bh.startswith('cf_')]
		
		#if self.cnf.private_exists(SERVICE_NAME):
		#	self.components = config.split(self.ini.get(SERVICE_NAME, 'components'))
		#	self.services = config.split(self.ini.get(SERVICE_NAME, 'services'))


	def on_start(self):
		if self.cnf.state == config.ScalarizrState.RUNNING:
			self._start_services()


	def on_host_init_response(self, msg):
		if SERVICE_NAME in msg.body:
			ini = msg.body[SERVICE_NAME].copy()
			#self.components = ini['components']
			#ini['components'] = ','.join(ini['components'])
			#ini['services'] = ','.join(ini['services'])
			self.cnf.update_ini(SERVICE_NAME, ini)
			#self.cnf.update_ini(SERVICE_NAME, {'services': ini['services']}, private=False)
		else:
			raise handlers.HandlerError("Property '%s' in 'HostInitResponse' message is undefined", 
									SERVICE_NAME)
	
	
	def on_before_host_up(self, msg):
		hostup = dict()
		
		if 'cloud_controller' in self.components:
			self.cloudfoundry.mbus_url = hostup['mbus_url'] = \
					'mbus://%s:4222/' % self.platform.get_private_ip()

		self._start_services()		
			
		self.ini.update_ini(SERVICE_NAME, hostup)
		msg.body[SERVICE_NAME] = hostup
		
		
	def _start_services(self):
		for cmp in self.components + self.services:
			self.cloudfoundry.start(cmp)
		# @todo check that all of them finally running

	
	def _stop_services(self):
		for cmp in self.components + self.services:
			self.cloudfoundry.stop(cmp)

