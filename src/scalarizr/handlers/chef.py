'''
Created on Oct 24, 2011

@author: marat
'''

import logging
import os

from scalarizr.bus import bus
from scalarizr.util import system2
from scalarizr.util.software import whereis
from scalarizr.handlers import Handler, HandlerError
from scalarizr.externals.chef.api import ChefAPI
try:
	import json
except ImportError:
	import simplejson as json


LOG = logging.getLogger(__name__)
CLIENT_CONF_TPL = '''
log_level        :info
log_location     STDOUT
chef_server_url  '%(server_url)s'
validation_client_name '%(validator_name)s'
'''


def get_handlers():
	return (ChefHandler(), )


class ChefHandler(Handler):
	def __init__(self):
		bus.on(init=self.on_init)
		self.on_reload()

	def on_init(self, *args, **kwds):
		bus.on(
			host_init_response=self.on_host_init_response,
			before_host_up=self.on_before_host_up,
			reload=self.on_reload
		)

	def on_reload(self):
		try:
			self._chef_client_bin = whereis('chef-client')[0]
		except IndexError:
			raise HandlerError('chef-client not found')
		try:
			self._ohai_bin = whereis('ohai')
		except IndexError:
			raise HandlerError('ohai not found')
		self._chef_data = None
		self._client_conf_path = '/etc/chef/client.rb'
		self._validator_key_path = '/etc/chef/validation.pem'
		self._client_key_path = '/etc/chef/client.pem'


	def on_host_init_response(self, message):
		if 'chef' in message.body:
			self._chef_data = message.chef.copy()


	def on_before_host_up(self, msg):
		if not self._chef_data:
			return
		
		try:
			# Create client configuration
			dir = os.path.dirname(self._client_conf_path)
			if not os.path.exists(dir):
				os.makedirs(dir)
			with open(self._client_conf_path, 'w+') as fp:
				fp.write(CLIENT_CONF_TPL % self._chef_data)
			os.chmod(self._client_conf_path, 0644)
				
			# Write validation cert
			with open(self._validator_key_path, 'w+') as fp:
				fp.write(self._chef_data['validator_key'])
				
			# Register node
			LOG.info('Registering Chef node')
			try:
				self.run_chef_client()
			finally:
				os.remove(self._validator_key_path)
				
			LOG.debug('Initializing Chef API client')
			node_name = self._chef_data['node_name'] = self.get_node_name()
			chef = ChefAPI(self._chef_data['server_url'], self._client_key_path, node_name)
			
			LOG.debug('Loading node')
			node = chef['/nodes/%s' % node_name]
			
			LOG.debug('Updating run_list')
			node['run_list'] = [u'role[%s]' % self._chef_data['role']] 
			chef.api_request('PUT', '/nodes/%s' % node_name, data=node)
				
			LOG.info('Applying run_list')
			self.run_chef_client()
			
			msg.chef = self._chef_data
			
		finally:
			self._chef_data = None
		
		
	def run_chef_client(self):
		system2([self._chef_client_bin])


	def get_node_name(self):
		cloud = json.loads(system2([self._ohai_bin, 'cloud'])[0])
		return cloud[0][1]


