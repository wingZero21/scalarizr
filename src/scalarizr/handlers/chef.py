'''
Created on Oct 24, 2011

@author: marat
'''

import logging

from scalarizr.handlers import Handler
from scalarizr.bus import bus

import os

from scalarizr.util import system2
from scalarizr.util.software import whereis

from scalarizr.handlers import HandlerError #Handler, ServiceCtlHanler

import sqlite3

import string
import re

LOG = logging.getLogger(__name__)

def get_handlers():
	return (ChefHandler(), )


class ChefHandler(Handler):
	def __init__(self):
		bus.on(init=self.on_init)

	def on_init(self, *args, **kwds):
		bus.on(host_init_response=self.on_host_init_response)

	def on_host_init_response(self, message):
		'''
		input from scalr in message
		chef.server_url
		chef.node_name
		chef.client_key
		'''

		if not message.body.has_key("chef"):
			LOG.debug('\n\nchef tag in input message not found. Check input message\n\n')
			raise HandlerError("Error: HHostInitResponse message for Chef behaviour must have 'chef'"
				" propertyehaviour must have 'chef' property")
		try:
			chef_data = message.chef.copy()
			self._logger.debug("Update chef configs with %s"% chef_data)
			self._write_config(chef_data)

			#start chef-client:
			path2chef = whereis("chef-client")
			if not path2chef:
				raise HandlerError('Error: Not found chef-client')
				#TODO: HandlerError ?

			(out, err, returncode)=system2(path2chef, raise_exc = False)
			
			LOG.debug('\nerr: \n%s\nout: \n%s\nreturncode: \n%s' %
				(err, out, returncode))
			
			if returncode != 0:
				raise HandlerError("Error in chef-client, on HostInitResponse can't" 
					" start or connect to chef.")
		
		except Exception,e:
			
			if os.path.exists('/var/chef/cache/chef-stacktrace.out'):
					with open('/var/chef/cache/chef-stacktrace.out','r') as f:
						LOG.debug('\nChef-stacktrace :%s' % f.read())

			if os.path.exists('/var/chef/cache/failed-run-data.json'):
				with open('/var/chef/cache/failed-run-data.json','r') as f:
					LOG.debug('Error %s' % f.read())

			if os.path.exists('/etc/chef/client.pem'):
				os.remove('/etc/chef/client.pem')

			LOG.debug("\n\nException on HostInitResponse."
				". Details:%s\n" % e)

			#TODO: HandlerError or other?
			raise HandlerError("\nError on HostInitResponse Chef. Details:%s" % e)

	def _write_config(self, chef_data):
		'''input arg @chef_data: dict'''
		PATH = "/etc/chef"
		LOG.debug("\n\nserver_url: %s\nnod_name: %s\nclient_key: '%s'\n" %
				(chef_data.get('server_url'), chef_data.get('node_name'),
				chef_data.get('client_key')))
		try:
			if not os.path.exists(PATH):
				try:
					os.mkdir(PATH)
				except Exception, e:
					raise LookupError("Cant create folder, configs files not created."
						" Details: %s" % e)

			with open(PATH+"/client.rb", "w") as f:
				f.writelines([
					"log_level\t%s" % ':info\n',
					"log_location\t%s" % 'STDOUT\n',
					"chef_server_url\t'%s'\n" % chef_data['server_url'],
					"node_name\t'%s'\n" % chef_data['node_name']])

			with open(PATH+"/client.pem", "w") as f:
				f.write(chef_data['client_key'])

		except Exception, e:
			LOG.warn("\n Can't create configures files. Details: %s\n"%e)

	def on_host_up(self, msg):
		pass

'''
1 select * from db where =='HostInitResp'
2 recived and save in temp.xml
3
<body>
	<chef>
		<server_url>
		<node_name>
		<client_key>

szradm --msgsnd -o control -n HostInitResponse -f hir.xml -e http://0.0.0.0:8013
etc/scalr/private.d/db.sqlite '''	