'''
Created on Dec 27, 2010

@author: spike
'''
from szr_integtest import RESOURCE_PATH, config, get_selenium
from szr_integtest_libs.scalrctl import ScalrCtl, FarmUI, SshManager
from szr_integtest_libs.szrdeploy import ScalarizrDeploy
from szr_integtest_libs.ssh_tool import MutableLogFile, execute

from libcloud.types import Provider 
from libcloud.providers import get_driver 
from libcloud.base import NodeSize, NodeImage
from scalarizr.util.filetool import read_file
from itertools import chain

import json
import os
import re
import time
import atexit
import socket



platform = os.environ['PLATFORM']
platform_config_path = os.path.join(RESOURCE_PATH, platform + '.json')
_user_platform_cnf_path = os.path.expanduser('~/.scalr-dev/' + platform + '.json' )	

if not os.path.exists(platform_config_path):
	raise Exception('Config file for platform "%s" does not exist.' % platform)

def read_json_config(cnf_path):
	
	def convert_dict_from_unicode(data):
		if isinstance(data, unicode):
			return str(data)
		elif isinstance(data, dict):
			return dict(map(convert_dict_from_unicode, data.iteritems()))
		elif isinstance(data, (list, tuple, set, frozenset)):
			return type(data)(map(convert_dict_from_unicode, data))
		else:
			return data
		
	raw_config = read_file(cnf_path)

	try:
		config = convert_dict_from_unicode(json.loads(raw_config))
	except:
		raise Exception('Config file "%s" does not contain valid json configuration.' % cnf_path)

	return config

platform_config = read_json_config(platform_config_path)
# TODO: Temporary solution. Find a way to merge configurations properly (metaconf?)
if os.path.exists(_user_platform_cnf_path):
	user_platform_cnf = read_json_config(_user_platform_cnf_path)
	platform_config.update(user_platform_cnf)

class DataProvider(object):
	_instances  = {}
	_server		= None
	conn		= None
	
	def __new__(cls, *args, **kwargs):
		key = tuple(zip(kwargs.iterkeys(), tuple([x if type(x) != dict else tuple(x.items()) for x in kwargs.itervalues()])))
		print key		
		if not key in DataProvider._instances:
			DataProvider._instances[key] = super(DataProvider, cls).__new__(cls, *args, **kwargs)
		return DataProvider._instances[key]
	
	def __init__(self, behaviour='raw', farm_settings=None, **kwargs):
		
		def cleanup():
			self.clear()
		#atexit.register(cleanup)
		
		try:
			self.platform	= os.environ['PLATFORM']
			self.dist		= os.environ['DIST']
		except:
			raise Exception("Can't get platform and dist from os environment.")
		
		self.driver = get_driver(getattr(Provider, self.platform.upper()))
		self.__credentials = platform_config['platrform_credentials']
		self.conn = self.driver(**self.__credentials)
		self.default_size  = platform_config['default_size']

		self.behaviour	= behaviour
		self.farm_settings = farm_settings
		
		for configuration in platform_config['images'][self.dist][self.behaviour]:
			if set(kwargs) <= set(configuration):
				if self.behaviour == 'raw':
					self.image_id 	= configuration['image_id']
					self.ssh_config = platform_config['ssh']
					'''
					self.key_name	= config.get('./boto-ec2/key_name')
					self.key_path	= config.get('./boto-ec2/key_path')
					self.ssh_key_password = config.get('./boto-ec2/ssh_key_password')
					'''
				else:
					self.role_name = configuration['role_name']
					self.farm_id   = config.get('./test-farm/farm_id')
					self.ssh_config = {}
					self.ssh_config['key'] = config.get('./test-farm/farm_key')
					'''
					self.key_path  = config.get('./test-farm/farm_key')
					'''
					self.farmui = FarmUI(get_selenium())
					self.farmui.use(self.farm_id)
					self.scalrctl = ScalrCtl(self.farm_id)
					self.server_id_re = re.compile('\[FarmID:\s+%s\].*?%s\s+scaling\s+\up.*?ServerID\s+=\s+(?P<server_id>[\w-]+)'
													% (self.farm_id, self.role_name), re.M)
				break
		else:
			raise Exception('No suitable configuration found. Params: %s' % kwargs)				
			
	def server(self):
		if self._server:
			return self._server

		if self.behaviour == 'raw':
			if not isinstance(self.default_size, NodeSize):
				self.default_size = NodeSize(id=self.default_size, name="", \
											 ram=None, disk=None, bandwidth=None, price=None, driver="")										
			if not isinstance(self.image_id, NodeImage):
				self.image_id = NodeImage(self.image_id, name="", driver="")
			
			kwargs = {'name' : 'Integtest-' + time.strftime('%Y_%m_%d-%H_%M') , 'image' : self.image_id, 'size' : self.default_size}
			# Set keypair for Amazon AWS
			if self.ssh_config.get('keypair'):
				kwargs['ex_keyname'] = self.ssh_config.get('keypair')
			# Set security group
			if self.ssh_config.get('security_group'):
				kwargs['ex_securitygroup'] = self.ssh_config.get('security_group')
			# Set authorized_keys for rackspace
			if self.ssh_config.get('ssh_pub_key') and self.ssh_config.get('ssh_private_key'):
				pub_key_path = self.ssh_config.get('ssh_pub_key')
				pub_key = read_file(pub_key_path)
				kwargs['ex_files'] = {'/root/.ssh/authorized_keys' : pub_key, '/root/authorized_keys' : pub_key}
			
			node = self.conn.create_node(**kwargs)

			ip_retrieved = False
			while not ip_retrieved:
				for instance in self.conn.list_nodes():
					if instance.uuid == node.uuid and instance.public_ip[0]:
						node = instance
						ip_retrieved = True
						break
				else:
					time.sleep(5)					
			
			host = node.public_ip[0]
			key  = self.ssh_config.get('key') or self.ssh_config.get('ssh_private_key')
			key_pass = self.ssh_config.get('key_pass')
			password = node.__dict__.get('password')
			ssh = SshManager(host, key, key_pass, password)
			self._server = Server(node, ssh, image_id=self.image_id.id)
		else:
			if self.farmui.state == 'terminated':
				self.farmui.remove_all_roles()
				# FIXME: Where farm settings are?
				self.farmui.add_role(self.role_name, self.farm_settings)
				self.farmui.launch()
				
			out = self.scalrctl.exec_cronjob('Scaling')
			result = re.search(self.server_id_re, out)
			if not result:
				raise Exception("Can't create server - farm '%s' hasn't been scaled up." % self.farm_id)
			server_id = result.group('server_id')
			host = self.farmui.get_public_ip(server_id)
			for instance in self.conn.list_nodes():
				if instance.public_ip[0] == host:
					node = instance
					break
			else:
				raise Exception("Can't find node with public ip '%s'" % host)
					
			key  = self.ssh_config.get('key')
			ssh = SshManager(host, key)
			self._server = Server(node, ssh, role_name = self.role_name, scalr_id=server_id)
		return self._server
			
	def clear(self):
		if hasattr(self, 'farmui'):
			try:
				self.farmui.terminate()
			except (Exception, BaseException),e:
				if not 'has been already terminated' in str(e):
					raise				
		if self._server:
			self._server.terminate()
			
	def sync(self):
		pass


class Server(object):
	_log_channel = None
	
	def __init__(self, node, ssh, image_id=None, role_name=None, scalr_id=None):
		self.image_id = image_id
		self.role_name = role_name
		self.ssh_manager = ssh
		self.node = node
		self.scalr_id = scalr_id
	
	def ssh(self):
		if not self.ssh_manager.connected:
			self.ssh_manager.connect()
		return self.ssh_manager.get_root_ssh_channel()
	
	'''
	def run(self):
		pass
	'''
	
	def terminate(self):
		if not self.node.destroy():
			raise Exception("Failed to terminate instance.")
		
	@property
	def public_ip(self):
		return socket.gethostbyname(self.node.public_ip[0])
	
	@property
	def log(self):
		if not hasattr(self, '_log'):
			self._log_channel = self.ssh()
			self._log = MutableLogFile(self._log_channel)
		return self._log.head()
		
	@property
	def cloud_id(self):
		return self.node.id
	
	def get_message(self, message_id):
		if not hasattr(self, '_control_ssh'):
			self._control_ssh = self.ssh()
		sqlite3_installed = execute(self._control_ssh, 'which sqlite3', 15)
		if not sqlite3_installed:
			self._install_sqlite()
		cmd = 'select message from p2p_message where message_id="%s"' % message_id
		return execute(self._control_ssh, 'sqlite3 /etc/scalr/private.d/db.sqlite ' + cmd, 10)		
	
	def _install_sqlite(self):
		szrdeploy = ScalarizrDeploy(self.ssh_manager)
		dist = szrdeploy.dist
		del(szrdeploy)
		if dist == 'debian':
			out = execute(self._control_ssh, 'apt-get -y install sqlite3', 240)
			error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
			if error:
				raise Exception("Can't install sqlite3 package: '%s'" % error.group('err_text'))		
		else:
			out = execute(self._control_ssh, 'yum -y install sqlite3', 240)
			if not re.search('Complete!|Nothing to do', out):
				raise Exception('Cannot install sqlite3 %s' % out)
		