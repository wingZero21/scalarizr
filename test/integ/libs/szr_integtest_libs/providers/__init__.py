'''
Created on Dec 27, 2010

@author: spike
'''
from szr_integtest import RESOURCE_PATH, config, get_selenium, MutableLogFile
from scalarizr.util.filetool import read_file
from libcloud.types import Provider 
from libcloud.providers import get_driver 
from libcloud.base import NodeSize, NodeImage
from szr_integtest_libs.scalrctl import ScalrCtl, FarmUI, SshManager
import json
import os
import re
import time
import atexit

platform = os.environ['PLATFORM']
platform_config_path = os.path.join(RESOURCE_PATH, platform + '.json')

if not os.path.exists(platform_config_path):
	raise Exception('Config file for platform "%s" does not exist.' % platform)

raw_config = read_file(platform_config_path)
try:
	platform_config = json.loads(raw_config)
except:
	raise Exception('Config file for platform "%s" does not contain valid json configuration.')


class DataProvider(object):
	_instances  = {}
	_server		= None
	conn		= None
	
	def __new__(cls, *args, **kwargs):
		key = tuple(kwargs.items())
		if not key in DataProvider._instances:
			DataProvider._instances[key] = super(DataProvider, cls).__new__(cls, *args, **kwargs)
		return DataProvider._instances[key]
	
	def __init__(self, behaviour='raw', **kwargs):
		
		try:
			self.platform	= os.environ['PLATFORM']
			self.dist		= os.environ['DIST']
		except:
			raise Exception("Can't get platform and dist from os environment.")
		
		self.driver = get_driver(getattr(Provider, self.platform.upper()))
		self.__credentials = platform_config['platrform_credentials']
		self.default_size  = platform_config['default_size']

		self.behaviour	= behaviour
		
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
		
		if not self.conn:
			self.conn = self.driver(*self.__credentials)

		if self.behaviour == 'raw':
			if not isinstance(self.default_size, NodeSize):
				self.default_size = NodeSize(id=self.default_size, name="", \
											 ram=None, disk=None, bandwidth=None, price=None, driver="")										
			if not isinstance(self.image_id, NodeImage):
				self.image_id = NodeImage(self.image_id, name="", driver="")
						
			if self.ssh_config.get('keypair'):
				node = self.conn.create_node(name='Integtest-' + time.strftime('%Y_%m_%d-%H_%M') , \
										image=self.image_id, size=self.default_size, ex_keyname=self.ssh_config.get('keypair'))
				
			else:
				node = self.conn.create_node(name='Integtest-' + time.strftime('%Y_%m_%d-%H_%M') , image=self.image_id, size=self.default_size)
			
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
			key  = self.ssh_config.get('key')
			key_pass = self.ssh_config.get('key_pass')
			password = node.__dict__.get('password')
			ssh = SshManager(host, key, key_pass, password)
			ssh.connect()
			self._server = Server(node, self.image_id.id, ssh = ssh)
		else:
			if self.farmui.state == 'terminated':
				self.farmui.remove_all_roles()
				# FIXME: Откуда получать настройки фермы?
				self.farmui.add_role(self.role_name)
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
			ssh.connect()
			self._server = Server(node, role_name = self.role_name, ssh = ssh)
		return self._server
			
	@atexit.register
	def clear(self):
		if hasattr(self, 'farmui'):
			try:
				self.farmui.terminate()
			except (Exception, BaseException),e:
				if not 'has been already terminated' in str(e):
					raise				
		if self._server:
			self._server.destroy()
			
	def sync(self):
		pass


class Server(object):
	_log_channel = None
	
	def __init__(self, node, image_id=None, role_name=None, ssh):
		self.image_id = image_id
		self.role_name = role_name
		self.ssh_manager = ssh
		self.node = node
	
	def ssh(self):
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
		return self.node.public_ip[0]
	
	@property
	def log(self):
		if not hasattr(self, '_log'):
			self._log_channel = self.ssh()
			self._log = MutableLogFile(self._log_channel)
		return self._log.head()
		