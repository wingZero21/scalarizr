'''
Created on Dec 27, 2010

@author: spike
'''
from szr_integtest import RESOURCE_PATH, config, get_selenium
from szr_integtest_libs.scalrctl import ScalrCtl, FarmUI, SshManager, ui_login
from szr_integtest_libs.szrdeploy import ScalarizrDeploy
from szr_integtest_libs.ssh_tool import MutableLogFile, execute

from libcloud.types import Provider 
from libcloud.providers import get_driver 
from libcloud.base import NodeSize, NodeImage
from scalarizr.util.filetool import read_file
from scalarizr.util import wait_until
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

keys_path = os.path.expanduser('~/.scalr-dev/farm_keys/')

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

def merge_dicts(a,b):
	res = {}
	for key in a.keys():
		if not key in b:
			res[key] = a[key]
			continue
		
		if type(a[key]) != type(b[key]):
			res[key] = b[key]
		elif dict == type(a[key]):
			res[key] = merge_dicts(a[key], b[key])
		else:
			res[key] = b[key]
		del(b[key])
	
	res.update(b)
	return res

platform_config = read_json_config(platform_config_path)
# TODO: Temporary solution. Find a way to merge configurations properly (metaconf?)
if os.path.exists(_user_platform_cnf_path):
	user_platform_cnf = read_json_config(_user_platform_cnf_path)
	platform_config = merge_dicts(platform_config, user_platform_cnf)

class DataProvider(object):
	_instances  = {}
	_servers	= []
	conn		= None
	
	def __new__(cls, *args, **kwargs):
		key = tuple(zip(kwargs.iterkeys(), tuple([x if type(x) != dict else tuple(x.items()) for x in kwargs.itervalues()])))
		if not key in DataProvider._instances:
			DataProvider._instances[key] = super(DataProvider, cls).__new__(cls, *args, **kwargs)
		return DataProvider._instances[key]
	
	def __init__(self, behaviour='raw', farm_settings=None, scalr_srv_id=None, dist=None, **kwargs):
		
		def cleanup():
			self.clear()
		#atexit.register(cleanup)
		
		try:
			self.platform	= os.environ['PLATFORM']
			self.dist		= dist or os.environ['DIST']
		except:
			raise Exception("Can't get platform and dist from os environment.")
		
		self.driver = get_driver(getattr(Provider, self.platform.upper()))
		self.__credentials = platform_config['platrform_credentials']
		self.conn = self.driver(**self.__credentials)
		
		if scalr_srv_id:
			self.farm_id   = config.get('./test-farm/farm_id')
			self.farmui = FarmUI(get_selenium())
			self.farmui.use(self.farm_id)
			host = self.farmui.get_public_ip(scalr_srv_id, 60)
			self.role_name = self.farmui.get_role_name(scalr_srv_id)
			node = self._get_node(host)
			key = os.path.join(keys_path, '%s.pem' % self.role_name)
			if not os.path.exists(key):
				key = os.path.join(keys_path, 'farm-%s.pem' % self.farm_id) + '.pem'
			ssh = SshManager(host, key)
			self._servers.append(Server(node, ssh, role_name=self.role_name, scalr_id=scalr_srv_id))
			return
			
		self.default_size  = platform_config['default_size']

		self.behaviour	= behaviour
		self.farm_settings = farm_settings
		
		for configuration in platform_config['images'][self.dist][self.behaviour]:
			if set(kwargs.items()) <= set(configuration.items()):
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
					self.ssh_config = dict(key=config.get('./test-farm/farm_key'))
					'''
					self.key_path  = config.get('./test-farm/farm_key')
					'''
					self.farmui = FarmUI(get_selenium())
					ui_login(self.farmui.sel)
					self.farmui.use(self.farm_id)
					self.scalrctl = ScalrCtl(self.farm_id)
					self.server_id_re = re.compile('\[FarmID:\s+%s\].*?%s\s+scaling\s+\up.*?ServerID\s+=\s+(?P<server_id>[\w-]+)'
													% (self.farm_id, self.role_name), re.M)
				break
		else:
			raise Exception('No suitable configuration found. Params: %s' % kwargs)				
			
	def server(self, index=0):
		while len(self._servers) < (index + 1):
			self._scale_up()
		return self._servers[index]
		
	def _scale_up(self):
		
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
			ssh = SshManager(host, key, key_pass, password, timeout=240)
			self._servers.append(Server(node, ssh, image_id=self.image_id.id))
		else:
			if self.farmui.state == 'terminated':
				self.farmui.use(self.farm_id)
				self.farmui.remove_all_roles()
				self.farmui.save()
				self.farmui.add_role(self.role_name, settings=self.farm_settings)
				self.farmui.save()
				self.farmui.launch()
			"""
			else:
				self.farmui.use(self.farm_id)
				self.edit_role(self.farm_settings)
				self.farmui.save()
			"""
			"""
			elif self.farm_settings:
				self.farmui.edit_role(self.role_name, settings=self.farm_settings)
				self.farmui.save()
			"""
					
			out = self.scalrctl.exec_cronjob('Scaling')
			result = re.search(self.server_id_re, out)
			if not result:
				raise Exception("Can't create server - farm '%s' hasn't been scaled up." % self.farm_id)
			server_id = result.group('server_id')
			host = self.farmui.get_public_ip(server_id)
			node = self._get_node(host)
			key = os.path.join(keys_path, self.role_name) + '.pem'
			self.wait_for_szr_port(host)
			time.sleep(5)
			ssh = SshManager(host, key, timeout=240)
			self._servers.append(Server(node, ssh, role_name = self.role_name, scalr_id=server_id))
		return self._servers[-1]
			
	def clear(self):
		if hasattr(self, 'farmui'):
			try:
				self.farmui.terminate()
			except (Exception, BaseException),e:
				if not 'has been already terminated' in str(e):
					raise				
		if self._servers:
			for srv in self._servers:
				srv.terminate()
			
	def sync(self):
		pass
	
	def _get_node(self, ip):
		for instance in self.conn.list_nodes():
			public_ip = instance.public_ip
			inst_ip = socket.gethostbyname(public_ip if type(public_ip) == str else public_ip[0])
			if inst_ip == ip:
				return instance
		else:
			raise Exception("Can't find node with public ip '%s'" % ip)
	
	def wait_for_szr_port(self, host):
		def _check_szr_port(host):
			try:
				socket.socket().connect((host, 8013))
				return True
			except:
				return False				
		wait_until(_check_szr_port, [host], sleep=5, timeout=240)
	
	
	def wait_for_hostup(self, server):
		self.wait_for_szr_port(server.public_ip)
		log_reader = server.log.head()
		log_reader.expect("Message 'HostInit' delivered", 120)						
		self.scalrctl.exec_cronjob('ScalarizrMessaging')
		log_reader.expect("Message 'HostUp' delivered", 120)
		self.scalrctl.exec_cronjob('ScalarizrMessaging')
		
	def edit_role(self, new_settings):
		self.farmui.use(self.farm_id)
		try:
			self.farmui.edit_role(self.role_name, new_settings)
		except (Exception, BaseException), e:
			if not "doesn't have role" in str(e):
				raise
			self.farmui.add_role(self.role_name, settings = new_settings)
		self.farmui.save()				

class Server(object):
	_log_channel = None
	_dist 		 = None
	
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
	
	def sftp(self):
		if not self.ssh_manager.connected:
			self.ssh_manager.connect()
		return self.ssh_manager.get_sftp_client()
	
	def terminate(self):
		if not self.node.destroy():
			raise Exception("Failed to terminate instance.")
		
	@property
	def public_ip(self):
		hostname = self.node.public_ip
		return socket.gethostbyname(hostname[0] if type(hostname) == list else hostname)
	
	@property
	def private_ip(self):
		hostname = self.node.private_ip
		return hostname[0] if type(hostname) == list else hostname
	
	@property
	def log(self):
		if not hasattr(self, '_log'):
			self._log_channel = self.ssh()
			self._log = MutableLogFile(self._log_channel)
		return self._log
		
	@property
	def cloud_id(self):
		return self.node.id
	
	def get_message(self, message_id=None, message_name=None):
		if not hasattr(self, '_control_ssh'):
			self._control_ssh = self.ssh()
		sqlite3_installed = execute(self._control_ssh, 'which sqlite3', 15)
		if not sqlite3_installed:
			self.install_software('sqlite3')
		if message_id:
			cmd = '\'select message from p2p_message where message_id="%s" limit 1\'' % message_id
		elif message_name:
			cmd = '\'select message from p2p_message where message_name="%s" limit 1\'' % message_name
		return execute(self._control_ssh, 'sqlite3 /etc/scalr/private.d/db.sqlite ' + cmd, 10)		
	
	def install_software(self, software):
		if not hasattr(self, '_control_ssh'):
			self._control_ssh = self.ssh()
		if self.dist == 'debian':
			out = execute(self._control_ssh, 'apt-get -y install %s' % software, 240)
			error = re.search('^E:\s*(?P<err_text>.+?)$', out, re.M)
			if error:
				raise Exception("Can't install %s: '%s'" % (software, error.group('err_text')))		
		else:
			out = execute(self._control_ssh, 'yum -y install %s' % software, 240)
			if not re.search('Complete!|Nothing to do', out):
				raise Exception('Cannot install %s: %s' % (software, out))
			
	@property	
	def dist(self):
		if not self._dist:
			szrdeploy = ScalarizrDeploy(self.ssh_manager)
			self._dist = szrdeploy.dist
			del(szrdeploy)
		return self._dist
		