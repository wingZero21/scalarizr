'''
Created on Dec 23, 2009

@author: marat
'''

class QueryEnvError(BaseException):
	pass

class QueryEnvService(object):
	_logger
	_service_url
	_api_version
	_farm_id
	_instance_id
	_key
	_key_id
	
	def __init__(self, service_url, farm_id, instance_id, key=None, key_id=None, api_version="2009-03-05"):
		pass
	
	def set_keys(self, key_id, key):
		pass
	
	def list_roles (self, role_name=None, behaviour=None):
		"""
		@return Role[]
		"""
		pass
	
	def list_role_params(self, role_name):
		"""
		@return dict
		"""
		pass
	
	def list_scripts (self, event, asynchronous=None, name=None):
		"""
		@return Script[]
		"""
		pass
	
	def list_virtual_hosts (self, name=None, https=None):
		"""
		@return VirtualHost[]
		"""
		pass
	
	def get_https_certificate (self):
		"""
		@return (cert, pkey)
		"""
		pass
	
	def list_ebs_mountpoints (self):
		"""
		@return Mountpoint[]
		"""
		pass
	
	def get_latest_version (self):
		"""
		@return string
		"""
		pass
		
	def _get_canonical_string (self, params={}):
		"""
		@return string
		"""
		pass
	
	def _sign (self, canonical_string, key):
		"""
		@return: string
		"""
		self._request({}, self._read_list_role_params_response)
		pass
		
	def _request (self, params={}, response_reader):
		"""
		@return object
		"""
		pass
		
	def _read_list_roles_response(self, xml):
		pass
	
	def _read_list_role_params_response(self, xml):
		pass
	
	
class Role(object):
	behaviour = None
	name = None
	hosts = []

class RoleHost(object):
	replication_master = False
	internal_ip = None
	external_ip	= None
	
	