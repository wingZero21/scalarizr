'''
Created on Dec 23, 2009

@author: marat
'''

class QueryEnvError(BaseException):
	pass

class QueryEnvService(object):
	_logger = None
	_service_url = None
	_api_version = None
	_farm_id = None
	_instance_id = None
	_key = None
	_key_id = None
	
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
		
	def _request (self, params={}, response_reader=None):
		"""
		@return object
		"""
		pass
	
	def _remove_whitespace_nodes(self, parent):
		for child in list(parent.childNodes):
			if child.nodeType==child.TEXT_NODE and child.data.strip()=='':
				parent.removeChild(child)
			else:
				self._remove_whitespace_nodes(child)	
		
	def _read_list_roles_response(self, xml):
		ret = []
		print("raw>> ")
		print(xml.documentElement.toxml())
		self._remove_whitespace_nodes(xml.documentElement)
		print("cleared>> ")
		print(xml.documentElement.toxml())
		
		response = xml.documentElement
		
		for rolesEl in response.childNodes:
			for roleEl in rolesEl.childNodes:
				role = Role()
				role.behaviour = roleEl.getAttribute("behaviour")
				role.name = roleEl.getAttribute("name")
				for hostsEL in roleEl.childNodes:
					for hostEL in hostsEL.childNodes:
						host = RoleHost()
						host.replication_master = hostEL.getAttribute("replication-master")
						host.internal_ip = hostEL.getAttribute("internal-ip")
						host.external_ip = hostEL.getAttribute("external-ip")
						role.hosts.append(host)
				ret.append(role)

		return ret
	
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
	
	