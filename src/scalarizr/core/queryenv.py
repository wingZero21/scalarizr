'''
Created on Dec 23, 2009

@author: Dmytro Korsakov
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
		
	def _get_canonical_string (self, params={}):
		"""
		@return string
		"""
		s = ""
		for key, value in sorted(params.items()):
			s = s + str(key) + str(value)
		return s
		
	def _sign (self, canonical_string, key):
		"""
		@return: string
		"""
		import hmac
		digestmod=hashlib.sha1()
		h = hmac.new(key, canonical_string, digestmod)
		sign = h.hexdigest()
		self._request({}, self._read_list_role_params_response)
		return sign
	
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
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		
		for role_el in response.firstChild.childNodes:
			role = Role()
			role.behaviour = role_el.getAttribute("behaviour")
			role.name = role_el.getAttribute("name")
			for host_el in role_el.firstChild.childNodes:
				host = RoleHost()
				host.index = int(host_el.getAttribute("index"))
				if host_el.hasAttribute("replication-master"):
				    host.replication_master = bool(int(host_el.getAttribute("replication-master")))
				host.internal_ip = host_el.getAttribute("internal-ip")
				host.external_ip = host_el.getAttribute("external-ip")
				role.hosts.append(host)
				
			ret.append(role)

		return ret
	
	
	def _read_list_ebs_mountpoints_response(self, xml):
		import string
		ret = []
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		
		for mountpoint_el in response.firstChild.childNodes:
			mountpoint = Mountpoint()
			mountpoint.name = mountpoint_el.getAttribute("name")
			mountpoint.dir = mountpoint_el.getAttribute("dir")
			mountpoint.create_fs = bool(int(mountpoint_el.getAttribute("createfs")))
			mountpoint.is_array = bool(int(mountpoint_el.getAttribute("isarray")))
			for volume_el in mountpoint_el.firstChild.childNodes:
				volume = Volume()
				volume.volume_id = volume_el.getAttribute("volume-id")
				volume.device = volume_el.getAttribute("device")
				mountpoint.volumes.append(volume)
				
			ret.append(mountpoint)

		return ret
	
	
	def _read_list_scripts_response(self, xml):
		ret = []
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		
		for script_el in response.firstChild.childNodes:
			script = Script()
			script.asynchronous = bool(int(script_el.getAttribute("asynchronous")))
			script.exec_timeout = int(script_el.getAttribute("exec-timeout"))
			script.name = script_el.getAttribute("name")
			script.body = script_el.firstChild.firstChild.nodeValue
			ret.append(script)		
		return ret
	
	def _read_list_role_params_response(self, xml):
		ret = {}
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		
		for param_el in response.firstChild.childNodes:
			ret[param_el.getAttribute("name")] = param_el.firstChild.firstChild.nodeValue
				
		return ret
	
	def _get_latest_version_response(self, xml):
		version = ""
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		version = response.firstChild.firstChild.nodeValue
		return version
	
	def _get_https_certificate_response(self, xml):
		
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		cert = response.firstChild.firstChild.nodeValue
		pkey = response.lastChild.firstChild.nodeValue
		certificate = (cert, pkey)
		return certificate

	def _read_list_virtualhosts_response(self, xml):
		ret = []
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		
		for vhost_el in response.firstChild.childNodes:
			vhost = VirtualHost()
			vhost.hostname = vhost_el.getAttribute("hostname")
			vhost.type = vhost_el.getAttribute("type")
			vhost.raw = vhost_el.firstChild.firstChild.nodeValue
			if vhost_el.hasAttribute("https"):
				vhost.https = bool(int(vhost_el.getAttribute("https")))
			ret.append(vhost)		
		return ret
	
class Mountpoint(object):
	name = None
	dir = None
	create_fs = False
	is_array = False
	volumes  = []
	
class Volume(object):
	volume_id  = None
	device = None
		
class Role(object):
	behaviour = None
	name = None
	hosts = []

class RoleHost(object):
	index = None
	replication_master = False
	internal_ip = None
	external_ip	= None
	
class Script(object):
	asynchronous = False
	exec_timeout = None 
	name = None
	body = None
	
class VirtualHost(object):
	hostname = None
	type = None
	raw = None
	https = False
