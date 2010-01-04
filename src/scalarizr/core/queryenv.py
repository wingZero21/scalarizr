'''
Created on Dec 23, 2009

@author: Dmytro Korsakov
'''
import logging
from scalarizr.messaging import MessageProducer, Message, MessagingError

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
		self._logger = logging.getLogger(__package__)
		self._service_url = service_url
		self._farm_id = farm_id
		self._instance_id = instance_id
		self._key = key
		self._key_id = key_id
		self._api_version = api_version
	
	def set_keys(self, key_id, key):
		self._key_id = key_id
		self._key = key
	
	def list_roles (self, role_name=None, behaviour=None):
		"""
		@return Role[]
		"""
		parametres = {}
		if None != role_name :
			parametres["role"] = role_name
		if None != behaviour:
			parametres["behaviour"] = behaviour
			
		return self._request("list-roles",parametres, self._read_list_roles_response)
	
	def list_role_params(self, role_name=None):
		"""
		@return dict
		"""
		parametres = {}
		if None != role_name :
			parametres["role"] = role_name
		return self._request("list-role-params",parametres, self._read_list_role_params_response)
	
	def list_scripts (self, event=None, asynchronous=None, name=None):
		"""
		@return Script[]
		"""
		parametres = {}
		if None != event :
			parametres["event"] = event
		if None != asynchronous:
			parametres["asynchronous"] = asynchronous
		if None != name :
			parametres["name"] = name
		return self._request("list-scripts",parametres, self._read_list_scripts_response)
	
	def list_virtual_hosts (self, name=None, https=None):
		"""
		@return VirtualHost[]
		"""
		parametres = {}
		if None != name :
			parametres["name"] = name
		if None != https:
			parametres["https"] = https
		return self._request("list-virtualhosts",parametres, self._read_list_virtualhosts_response)
	
	def get_https_certificate (self):
		"""
		@return (cert, pkey)
		"""
		return self._request("get-https-certificate",{}, self._read_get_https_certificate_response)
	
	def list_ebs_mountpoints (self):
		"""
		@return Mountpoint[]
		"""
		return self._request("list-ebs-mountpoints",{}, self._read_list_ebs_mountpoints_response)
	
	def get_latest_version (self):
		"""
		@return string
		"""
		return self._request("get-latest-version",{}, self._read_get_latest_version_response)
		
	def _get_canonical_string (self, params={}):
		"""
		@return string
		"""
		s = ""
		for key, value in sorted(params.items()):
			s = s + str(key) + str(value)
		#print s
		return s
		
	def _sign (self, canonical_string, key):
		"""
		@return: string
		"""
		import hmac
		import hashlib
		import binascii
		
		digest = hmac.new(key, canonical_string, hashlib.sha1).digest()
		sign = binascii.b2a_base64(digest)
		if sign.endswith('\n'):
			sign = sign[:-1]
		return sign
	
	def _request (self, command, params={}, response_reader=None):
		"""
		@return object
		"""
		import time
		import urllib
		import urllib2
		from xml.dom.minidom import parseString
		
		
		request_body = {}
		request_body["operation"] = command
		request_body["version"] = self._api_version
		request_body["KeyID"] = self._key_id 
		request_body["farmid"] = self._farm_id 
		request_body["instanceid"] = self._instance_id
		if {} != params :
			for key, value in params.items():
				request_body[key] = value
				
		url = self._service_url + self._api_version + '/' + command
		timestamp = self._get_http_timestamp()
		data = self._get_canonical_string(request_body) 
		data += timestamp
		#try:
		signature = self._sign(data, self._key)
		post_data = urllib.urlencode(request_body)
		headers = {"Date": timestamp, "X-Signature": signature}
		#print "canonical string + timestamp = ", data
		print "post data = ", post_data
		#print "post url  = ", url
		#print "base64 signature = ", signature
		#print "headers = ", headers
		req = urllib2.Request(url, post_data, headers)
		response = urllib2.urlopen(req)
		#print "Info>>> ", response.info()
		#print "URL>>> ", response.geturl()
		# create xml, handle errors
		#try:
		xml = parseString(response.read())
		return response_reader(xml)
		#except URLError, e:
			#print "Caught: " + e

			
	def _get_http_timestamp(self):
		import time
		import datetime
		return time.strftime("%a %d %b %Y %H:%M:%S %Z", time.gmtime())
	
		
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
		print "list_roles_response (xml)>> ", response.toxml()
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
		print "list_ebs_mountpoints_response (xml)>> ", response.toxml()
		
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
		print "list_scripts_response (xml)>> ", response.toxml()
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
		print "list_role_params_response (xml)>> ", response.toxml()
		for param_el in response.firstChild.childNodes:
			ret[param_el.getAttribute("name")] = param_el.firstChild.firstChild.nodeValue
				
		return ret
	
	def _read_get_latest_version_response(self, xml):
		version = ""
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		print "get_latest_version_response (xml)>> ", response.toxml()
		version = response.firstChild.firstChild.nodeValue
		return version
	
	def _read_get_https_certificate_response(self, xml):
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		print "get_https_certificate_response (xml)>> ", response.toxml()
		if len(response.childNodes):
			cert = response.firstChild.firstChild.nodeValue
			pkey = response.lastChild.firstChild.nodeValue
			return (cert, pkey)
		
		return (None, None)	

	def _read_list_virtualhosts_response(self, xml):
		ret = []
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		print "list_virtualhosts_response (xml)>> ", response.toxml()
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
	def __repr__(self):
		return "name = " + str(self.name) \
	+ "; dir = " + str(self.dir) \
	+ "; create_fs = " + str(self.create_fs) \
	+ "; is_array = " + str(self.is_array) \
	+ "; volumes = " + str(self.volumes)
	
class Volume(object):
	volume_id  = None
	device = None
	def __repr__(self):
		return 'volume_id = ' + str(self.volume_id) \
	+ "; device = " + str(self.device)
		
class Role(object):
	behaviour = None
	name = None
	hosts = []
	def __repr__(self):
		return 'behaviour = ' + str(self.behaviour) \
	+ "; name = " + str(self.name) \
	+ "; hosts = " + str(self.hosts) + ";"

class RoleHost(object):
	index = None
	replication_master = False
	internal_ip = None
	external_ip	= None
	def __repr__(self):
		return "index = " + str(self.index) \
	+ "; replication_master = " + str(self.replication_master) \
	+ "; internal_ip = " + str(self.internal_ip) \
	+ "; external_ip = " + str(self.external_ip)
	
class Script(object):
	asynchronous = False
	exec_timeout = None 
	name = None
	body = None
	def __repr__(self):
		return "asynchronous = " + str(self.asynchronous) \
	+ "; exec_timeout = " + str(self.exec_timeout) \
	+ "; name = " + str(self.name) \
	+ "; body = " + str(self.body)
	
class VirtualHost(object):
	hostname = None
	type = None
	raw = None
	https = False
	def __repr__(self):
		return "hostname = " + str(self.hostname) \
	+ "; type = " + str(self.type) \
	+ "; raw = " + str(self.raw) \
	+ "; https = " + str(self.https)
