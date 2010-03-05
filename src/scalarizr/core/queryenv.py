'''
Created on Dec 23, 2009

@author: Dmytro Korsakov
'''
import logging
import time
from urllib2 import urlopen, Request, URLError, HTTPError
from urllib import urlencode, splitnport
from xml.dom.minidom import parseString
import hmac
import hashlib
import binascii


class QueryEnvError(Exception):
	pass

class QueryEnvService(object):
	_logger = None
	_service_url = None
	_api_version = None
	_key = None
	_server_id = None
	
	def __init__(self, service_url, server_id=None, key=None, api_version="2009-03-05"):
		self._logger = logging.getLogger(__name__)
		self._service_url = service_url
		self._key = key
		self._server_id = server_id
		self._api_version = api_version
	
	def set_server_id(self, server_id):
		self._server_id = server_id
	
	def set_key(self, key):
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
		return s
		
	def _sign (self, canonical_string, key):
		"""
		@return: string
		"""
		digest = hmac.new(key, canonical_string, hashlib.sha1).digest()
		sign = binascii.b2a_base64(digest)
		if sign.endswith('\n'):
			sign = sign[:-1]
		return sign
	
	def _request (self, command, params={}, response_reader=None):
		"""
		@return object
		"""
	
		# Perform HTTP request
		request_body = {}
		request_body["operation"] = command
		request_body["version"] = self._api_version
		if {} != params :
			for key, value in params.items():
				request_body[key] = value
				
		url = self._service_url
		timestamp = self._get_http_timestamp()
		data = self._get_canonical_string(request_body) 
		data += timestamp
		signature = self._sign(data, self._key)
		post_data = urlencode(request_body)
		headers = {"Date": timestamp, "X-Signature": signature, "X-Server-Id": self._server_id}
		response = None
		try:
			req = Request(url, post_data, headers)
			response = urlopen(req)
		except URLError, e:
			if isinstance(e, HTTPError):
				resp_body = e.read() if not e.fp is None else ""
				if e.code == 401:
					raise QueryEnvError("Cannot authenticate on QueryEnv server. %s" % (resp_body))
				elif e.code == 400:
					raise QueryEnvError("Malformed request. %s" % (resp_body))
				elif e.code == 500:
					raise QueryEnvError("QueryEnv failed. %s" % (resp_body))
				else:
					raise QueryEnvError("Request to QueryEnv server failed (code: %d). %s" % (e.code, str(e)))
			else:
				host, port = splitnport(req.host, 80)
				raise QueryEnvError("Cannot connect to QueryEnv server on %s:%s. %s" 
						% (host, port, str(e)))

		# Parse XML response
		xml = None
		try:
			xml = parseString(response.read())
		except (TypeError, AttributeError), e:
			raise QueryEnvError("Cannot parse XML. %s" % (str(e)))
		return response_reader(xml)

			
	def _get_http_timestamp(self):
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
	
	def _read_get_latest_version_response(self, xml):
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		version = response.firstChild.firstChild.nodeValue
		return version
	
	def _read_get_https_certificate_response(self, xml):
		self._remove_whitespace_nodes(xml.documentElement)
		response = xml.documentElement
		if len(response.childNodes):
			cert = response.firstChild.firstChild.nodeValue
			pkey = response.lastChild.firstChild.nodeValue
			return (cert, pkey)
		
		return (None, None)	

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
