'''
Created on Dec 23, 2009

@author: Dmytro Korsakov
'''
from scalarizr.util import xml_strip
from scalarizr.util.cryptotool import sign_http_request
import logging
from urllib2 import urlopen, Request, URLError, HTTPError
from urllib import urlencode, splitnport
from xml.dom.minidom import parseString


class QueryEnvError(Exception):
	pass

class QueryEnvService(object):
	_logger = None
	
	url = None
	api_version = None
	key = None
	server_id = None
	
	def __init__(self, url, server_id=None, key=None, api_version="2009-03-05"):
		self._logger = logging.getLogger(__name__)
		self.url = url if url[-1] != "/" else url[0:-1]
		self.server_id = server_id		
		self.key = key
		self.api_version = api_version
	
	def list_roles (self, role_name=None, behaviour=None):
		"""
		@return Role[]
		"""
		parameters = {}
		if None != role_name :
			parameters["role"] = role_name
		if None != behaviour:
			parameters["behaviour"] = behaviour
			
		return self._request("list-roles",parameters, self._read_list_roles_response)
	
	def list_role_params(self, role_name=None):
		"""
		@return dict
		"""
		parameters = {}
		if None != role_name :
			parameters["role"] = role_name
		return self._request("list-role-params",parameters, self._read_list_role_params_response)
	
	def list_scripts (self, event=None, event_id=None, asynchronous=None, name=None):
		"""
		@return Script[]
		"""
		parameters = {}
		if None != event :
			parameters["event"] = event
		if None != event_id:
			parameters["event_id"] = event_id
		if None != asynchronous:
			parameters["asynchronous"] = asynchronous
		if None != name :
			parameters["name"] = name
		return self._request("list-scripts",parameters, self._read_list_scripts_response)
	
	def list_virtual_hosts (self, name=None, https=None):
		"""
		@return VirtualHost[]
		"""
		parameters = {}
		if None != name :
			parameters["name"] = name
		if None != https:
			parameters["https"] = https
		return self._request("list-virtualhosts",parameters, self._read_list_virtualhosts_response)
	
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

	def _request (self, command, params={}, response_reader=None):
		"""
		@return object
		"""
		# Perform HTTP request
		url = "%s/%s/%s" % (self.url, self.api_version, command)
		request_body = {}
		request_body["operation"] = command
		request_body["version"] = self.api_version
		if {} != params :
			for key, value in params.items():
				request_body[key] = value
				
		signature, timestamp = sign_http_request(request_body, self.key)		
		
		post_data = urlencode(request_body)
		headers = {
			"Date": timestamp, 
			"X-Signature": signature, 
			"X-Server-Id": self.server_id
		}
		response = None
		try:
			self._logger.debug("QueryEnv request: %s", post_data)
			req = Request(url, post_data, headers)
			response = urlopen(req)
		except URLError, e:
			if isinstance(e, HTTPError):
				resp_body = e.read() if e.fp is not None else ""
				raise QueryEnvError("Request failed. %s. URL: %s. Service message: %s" % (e, self.url, resp_body))				
			else:
				host, port = splitnport(req.host, req.port or 80)
				raise QueryEnvError("Cannot connect to QueryEnv server on %s:%s. %s" 
						% (host, port, str(e)))

		resp_body = response.read()
		self._logger.debug("QueryEnv response: %s", resp_body)


		# Parse XML response
		xml = None
		try:
			xml = xml_strip(parseString(resp_body))
		except (Exception, BaseException), e:
			raise QueryEnvError("Cannot parse XML. %s" % [str(e)])
		return response_reader(xml)

		
	def _read_list_roles_response(self, xml):
		ret = []
		
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
	
		response = xml.documentElement
		for param_el in response.firstChild.childNodes:
			ret[param_el.getAttribute("name")] = param_el.firstChild.firstChild.nodeValue
				
		return ret
	
	def _read_get_latest_version_response(self, xml):
		response = xml.documentElement
		version = response.firstChild.firstChild.nodeValue
		return version
	
	def _read_get_https_certificate_response(self, xml):
		response = xml.documentElement
		if len(response.childNodes):
			virtualhost = response.firstChild
			for ssl_data in virtualhost.childNodes:
				if ssl_data.nodeName=="cert":
					cert = ssl_data.firstChild.nodeValue
				elif ssl_data.nodeName=="pkey":
					pkey = ssl_data.firstChild.nodeValue
			if not cert:
				self._logger.error("Queryenv didn`t return SSL cert")
			if not pkey:
				self._logger.error("Queryenv didn`t return SSL keys")
			return (cert, pkey)
		self._logger.error("Queryenv return empty SSL cert & keys")
		return (None, None)	

	def _read_list_virtualhosts_response(self, xml):
		ret = []
		
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
	volumes  = None
	
	def __init__(self):
		self.volumes = []
	
	def __str__(self):
		opts = (self.name, self.dir, self.create_fs, len(self.volumes))
		return "qe:Mountpoint(name: %s, dir: %s, create_fs: %s, num_volumes: %d)" % opts
	
	def __repr__(self):
		return "name = " + str(self.name) \
	+ "; dir = " + str(self.dir) \
	+ "; create_fs = " + str(self.create_fs) \
	+ "; is_array = " + str(self.is_array) \
	+ "; volumes = " + str(self.volumes)
	
class Volume(object):
	volume_id  = None
	device = None
	
	def __str__(self):
		return "qe:Volume(volume_id: %s, device: %s)" % (self.volume_id, self.device)
	
	def __repr__(self):
		return 'volume_id = ' + str(self.volume_id) \
	+ "; device = " + str(self.device)
		
class Role(object):
	behaviour = None
	name = None
	hosts = None
	
	def __init__(self):
		self.hosts = []
	
	def __str__(self):
		opts = (self.name, self.behaviour, len(self.hosts))
		return "qe:Role(name: %s, behaviour: %s, num_hosts: %s)" % opts
	
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
