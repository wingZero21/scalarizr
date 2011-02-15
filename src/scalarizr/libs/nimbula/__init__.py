'''
Created on Feb 14th, 2011

@author: Dmytro Korsakov
'''
import os
import urlparse
import httplib
import urllib
import urllib2
import json
import string
import random


nimbula_url = 'NIMBULA_URL'
nimbula_username = 'NIMBULA_USERNAME'
nimbula_password = 'NIMBULA_PASSWORD'

def authenticate():
	'''
	['Server: nginx/0.6.32\r\n', 'Date: Tue, 15 Feb 2011 07:37:29 GMT\r\n', 
	'Content-Type: application/json\r\n', 'Connection: close\r\n', 
	'Expires: Tue, 15 Feb 2011 07:37:29 GMT\r\n', 
	'Set-Cookie: nimbula={"realm": "beach", "value": 
	"{\\"customer\\": \\"scalr\\", \\"expires\\": 1297759049.3628099, 
	\\"realm\\": \\"beach\\", \\"user\\": \\"/scalr/administrator\\", 
	\\"entity_type\\": \\"user\\"}", "signature": 
	"AFZNnE0e0SVkEYFIFM5jnDdfvKztL3g8YaBvkm9HHPh+Q/9aDNPR0VpP9iqs530wl
	fRKIGWXXJVQnL9xlxJQqsbUkxzAbarGzvwfg5+Zf3/BozyjwhhaYSljlAuHTBVueBz
	NTi8FyLhHIOWii9T0rJV+zOKaHPRr02D92sdAtt2cS8Xdb31Ax6e5UcQMij3WD0gIz
	TxZVawaI6veQZXrsarDXGBr1rQZzg+s8KH2A+O3D56DE+1jBvZEMN6+/BLO7m9OZsf
	s7XqPkc5N5UEYMSZVrI3rUDNlHf2j9Um99wibUdL5oDhQ2n0SOmzm1bC1HEbEGaDT5
	Jj0eabxTEhi3Gm9kKck"}; Expires=Tue, 15-Feb-2011 08:37:29 GMT; Path=/\r\n']
	'''
	
	login = os.environ[nimbula_username]
	pwd = os.environ[nimbula_password]
	auth_basename = '/authenticate/'
	
	if not (login or pwd):
		raise NimbulaError('No login inforation found')
	
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
	uri = urlparse.urljoin(os.environ[nimbula_url], auth_basename)
	request = urllib2.Request(uri)
	
	request.add_header('Accept', 'application/json')
	request.add_header('Content-Type', 'application/json')
	
	raw_data = dict(user=login, password=pwd)
	data = json.dumps(raw_data)
	
	urllib2.install_opener(opener)
	response = opener.open(request, data=data)
	
	retcode = response.code

	if not str(retcode).startswith('2'):
		raise NimbulaError('Auth failed')	
	
	info = response.info()
	headers = info.headers
	return headers


class NimbulaConnection:
	username = None
	password = None
	api_url = None
	
	def __init__(self, api_url, username, password=None):
		self.api_url = api_url
		self.username = username
		self.password = password
		
	def _get_object_URI(self, objname, basename=None):
		basename = basename or 'machineimage'
		if not objname.startswith('/'):
			objname = '%s/%s' % (self.username, objname)
		return urlparse.urljoin(self.api_url,basename+objname)
	

	def _request(self, uri, headers=None, query_method=None, force=True):
		request = urllib2.Request(uri)
		
		headers = headers or [('Accept', 'application/json'), ('Content-Type', 'application/json')]
		for header in headers:
			request.add_header(*header)
			
		if query_method:
			request.get_method = lambda: query_method
			
		try:
			f = urllib2.urlopen(request)
		except urllib2.HTTPError, e:
			if e.code==401 and force==True:
				authenticate()
				return self._request(uri, headers, query_method, force=False)
			else:
				raise NimbulaError(e)
			
		return f
		
		
	def add_machine_image(self, name, file=None, fp=None, attributes=None, account=None):
		''' 
		@param name: base name (ex: apache-deb5-20110217), autocomplete customer/user from self.username 
		@param file: file name
		@param fp: file-like object. One of `file` or `fp` should be provided
		'''
		if not file and not fp:
			raise NimbulaError
		
		boundary = "".join([random.choice(string.ascii_lowercase+string.digits) for x in xrange(31)])
		
		uri = self._get_object_URI(name)
	
	
	def get_machine_image(self, name, force=True):
		'''
		 '{"attributes": {"nimbula_compressed_size": 97120551, "nimbula_decompressed_size": 5905612288}, 
		 "account": null, "uri": "https://serverbeach.demo.nimbula.com:443/machineimage/nimbula/public/default", 
		 "file": null, "name": "/nimbula/public/default"}'
		'''
		uri = self._get_object_URI(name)
		f = self._request(uri)
		response = f.read()
		return response

	
	def delete_machine_image(self, name, force=True):
		uri = self._get_object_URI(name)
		f = self._request(uri, query_method='DELETE')
		response = f.read()
		return response
		
		
	def discover_machine_image(self, container=None):
		pass

	
class NimbulaError(BaseException):
	pass