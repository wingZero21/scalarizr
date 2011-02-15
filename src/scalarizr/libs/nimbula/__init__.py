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

nimbula_url = 'NIMBULA_URL'
nimbula_username = 'NIMBULA_USERNAME'
nimbula_password = 'NIMBULA_PASSWORD'

def authenticate():
	login = os.environ[nimbula_username]
	pwd = os.environ[nimbula_password]
	
	if not (login or pwd):
		raise NimbulaError('No login inforation found')
	
	raw_data = dict(user=login, password=pwd)
	data = json.dumps(raw_data)
	
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor())
	request = urllib2.Request('HTTPS://serverbeach.demo.nimbula.com:443/authenticate/')
	request.add_header('Accept', 'application/json')
	request.add_header('Content-Type', 'application/json')
	urllib2.install_opener(opener)
	response = opener.open(request, data=data)
	
	retcode = response.code

	if not str(retcode).startswith('2'):
		raise NimbulaError('Auth failed')	
	
	info = response.info()
	headers = info.headers
	
	return headers
		
	'''
	headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}
	conn = httplib.HTTPSConnection(os.environ[nimbula_url])
	conn.request("POST", '/authenticate/', data, headers)
	response = conn.getresponse()
	reg = response.getheaders()
	'''

class NimbulaConnection:
	username = None
	password = None
	api_url = None
	
	def __init__(self, api_url, username, password=None):
		self.api_url = api_url
		self.username = username
		self.password = password
		
	def _get_image_URI(self, name):
		basename = 'machineimage'
		if not name.startswith('/'):
			name = '/scalr/%s/%s' % (self.username, name)
		return urlparse.urljoin(self.api_url,basename+name)
		
	def add_machine_image(self, name, file=None, fp=None, attributes=None, account=None):
		''' 
		@param name: base name (ex: apache-deb5-20110217), autocomplete customer/user from self.username 
		@param file: file name
		@param fp: file-like object. One of `file` or `fp` should be provided
		'''
		if not file and not fp:
			raise NimbulaError
		
		uri = self._get_image_URI(name)
		
	def get_machine_image(self, name, force=True):
		'''
		 '{"attributes": {"nimbula_compressed_size": 97120551, "nimbula_decompressed_size": 5905612288}, 
		 "account": null, "uri": "https://serverbeach.demo.nimbula.com:443/machineimage/nimbula/public/default", 
		 "file": null, "name": "/nimbula/public/default"}'
		'''
		uri = self._get_image_URI(name)
		try:
			f = urllib2.urlopen(uri)
		except urllib2.HTTPError, e:
			if e.code==401 and force:
				authenticate()
				return self.get_machine_image(name, force=False)
				
				
		response = f.read()
		return response

		
		
	def delete_machine_image(self, name):
		uri = self._get_URI(name)
		
	def discover_machine_image(self, container=None):
		pass

	
class NimbulaError(BaseException):
	pass