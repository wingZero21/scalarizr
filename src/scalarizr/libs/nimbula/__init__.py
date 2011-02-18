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
import cookielib
import socket

from types import MachineImage

nimbula_url = 'NIMBULA_URL'
nimbula_username = 'NIMBULA_USERNAME'
nimbula_password = 'NIMBULA_PASSWORD'

cj = cookielib.CookieJar()
last_cookies = None
authenticated = False

EOL = '\r\n'

def authenticate():

	login = os.environ[nimbula_username]
	pwd = os.environ[nimbula_password]
	auth_basename = '/authenticate/'
	
	if not (login or pwd):
		raise NimbulaError('No login inforation found')
	
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cj))
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
	
	global last_cookies, authenticated
	last_cookies = ''.join(headers[-1][12:].split(';')[0])
	authenticated = True	
	
	return headers

	
class NimbulaError(BaseException):
	pass
		
		
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
	
	
	def get_machine_image(self, name):
		uri = self._get_object_URI(name)
		f = self._request(uri)
		response = f.read()
		return MachineImage(from_json=response)

	
	def delete_machine_image(self, name):
		'''
		for image_name = '/nimbula/public/default'
		NimbulaError: HTTP Error 401: Unauthorized
		'''
		uri = self._get_object_URI(name)
		f = self._request(uri, query_method='DELETE')
		response = f.read()
		return response		
		
		
	def discover_machine_image(self, container=None):
		uri = self._get_object_URI(container or self.username)
		f = self._request(uri)
		response = f.read()
		s = [json.dumps(img) for img in json.loads(response)['result']]
		return [MachineImage(from_json=obj) for obj in s]#response


	def delete_instance(self, name):
		uri = self._get_object_URI(name, 'instance')
		f = self._request(uri, query_method='DELETE')
		response = f.read()
		return response		

		
	def _request(self, uri, headers=None, query_method=None, force=True):
		
		if not authenticated:
			authenticate()
		
		request = urllib2.Request(uri)
		
		headers = headers or {'Accept':'application/json', 'Content-Type':'application/json'}
		
		for k,v in headers.items():
			request.add_header(k, v)
			
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
		
		
	def _send_data(self, data, connection):
		if isinstance(data, basestring):
			# Is it a string?
			try:
				connection.send(data)
			except socket.error, v:
				if v[0] == 104: 
					return 
				raise
		
		elif hasattr(data, 'read'):
			# Is it a file?
			while 1:
				part = data.read(100000)
				if part == '': break
				
				try:
					connection.send(part)
				except socket.error, v:
					if v[0] == 104: 
						return 
					raise

						
	def add_machine_image(self, name, file=None, fp=None, attributes=None, account=None):
		''' 
		@param name: base name (ex: apache-deb5-20110217), autocomplete customer/user from self.username 
		@param file: file name
		@param fp: file-like object. One of `file` or `fp` should be provided
		'''
		
		#host = 'serverbeach.demo.nimbula.com'
		host = urlparse.urlparse(self.api_url)[1]
		
		def _post(pairs, boundary):		
			for name, data in pairs:
				yield '--%s%s' % (boundary, EOL)
				content = 'Content-Type: application/json%s' % EOL if name=='attributes' else ''
				content +='Content-Disposition: form-data; name="%s"%s' % (name, EOL)
				yield content
				yield EOL
				if data:
					yield data
				yield EOL
		
		if not file and not fp:
			raise NimbulaError
		
		if not authenticated:
			authenticate()
								
		pairs = []
		pairs.append(('uri', ''))
		pairs.append(('name',  self.username+'/'+name))
		pairs.append(('attributes', '{}'))
		pairs.append(('account', self.username))
		
		boundary = "".join([random.choice(string.ascii_lowercase+string.digits) for x in xrange(31)])
		
		file_length = os.path.getsize(file) if file else os.fstat(fp.fileno())[6]
		full_length = file_length
		for entry in _post(pairs, boundary): full_length += len(entry)
		
		headers = []
		headers.append(('Content-Length',full_length))
		headers.append(('AcceptEncoding', 'gzip;q=1.0, identity; q=0.5'))
		headers.append(('Accept', 'application/json'))
		headers.append(('Host', host))
		headers.append(('Content-Type', 'multipart/form-data; boundary=%s' % boundary))
		headers.append(('Cookie', last_cookies))	
		
		connection  = httplib.HTTPSConnection(host, timeout=300)
		
		try:
			if connection.sock is None:
				connection.connect()
			#connection.set_debuglevel(10)	
			connection.putrequest('POST', '/machineimage/', skip_host=True)
			
			for (k, v) in headers:
				connection.putheader(k, v)
			connection.endheaders()
			
			for content in _post(pairs, boundary):
				self._send_data(content, connection)
			
			self._send_data('--%s' % boundary+EOL, connection)
			
			cl_entry = 'Content-Length: %s' % file_length
			cl_entry += '%sContent-Disposition: form-data' % EOL
			cl_entry += '; name="file"; filename="%s"%s' % (file or fp.name, EOL)
			
			self._send_data(cl_entry, connection)	
			self._send_data(EOL, connection)	
				
			data = fp or open(file, 'rb')
			if data:
				self._send_data(data, connection)
			
			self._send_data(EOL, connection)	
			self._send_data('--%s--' % boundary+EOL, connection)
			
			response = connection.getresponse().read()
			
			message = json.loads(response)
			if message.has_key('message') and message['message'] == 'Conflict':
				raise NimbulaError('Image already exists')
			
			return MachineImage(from_json=response)
		except KeyboardInterrupt:		
			raise
		
		except BaseException, e:
			raise NimbulaError(e)
				