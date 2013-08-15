import BaseHTTPServer
import uuid
import os
import shutil
import subprocess
import xml.dom.minidom as dom
import string


from lettuce import step
import mock

from habibi import crypto


VAGRANT_FILE = '''
Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu1204"
  config.vm.box_url = "http://scalr-labs.s3.amazonaws.com/ubuntu1204-lxc_devel_20130814.box"
  config.vm.synced_folder "../..", "/vagrant0"
  config.vm.provision :chef_solo do |chef|
     chef.cookbooks_path = "../../cookbooks/cookbooks"
     chef.add_recipe "vagrant_boxes::scalarizr_lxc"
     chef.json = { :user_data => "$user_data" }
  end
end
'''

ROUTER_IP = '10.0.3.1'

class Habibi(object):
	def __init__(self, behaviors, base_dir=None, queryenv_mock=None):
		if 'chef' not in behaviors:
			behaviors.append('chef')
		self.behaviors = behaviors
		self.base_dir = base_dir or '.habibi'
		self.queryenv_mock = queryenv_mock or mock.Mock()
		#self.messaging_mock = messaging_mock or mock.Mock()
		self.MessagingHTTPHandler.habibi = self
		self.QueryEnvHTTPHandler.habibi = self
		self.servers = []


	def run_server(self):
		server = {
			'server_id': str(uuid.uuid4()),
			'crypto_key': crypto.keygen(),
			'public_ip': None,
			'private_ip': None,
			'status': 'pending',
			'index': self._next_server_index()
		}
		self.servers.append(server)

		server_dir = self.base_dir + '/' + server['server_id']
		os.makedirs(server_dir)
		with open(server_dir + '/Vagrantfile', 'w+') as fp:
			tpl = string.Template(VAGRANT_FILE)
			fp.write(tpl.substitute(
				user_data=self._pack_user_data(self._user_data(server))
			))

		#subprocess.Popen('vagrant init ubuntu1210', shell=True, cwd=server_dir).communicate()
		# TODO: init Vagrnatfile from VAGRANT_FILE template
		subprocess.Popen('vagrant up --provider lxc', shell=True, cwd=server_dir).communicate()
		# TODO: server['machine_id']

	def start(self):
		self.messaging_server = BaseHTTPServer.HTTPServer(('', 10001), self.MessagingHTTPHandler)
		self.messaging_thread = threading.Thread(self.messaging_server.serve_forever)
		self.messaging_thread.setDaemon(True)
		self.messaging_thread.start()

		self.queryenv_server = BaseHTTPServer.HTTPServer(('', 10002), self.QueryEnvHTTPHandler)
		self.queryenv_thread = threading.Thread(self.queryenv_server.serve_forever)
		self.queryenv_thread.setDaemon(True)
		self.queryenv_thread.start()

		if os.path.eixsts(self.base_dir):
			shutil.rmtree(self.base_dir)
		os.makedirs(self.base_dir)

	def stop(self):
		self.messaging_server.shutdown()
		self.queryenv_server.shutdown()


	def _next_server_index(self):
		# TODO: handle holes
		return len(self.servers)

	def _user_data(self, server):
		return {
			'szr_key': server['crypto_key'],
            'serverid': server['server_id'],
            'p2p_producer_endpoint': 'http://' + ROUTER_IP + '/messaging',
            'queryenv_url': 'http://' + ROUTER_IP + '/query-env',
            'behaviors': ','.join(self.behaviors),
            'farm_roleid': '1',
            'roleid': '1',
            'env_id': '1',
            'platform': 'lxc',
            'server_index': server['index']
		}

	def _pack_user_data(self, user_data):
		return ';'.join(['{0}={1}'.format(k, v) for k, v in user_data.items()])


	def render_html(self, hdlr, http_code, http_body=None):
		if http_code >= 400 and not http_body:
			exc_info = sys.exc_info()
			http_body = '{0}: {1}'.format(exc_info[0].__class__.__name__, exc_info[1])
		http_body = http_body or ''
		hdlr.send_response(http_code)
		hdlr.send_header('Content-length', len(http_body))
		hdlr.end_headers()
		hdlr.wfile.write(http_body)

	def servers(self, pattern):
		if pattern:
			if re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', pattern):
				for server in servers:
					if server['server_id'] == pattern:
						return [server]
		else:
			return self.servers
		msg = 'Empty results for search servers by pattern: {0}'.format(pattern)
		raise LookupError(msg)

	def send(self, msg_name, server_pattern=None, source_msg=None):
		servers = self.servers(server_pattern)

		for server in servers:
			msg = Message()
			msg.id = str(uuid4.uuid())
			msg.name = msg_name
			msg.body = source_msg.body

			# call hook

			try:
				xml_data = msg.toxml()
				encrypted_data = crypto.encrypt(xml_data, server['crypto_key'])
				signature, timestamp = crypto.sign_http_request(encrypted_data, server['crypto_key'])

				url = server['public_ip'] + '/control'
				req = urllib2.Request(url, encrypted_data, {
					'Content-type': 'application/xml',
					'Date': timestamp,
					'X-Signature': signature,
					'X-Server-Id': server['server_id']
				})
				opener = urllib2.build_opener(urllib2.HTTPRedirectHandler())
				opener.open(req)
			except:
				LOG.warn('Undelivered message: %s' % msg)
		

	def on_message(self, msg):
		server = self.servers(msg.meta['server_id'])[0]
		if msg.name == 'HostInit':
			server['status'] = 'initializing'
			self.send('HostInitResponse', server=server, source_msg=msg)
			self.send('HostInit', source_msg=msg)
		elif msg.name == 'BeforeHostUp':
			self.send('BeforeHostUp', source_msg=msg)
		elif msg.name == 'HostUp':
			server['status'] = 'running'
			self.send('HostUp', source_msg=msg)
		else:
			raise Exception('Unprocessed message: %s' % msg)



	class MessagingHTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):
		habibi = None


		def do_POST(self):
			if os.path.basename(self.path) != 'control':
				habibi.render_html(self, 201)
				return

			try:
				encrypted_data = self.rfile.read(int(self.headers['Content-length']))
				server_id = self.headers['X-Server-Id']
				crypto_key = self.find_server(server_id)['crypto_key']
				xml_data = crypto.decrypt(encrypted_data, crypto_key)
				message = Message.fromxml(xml_data)
				habibi.render_html(self, 201)
			except:
				habibi.render_html(self, 400)
			else:
				habibi.on_message(message)


	class QueryEnvHTTPHandler(BaseHTTPServer.BaseHTTPRequestHandler):
		habibi = None

		def do_POST(self):
			pass


class Message(object):
    def __init__(self, name=None, meta=None, body=None):
        self.id = None
        self.name = name
        self.meta = meta or {}
        self.body = body or {}

    def fromjson(self, json_str):
        if isinstance(json_str, str):
            json_str = json_str.decode('utf-8')

        json_obj = json.loads(json_str)
        for attr in  ('id', 'name', 'meta', 'body'):
            assert attr in json_obj, 'Attribute required: %s' % attr
            setattr(self, attr, copy.copy(json_obj[attr]))

    def fromxml (self, xml):
        if isinstance(xml, str):
            xml = xml.decode('utf-8')
        doc = dom.parseString(xml.encode('utf-8'))
        xml_strip(doc)

        root = doc.documentElement
        self.id = root.getAttribute("id")
        self.name = root.getAttribute("name")

        for ch in root.firstChild.childNodes:
            self.meta[ch.nodeName] = self._walk_decode(ch)
        for ch in root.childNodes[1].childNodes:
            self.body[ch.nodeName] = self._walk_decode(ch)


    def tojson(self):
        result = dict(id=self.id, name=self.name,
                                  body=self.body, meta=self.meta)

        return json.dumps(result, ensure_ascii=True)


    def _walk_decode(self, el):
        if el.firstChild and el.firstChild.nodeType == 1:
            if all((ch.nodeName == "item" for ch in el.childNodes)):
                return list(self._walk_decode(ch) for ch in el.childNodes)
            else:
                return dict(tuple((ch.nodeName, self._walk_decode(ch)) for ch in el.childNodes))
        else:
            return el.firstChild and el.firstChild.nodeValue or None

    def __str__(self):
        impl = dom.getDOMImplementation()
        doc = impl.createDocument(None, "message", None)

        root = doc.documentElement;
        root.setAttribute("id", str(self.id))
        root.setAttribute("name", str(self.name))

        meta = doc.createElement("meta")
        root.appendChild(meta)
        self._walk_encode(self.meta, meta, doc)

        body = doc.createElement("body")
        root.appendChild(body)
        self._walk_encode(self.body, body, doc)

        return doc.toxml('utf-8')

    toxml = __str__

    def _walk_encode(self, value, el, doc):
        if getattr(value, '__iter__', False):
            if getattr(value, "keys", False):
                for k, v in value.items():
                    itemEl = doc.createElement(str(k))
                    el.appendChild(itemEl)
                    self._walk_encode(v, itemEl, doc)
            else:
                for v in value:
                    itemEl = doc.createElement("item")
                    el.appendChild(itemEl)
                    self._walk_encode(v, itemEl, doc)
        else:
            '''
            if not isinstance(value, unicode):
                    if value is not None:
                            value = str(value)
                    else:
                            value = ''
            el.appendChild(doc.createTextNode(value))
            '''
            if value is not None and not isinstance(value, unicode):
                value = str(value).decode('utf-8')
            el.appendChild(doc.createTextNode(value or ''))



class breakpoint(object):
	def __init__(self, **kwds):
		for name, value in kwds.items():
			setattr(self, name, value)

	def __call__(self, f):
		f.breakpoint = self

class Spy1(object):

	@breakpoint(msg='HostInit', sender='base.1')
	def hi(self):
		print 'recv HI from server'

	@breakpoint(msg='HostInit', receiver='base')
	def hi_all(self):
		print 'send HI to all servers'

	@breakpoint(msg='HostInitResponse', msg_to='base.1')
	def hir(self):
		print 'send HIR to server'

	@breakpoint(msg='BeforeHostUp', msg_from='base.1')
	def bhup_in(self):
		print 'recv BeforeHostUp from server'

	@breakpoint(msg='BeforeHostUp', msg_to='base.1')
	def bhup_out(self):
		print 'send BeforeHostUp to server'

	@breakpoint(msg='HostUp', msg_from='base.1')
	def hup(self):
		print 'recv HostUp from server'


class Barrier(object):
	def __init__(self, size):
		selkf.size = size
