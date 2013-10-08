
# pylint: disable=R0902, W0613, R0913, R0914, R0201, R0904

import BaseHTTPServer
import uuid
import os
import sys
import re
import shutil
import subprocess
import xml.dom.minidom as dom
import xml.etree.ElementTree as etree
import urllib2
import time
import string
import logging
import threading
import wsgiref
import binascii
import json
import copy
import cgi
from collections import Hashable

from habibi import crypto, storage


logging.basicConfig(
        stream=sys.stderr, 
        level=logging.DEBUG, 
        format='%(name)-20s %(levelname)-8s - %(message)s')
LOG = logging.getLogger('habibi')
VAGRANT_FILE = '''
Vagrant.configure("2") do |config|
  config.vm.box = "ubuntu1204"
  config.vm.box_url = "http://scalr-labs.s3.amazonaws.com/ubuntu1204-lxc_devel_20130814.box"
  config.vm.synced_folder "../..", "/vagrant0"
  config.vm.provision :chef_solo do |chef|
     chef.cookbooks_path = ["../../cookbooks/cookbooks", "../../public_cookbooks/cookbooks"]
     chef.add_recipe "$behavior"
     chef.add_recipe "vagrant_boxes::scalarizr_lxc"
     chef.json = { :user_data => "$user_data" }
  end
end
'''


def hashable(obj):
    return isinstance(obj, Hashable)


class Notification(object):

    def __init__(self, name=None):
        self._lock = threading.Condition()
        self.name = name or ''
        self._times_notified = 0
        self._times_waited = 0
        self._waiters_qty = 0

    def wait(self, timeout=None):
        try:
            self._lock.acquire()
            self._waiters_qty += 1
            self._lock.wait(timeout)
            self._waiters_qty -= 1
            self._times_waited += 1
        finally:
            self._lock.release()

    def notify(self):
        self._lock.acquire()
        self._lock.notify_all()
        self._times_notified += 1
        self._lock.release()

    @property
    def times_notified(self):
        return self._times_notified

    @property
    def times_waited(self):
        return self._times_waited

    @property
    def waiters_qty(self):
        return self._waiters_qty


class NotificationCenter(object):

    def __init__(self):
        self._notification_pool = {}

    def _normalize_notification_name(self, name):
        if hashable(name) and not isinstance(name, str):
            return str(hash(name))
        return name

    def wait_notification(self, name, timeout=None):
        name = self._normalize_notification_name(name)
        if name not in self._notification_pool:
            self._notification_pool[name] = Notification(name)
        self._notification_pool[name].wait(timeout)

    def notify(self, name):
        name = self._normalize_notification_name(name)
        if name in self._notification_pool:
            self._notification_pool[name].notify()

    def get_notification(self, name):
        name = self._normalize_notification_name(name)
        return self._notification_pool.get(name)


class Server(object):

    def __init__(self,
                 behaviors,
                 sid=None,
                 index=0,
                 crypto_key=None,
                 farm_hash=None,
                 public_ip=None,
                 private_ip=None,
                 status='pending',
                 zone=None,
                 notification_center=None):
        self.id = sid or str(uuid.uuid4())
        self.index = index
        self.crypto_key = crypto_key or crypto.keygen()
        self.farm_hash = farm_hash or crypto.keygen(10)
        self.public_ip = public_ip
        self.private_ip = private_ip
        self._status = status
        self.behaviors = behaviors
        self.zone = zone
        self.notification_center = notification_center

    def get_rootfs_path(self):
        rootfs_path = ''
        lxc_containers_path = '/var/lib/lxc'

        for d in os.listdir(lxc_containers_path):
            if self.id in d:
                rootfs_path = os.path.join(lxc_containers_path, d+'/rootfs')
                break

        if not rootfs_path:
            raise BaseException("Can't find server with id: %s" % self.id)

        return rootfs_path

    def set_status(self, new_status):
        self._status = new_status
        self.notification_center.notify((self, 'status'))

    def get_status(self):
        return self._status

    status = property(get_status, set_status)

class Habibi(object):

    def __init__(self, behavior, base_dir=None):
        #if 'chef' not in behaviors:
        #    behaviors.append('chef')
        self.role = {'behaviors': [behavior],
                     'name': 'habibi-lxc'}
        self.servers = []
        self.breakpoints = []
        self.router_ip = '10.0.3.1'
        self.port = 10001
        self.queryenv_version = '2012-07-01'
        self.base_dir = base_dir or '.habibi'
        self.RequestHandler.habibi = self
        self.queryenv = QueryEnv(self)
        self.web_server = self.web_server_thread = None
        self.msg_center = NotificationCenter()

    def run_server(self, zone='lxc-zone'):
        server = Server(behaviors=self.role['behaviors'],
                        index=self._next_server_index(),
                        notification_center=self.msg_center,
                        zone=zone)
        self.servers.append(server)

        server_dir = self.base_dir + '/' + server.id
        os.makedirs(server_dir)
        with open(server_dir + '/Vagrantfile', 'w+') as fp:
            tpl = string.Template(VAGRANT_FILE)
            fp.write(tpl.substitute(
                user_data=self._pack_user_data(self._user_data(server)),
                behavior=self.role['behaviors'][0]
                ))

        #subprocess.Popen('vagrant init ubuntu1210', shell=True, cwd=server_dir).communicate()
        subprocess.Popen('vagrant up --provider lxc', shell=True, cwd=server_dir).communicate()
        # TODO: server['machine_id']
        return server

    def stop_server(self, server):
        if isinstance(server, str):
            server = self.find_servers(server)[0]
        server_dir = self.base_dir + '/' + server.id
        p = subprocess.Popen('vagrant halt', shell=True, cwd=server_dir)
        p.communicate()

    def destroy_server(self, server):
        if isinstance(server, str):
            server = self.find_servers(server)[0]
        server_dir = self.base_dir + '/' + server.id
        p = subprocess.Popen('vagrant destroy -f', shell=True, cwd=server_dir)
        p.communicate()

    def _perform_server_command(self, server, command):
        if isinstance(server, str):
            server = self.find_servers(server)[0]
        server_dir = self.base_dir + '/' + server.id
        p = subprocess.Popen('vagrant ssh -c "sudo %s"' % command, shell=True, cwd=server_dir)
        return p.communicate()

    def cut_off_server(self, server):
        """ breaks connection to and from server """
        self._perform_server_command(server, 'iptables -A INPUT -p tcp --dport 22 -j ACCEPT')
        self._perform_server_command(server, 'iptables -A OUTPUT -p tcp --sport 22 -j ACCEPT')
        self._perform_server_command(server, 'iptables -P INPUT DROP')
        self._perform_server_command(server, 'iptables -P OUTPUT DROP')
        self._perform_server_command(server, 'iptables -P FORWARD DROP')

    def start(self):
        self.web_server = BaseHTTPServer.HTTPServer(('', self.port), self.RequestHandler)
        self.web_server_thread = threading.Thread(name='WebServer', 
                                                 target=self.web_server.serve_forever)
        self.web_server_thread.setDaemon(True)
        self.web_server_thread.start()

        storage_manager = storage.StorageManager()
        self.storage_server = wsgiref.simple_server.make_server('0.0.0.0', storage.port, storage_manager)
        storage_thread = threading.Thread(target=self.storage_server.serve_forever, name='Storage server')
        storage_thread.start()

        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)

    def stop(self):
        self.web_server.shutdown()
        self.storage_server.shutdown()

    def spy(self, spy):
        self.breakpoints = []
        for name in dir(spy):
            fn = getattr(spy, name)
            if hasattr(fn, 'breakpoint'):
                bp = Breakpoint(fn.breakpoint)
                self.breakpoints.append((fn, bp))

    def apply_breakpoints(self, cond, kwds):
        test_bp = Breakpoint(cond)
        for bp in self.breakpoints:
            if bp[1] == test_bp:
                bp[0](**kwds)
                LOG.debug('hash of running bg %s', hash(bp[0]))
                self.msg_center.notify(bp[0])

    def _next_server_index(self):
        # TODO: handle holes
        return len(self.servers) + 1

    def _user_data(self, server):
        return {'szr_key': server.crypto_key,
                'hash': server.farm_hash,
                'serverid': server.id,
                'p2p_producer_endpoint': 'http://{0}:{1}/messaging'.format(self.router_ip,
                                                                           self.port),
                'queryenv_url': 'http://{0}:{1}/query-env'.format(self.router_ip,
                                                                  self.port),
                'behaviors': ','.join(server.behaviors),
                'farm_roleid': '1',
                'roleid': '1',
                'env_id': '1',
                'platform': 'lxc',
                'region': server.zone,
                'server_index': str(server.index),
                'storage_service_url': 'http://%s:%s'.format(self.router_ip, storage.port)}


    def _pack_user_data(self, user_data):
        return ';'.join(['{0}={1}'.format(k, v) for k, v in user_data.items()])

    def find_servers(self, pattern):
        # TODO: finish else clauses - find server if pattern is not uuid
        if pattern:
            if re.search(r'\w{8}-\w{4}-\w{4}-\w{4}-\w{12}', pattern):
                for server in self.servers:
                    if server.id == pattern:
                        return [server]
            else:
                LOG.warn('pattern %s doesnt match uuid4', pattern)
        else:
            return [server for server in self.servers 
                        if server.status != 'pending']
        msg = 'Empty results for search servers by pattern: {0}'.format(pattern)
        raise LookupError(msg)

    def send(self, msg_name, server_pattern=None, source_msg=None):
        servers = self.find_servers(server_pattern)
        #source_server = self.find_servers(source_msg.meta['server_id'])[0]

        for server in servers:
            msg = Message()
            msg.id = str(uuid.uuid4())
            msg.name = msg_name
            msg.body = source_msg.body.copy()
            msg.body['scripts'] = []
            msg.body['behaviour'] = list(server.behaviors)

            self.apply_breakpoints(cond={'msg_name': msg.name, 
                                         'target_index': str(server.index),
                                         'target_behavior': server.behaviors[0]}, 
                                   kwds={'target_msg': msg,
                                         'target_server': server,
                                         'source_msg': source_msg})

            LOG.debug('<~ %s to %s', msg.name, server.id)
            xml_data = msg.toxml()
            LOG.debug(' * data: %s', xml_data)
            LOG.debug(' * key: %s', server.crypto_key)
            crypto_key = binascii.a2b_base64(server.crypto_key)
            encrypted_data = crypto.encrypt(xml_data, crypto_key)
            signature, timestamp = crypto.sign(encrypted_data, crypto_key)

            url = 'http://{0}:8013/control'.format(server.public_ip)
            req = urllib2.Request(url, encrypted_data, {
                'Content-type': 'application/xml',
                'Date': timestamp,
                'X-Signature': signature,
                'X-Server-Id': server.id})
            opener = urllib2.build_opener(urllib2.HTTPRedirectHandler())
            opener.open(req)
        
    def on_message(self, msg):
        server_id = msg.meta['server_id']
        LOG.debug('~> %s from %s', msg.name, server_id)
        server = self.find_servers(server_id)[0]

        self.apply_breakpoints(cond={'msg_name': msg.name, 
                                     'source_index': str(server.index),
                                     'source_behavior': server.behaviors[0]}, 
                               kwds={'source_msg': msg})

        if msg.name == 'HostInit':
            server.status = 'initializing'
            server.public_ip = msg.body['remote_ip']
            server.private_ip = msg.body['local_ip']
            server.crypto_key = msg.body['crypto_key'].strip()

            time.sleep(1)  # It's important gap for Scalarizr
            self.send('HostInitResponse', server_pattern=server_id, source_msg=msg)
            self.send('HostInit', source_msg=msg)
        elif msg.name == 'BeforeHostUp':
            self.send('BeforeHostUp', source_msg=msg)
        elif msg.name == 'HostUp':
            server.status = 'running'
            self.send('HostUp', source_msg=msg)
        elif msg.name in ('OperationDefinition', 'OperationProgress', 'OperationResult'):
            pass
        else:
            raise Exception('Unprocessed message: %s' % msg)


    class RequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
        LOG = logging.getLogger('habibi.web')

        def do_POST(self):
            try:
                if self.path.startswith('/messaging'):
                    self.do_messaging()
                elif self.path.startswith('/query-env'):
                    self.do_queryenv()
                else:
                    msg = 'Unknown route: {0}'.format(self.path)
                    raise Exception(msg)
            except:
                LOG.exception('POST exception')
                self.render_html(500)

        def do_HEAD(self):
            try:
                self.do_static()
            except:
                LOG.exception('GET exception')
                self.render_html(500)

        def do_GET(self):
            try:
                self.do_static()
            except:
                LOG.exception('HEAD exception')
                self.render_html(500)

        def do_messaging(self):
            if os.path.basename(self.path) != 'control':
                self.render_html(201)
                return

            server_id = self.headers['X-Server-Id']
            server = self.habibi.find_servers(server_id)[0]

            encrypted_data = self.rfile.read(int(self.headers['Content-length']))
            xml_data = crypto.decrypt(encrypted_data, binascii.a2b_base64(server.crypto_key))

            msg = Message()
            msg.fromxml(xml_data)
            self.render_html(201)

            hdlr_thread = threading.Thread(target=self.habibi.on_message, args=(msg, ))
            hdlr_thread.setDaemon(True)
            hdlr_thread.start()


        def do_queryenv(self):
            operation = self.path.rsplit('/', 1)[-1]
            fields = cgi.FieldStorage(
                fp=self.rfile,
                keep_blank_values=True
            )
            response = self.habibi.queryenv.run(operation, fields)
            self.render_html(200, response)


        def do_static(self, head=False):
            base = os.path.dirname(__file__)
            static_dir = base + '/static'
            filename = static_dir + self.path
            if os.path.exists(filename):
                with open(filename) as fp:
                    self.render_html(200, fp.read())
            else:
                self.render_html(404)


        def render_html(self, http_code, http_body=None, http_headers=None):
            if http_code >= 400:
                if not http_body:
                    exc_info = sys.exc_info()
                    http_body = '{0}: {1}'.format(exc_info[0].__class__.__name__, exc_info[1])
                self.send_error(http_code)
            else:
                self.send_response(http_code)

            http_body = http_body or ''
            if http_headers:
                for header, value in http_headers:
                    self.send_header(header, value)
            if not http_headers or (http_headers and not 'Content-length' in http_headers):
                self.send_header('Content-length', len(http_body))
            self.end_headers()
            self.wfile.write(http_body) 

        def log_message(self, format_, *args):
            self.LOG.debug(format_, *args)


class QueryEnv(object):
    habibi = None
    LOG = logging.getLogger('habibi.queryenv')

    def __init__(self, habibi):
        self.habibi = habibi
        self.fields = None
        self._handlers = dict()


    def subscribe(self, method, function):
        if not method in self._handlers:
            self._handlers[method] = list()
        self._handlers[method].append(function)

    def run(self, operation, fields):
        self.fields = fields
        try:
            method_name = operation.replace('-', '_')
            self.LOG.debug('run %s', operation)
            response = etree.Element('response')
            if hasattr(self, method_name):
                response.append(getattr(self, method_name)())
            if method_name in self._handlers:
                for hndlr in self._handlers[method_name]:
                    hndlr(method_name, response, fields)
            return etree.tostring(response)
        except:
            exc_info = sys.exc_info()
            LOG.error('Queryenv error', exc_info=exc_info)
            raise
        finally:
            self.fields = None

    def get_latest_version(self):
        ret = etree.Element('version')
        ret.text = self.habibi.queryenv_version
        return ret

    def list_global_variables(self):
        return etree.Element('variables')

    def get_global_config(self):
        ret = etree.Element('settings')
        settings = {'dns.static.endpoint': 'scalr-dns.com',
                    'scalr.version': '4.5.0',
                    'scalr.id': '884c7c0'}
        for key, val in settings.items():
            setting = etree.Element('setting', key=key)
            setting.text = val
            ret.append(setting)
        return ret

    def list_roles(self):
        ret = etree.Element('roles')
        role = etree.Element('role')
        role.attrib.update({'id': '1',
                            'role-id': '1',
                            'behaviour': ','.join(self.habibi.role['behaviors']),
                            'name': self.habibi.role['name']})
        hosts = etree.Element('hosts')
        role.append(hosts)
        for server in self.habibi.servers:
            host = etree.Element('host')
            host.attrib.update({'internal-ip': server.private_ip,
                                'external-ip': server.public_ip,
                                'status': server.status,
                                'index': str(server.index),
                                'cloud-location': server.zone})
            hosts.append(host)
        ret.append(role)
        return ret

    def get_service_configuration(self):
        return etree.Element('settings')


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

    def fromxml(self, xml):
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
        result = dict(id=self.id, name=self.name, body=self.body, meta=self.meta)
        return json.dumps(result, ensure_ascii=True)

    def _walk_decode(self, el):
        if el.firstChild and el.firstChild.nodeType == 1:
            if all((ch.nodeName == "item" for ch in el.childNodes)):
                return [self._walk_decode(ch) for ch in el.childNodes]
            else:
                return {ch.nodeName: self._walk_decode(ch) for ch in el.childNodes}
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
            if value is not None and not isinstance(value, unicode):
                value = str(value).decode('utf-8')
            el.appendChild(doc.createTextNode(value or ''))


class Breakpoint(object):

    def __init__(self, cond):
        bp = cond.copy()
        for key in ('source', 'target'):
            if bp.get(key):
                if '.' in bp[key]:
                    bp[key + '_behavior'], bp[key + '_index'] = bp[key].split('.')
                else:
                    bp[key + '_behavior'] = bp[key]
                del bp[key]
        self.cond = bp

    def __eq__(self, test_cond):
        if not isinstance(test_cond, Breakpoint):
            test_bp = Breakpoint(test_cond)
        else:
            test_bp = test_cond
        inc_cond = {}
        for k in self.cond.keys():
            inc_cond[k] = test_bp.cond.get(k)
        #LOG.debug('Compare test/self: %s and %s', inc_cond, self.cond)
        return inc_cond == self.cond


def xml_strip(el):
    for child in list(el.childNodes):
        if child.nodeType == child.TEXT_NODE and child.nodeValue.strip() == '':
            el.removeChild(child)
        else:
            xml_strip(child)
    return el 


def breakpoint(**kwds):
    def wrapper(fn):
        fn.breakpoint = kwds
        return fn
    return wrapper


class SampleSpy(object):

    @breakpoint(msg_name='HostInit', source='base.1')
    def hi(self, source_msg=None, **kwds):
        print 'recv HI from server %s' % source_msg.meta['server_id']

    @breakpoint(msg_name='HostInitResponse')
    def hi_all(self, target_msg=None, target_server=None, **kwds):
        target_msg.body['chef'] = {'server_url': 'http://example.test',
                                   'run_list': ['recipe[example]']}
        print 'send HIR to %s' % target_server.id

    @breakpoint(msg_name='BeforeHostUp', target='base')
    def bhup(self, **kwds):
        print 'send BeforeHostUp to all servers'

    @breakpoint(msg_name='HostUp', source='base')
    def hup(self, **kwds):
        print 'recv HostUp from server'


# class PxcSpy(object):
#     pass


# def before_all():
#     spy = PxcSpy()
#     hab = habibi.Habibi('pxc')
#     hab.spy(spy)
#     hab.start()
#     hab.msg_center.wait(spy.hi)


# def when_i_start_first_node():
#     hab.run_server()

# def test_method():
#     spy.host_init_response.wait()

#     spy.notifications.wait((server, 'status'))
