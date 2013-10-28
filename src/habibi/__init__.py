
# pylint: disable=R0902, W0613, R0913, R0914, R0201, R0904

import re
import os
import sys
import cgi
import json
import copy
import uuid
import time
import shutil
import random
import string
import urllib2
import logging
import binascii
import threading
import subprocess
import BaseHTTPServer
import xml.dom.minidom as dom
import xml.etree.ElementTree as etree

from wsgiref import simple_server

from habibi import crypto, storage, events


logging.basicConfig(
        stream=sys.stderr, 
        level=logging.DEBUG, 
        format='%(asctime)s %(name)-20s %(levelname)-8s - %(message)s')
LOG = logging.getLogger('habibi')

VAGRANT_FILE = '''
Vagrant.configure("2") do |config|
  #config.vm.box = "my-mongo"
  config.vm.box = "ubuntu1204"
  config.vm.box_url = "http://scalr-labs.s3.amazonaws.com/ubuntu1204-lxc_devel_20130814.box"
  config.vm.synced_folder "../..", "/vagrant0"
  config.berkshelf.enabled = true
  config.vm.provision :chef_solo do |chef|
    #chef.cookbooks_path = ["../../cookbooks/cookbooks", "../../public_cookbooks/cookbooks"]

$recipes_for_behaviours

    chef.add_recipe "vagrant_boxes::scalarizr_lxc"
    chef.json = { :user_data => "$user_data" }
  end
end
'''

BERKSHELF_FILE = """
site :opscode

repo_int = "git@github.com:Scalr/int-cookbooks.git"
repo_pub = "git@github.com:Scalr/cookbooks.git"

cookbook "vagrant_boxes", git: repo_int, rel: "cookbooks/vagrant_boxes", ref: "HEAD"
"""

ROUTER_IP = '10.0.3.1'
ROUTER_PORT = 10001


class Server(object):

    def __init__(self,
                 behaviours,
                 role,
                 sid=None,
                 index=0,
                 crypto_key=None,
                 farm_hash=None,
                 public_ip=None,
                 private_ip=None,
                 status='pending',
                 zone=None):
        """
        @param role: Role object
        @type role: FarmRole
        """
        self.id = sid or str(uuid.uuid4())
        self.index = index
        self.role = role
        self.crypto_key = crypto_key or crypto.keygen()
        self.farm_hash = farm_hash or crypto.keygen(10)
        self.public_ip = public_ip
        self.private_ip = private_ip
        self._status = status
        self.behaviours = behaviours
        self.zone = zone

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
        old_status = self._status
        self._status = new_status
        self.role.farm.event_mgr.apply_breakpoints(event='server_status_changed',
                                                    old_status=old_status,
                                                    new_status=new_status,
                                                    server=self)

    def get_status(self):
        return self._status

    status = property(get_status, set_status)

    def destroy(self):
        p = subprocess.Popen('vagrant destroy -f', shell=True, cwd=self.server_dir)
        p.communicate()
        self._status = 'terminated'
        self.role.farm.event_mgr.apply_breakpoints(event='server_terminated', server=self)

    def terminate(self, force=True):
        pass
        # TODO: Terminate server, notify farm


    def stop(self):
        p = subprocess.Popen('vagrant halt', shell=True, cwd=self.server_dir)
        p.communicate()

    def cut_off(self):
        self.perform_command('iptables -A INPUT -p tcp --dport 22 -j ACCEPT')
        self.perform_command('iptables -A OUTPUT -p tcp --sport 22 -j ACCEPT')
        self.perform_command('iptables -P INPUT DROP')
        self.perform_command('iptables -P OUTPUT DROP')
        self.perform_command('iptables -P FORWARD DROP')

    def perform_command(self, command):
        p = subprocess.Popen('vagrant ssh -c "sudo %s"' % command, shell=True, cwd=self.server_dir)
        return p.communicate()

    @property
    def server_dir(self):
        return  os.path.join(self.role.farm.base_dir, self.id)


class FarmRole(object):

    chef_recipes = None
    def __init__(self, name, behaviours, farm):
        """
        @param farm: Farm object (habibi)
        @type farm: Habibi
        """
        if not isinstance(behaviours, (list, tuple)):
            behaviours = [behaviours]
        self.behaviours = behaviours
        self.name = name
        self.farm = farm
        self.id = random.randint(1, 1000000)
        self.servers = []

    def _next_server_index(self):
        # TODO: handle holes
        return len(self.servers) + 1

    def _pack_user_data(self, user_data):
        return ';'.join(['{0}={1}'.format(k, v) for k, v in user_data.items()])

    def run_server(self, zone='lxc-zone'):
        if not self.farm.started:
            raise Exception("You should start your farm first.")
        server = Server(behaviours=self.behaviours,
                        index=self._next_server_index(),
                        zone=zone,
                        role=self)
        self.servers.append(server)

        server_dir = self.farm.base_dir + '/' + server.id
        os.makedirs(server_dir)

        recipes_for_behaviours_list = self.chef_recipes or self.behaviours
        with open(server_dir + '/Vagrantfile', 'w+') as fp:
            tpl = string.Template(VAGRANT_FILE)
            recipes_subst = "\n".join(["    chef.add_recipe '%s'" % recipe for recipe in recipes_for_behaviours_list])

            # TODO: multi behaviour roles
            fp.write(tpl.substitute(
                user_data=self._pack_user_data(self._user_data(server)),
                recipes_for_behaviours=recipes_subst
                ))

        with open(server_dir + '/Berksfile', 'w+') as fp:
            cookbooks_strs = []
            for recipe in recipes_for_behaviours_list:
                cookbook = '::' in recipe and recipe.split('::')[0] or recipe
                cookbook_str = 'cookbook "{0}", git: repo_pub, rel: "cookbooks/{0}", ref: "HEAD"'.format(cookbook)
                cookbooks_strs.append(cookbook_str)

            fp.write(BERKSHELF_FILE + '\n' + '\n'.join(cookbooks_strs))

        #subprocess.Popen('vagrant init ubuntu1210', shell=True, cwd=server_dir).communicate()
        lxc_start = subprocess.Popen('vagrant up --provider lxc', shell=True, cwd=server_dir)
        lxc_start.communicate()
        if lxc_start.returncode:
            raise Exception('Container start or provisioning failed. ret code: %s' % lxc_start.returncode)

        # TODO: server['machine_id']
        return server


    def _user_data(self, server):
        return {'szr_key': server.crypto_key,
                'hash': server.farm_hash,
                'serverid': server.id,
                'p2p_producer_endpoint': 'http://{0}:{1}/messaging'.format(ROUTER_IP, ROUTER_PORT),
                'queryenv_url': 'http://{0}:{1}/query-env'.format(ROUTER_IP, ROUTER_PORT),
                'behaviors': ','.join(server.behaviours),
                'farm_roleid': '1',
                'roleid': '1',
                'env_id': '1',
                'platform': 'lxc',
                'region': server.zone,
                'server_index': str(server.index),
                'storage_service_url': 'http://{0}:{1}'.format(ROUTER_IP, storage.port)}


    def find_servers(self, pattern=None):
        # TODO: finish else clauses - find server if pattern is not uuid
        ret = []
        if pattern is None:
            ret = [server for server in self.servers
                        if server.status != 'pending']

        elif pattern and isinstance(pattern, (str,unicode)):
            # Assuming it's id
            for server in self.servers:
                if server.id == pattern:
                    ret.append(server)

        elif isinstance(pattern, dict):
            # pattern = dict attrib -> value
            for server in self.servers:
                for find_attr, find_value in pattern.iteritems():
                    real_value = getattr(server, find_attr)
                    if real_value != find_value:
                        break
                else:
                    ret.append(server)

        if ret:
            return ret
        else:
            raise LookupError('Empty results for search servers by pattern: {0}'.format(pattern))


class Habibi(object):

    def __init__(self, base_dir=None):
        #if 'chef' not in behaviors:
        #    behaviors.append('chef')
        self.servers = []
        self.event_mgr = events.NotificationCenter()
        self.queryenv_version = '2012-07-01'
        self.base_dir = base_dir or '.habibi'
        self.RequestHandler.habibi = self
        self.queryenv = QueryEnv(self)
        self.web_server = self.web_server_thread = None
        self.roles = []
        self.farm_crypto_key = crypto.keygen()
        self.started = False


    def add_role(self, name, behaviours):
        role = FarmRole(name, behaviours, self)
        self.roles.append(role)
        return role


    def spy(self, spy):
        for attr_name in dir(spy):
            if not attr_name.startswith('_'):
                attr = getattr(spy, attr_name)
                if callable(attr) and hasattr(attr, '_breakpoint'):
                    self.event_mgr.add_breakpoint(attr._breakpoint, attr)


    def remove_role(self, role):
        if role not in self.roles:
            raise Exception('Role %s not found' % role.name)
        self.roles.remove(role)
        for server in role.servers:
            try:
                server.destroy()
            except:
                LOG.debug('Failed to terminate server %s' % server.id, exc_info=sys.exc_info())


    def start(self):
        self.web_server = BaseHTTPServer.HTTPServer(('', ROUTER_PORT), self.RequestHandler)
        self.web_server_thread = threading.Thread(name='WebServer', 
                                                target=self.web_server.serve_forever)
        self.web_server_thread.setDaemon(True)
        self.web_server_thread.start()

        self.storage_manager = storage.StorageManager(self)
        self.storage_manager.cleanup()
        self.storage_server = simple_server.make_server('0.0.0.0', storage.port, self.storage_manager)
        storage_thread = threading.Thread(target=self.storage_server.serve_forever, name='Storage server')
        storage_thread.setDaemon(True)
        storage_thread.start()

        if os.path.exists(self.base_dir):
            shutil.rmtree(self.base_dir)
        os.makedirs(self.base_dir)
        self.started = True

    def stop(self):
        self.web_server.shutdown()
        self.storage_manager._cleanup()
        self.storage_server.shutdown()
        self.started = False


    def find_servers(self, pattern=None):
        ret = []
        for role in self.roles:
            try:
                role_servers = role.find_servers(pattern)
                ret.extend(role_servers)
            except LookupError:
                pass

        if ret:
            return ret
        else:
            raise LookupError()


    def _send_triggered(self, msg_name, server_pattern=None, source_msg=None, source_server=None):
        servers = self.find_servers(server_pattern)
        #source_server = self.find_servers(source_msg.meta['server_id'])[0]

        for server in servers:
            try:
                msg = Message()
                msg.id = str(uuid.uuid4())
                msg.name = msg_name
                msg.body = source_msg.body.copy() if source_msg else {}
                msg.body['scripts'] = []
                msg.body['behaviour'] = list(server.behaviours)
                fields =  dict(source_msg=source_msg, source_server=source_server)
                self.send(msg, server, fields)

            except:
                e = sys.exc_info()[1]
                LOG.error('Failed to send message to server %s: %s', server.id, e)


    def send(self, msg, server, bpoint_add_fields=None):
        # TODO: remove redundant fields since we have better filters now
        bpoint = {'event': 'before_message_send',
                 'msg_name': msg.name,
                 'target_index': str(server.index),
                 'target_behaviour': server.behaviours[0],
                 'target_msg': msg,
                 'target_server': server}

        if bpoint_add_fields:
            bpoint.update(bpoint_add_fields)
        self.event_mgr.apply_breakpoints(bpoint)

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
        LOG.debug('Incoming message  "%s" from %s', msg.name, server_id)
        server = self.find_servers(server_id)[0]

        self.event_mgr.apply_breakpoints(**{'event': 'incoming_message',
                                                'msg_name': msg.name,
                                                'source_index': str(server.index),
                                                'source_behaviour': server.behaviours[0],
                                                'source_msg': msg, 'source_server': server})

        kwds = dict(source_msg=msg, source_server=server)

        if msg.name == 'HostInit':
            server.status = 'initializing'
            server.public_ip = msg.body['remote_ip']
            server.private_ip = msg.body['local_ip']
            server.crypto_key = msg.body['crypto_key'].strip()

            time.sleep(1)  # It's important gap for Scalarizr
            self._send_triggered('HostInitResponse', server_pattern=server_id, **kwds)
            self._send_triggered('HostInit', **kwds)
        elif msg.name == 'BeforeHostUp':
            self._send_triggered('BeforeHostUp', **kwds)
        elif msg.name == 'HostUp':
            server.status = 'running'
            self._send_triggered('HostUp', **kwds)
        elif msg.name == 'HostDown':
            pass
        elif msg.name == 'BeforeHostDown':
            pass
        elif msg.name in ('OperationDefinition', 'OperationProgress', 'OperationResult'):
            pass


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
        self._handlers = []

    def subscribe(self, function):
        self._handlers.append(function)

    def run(self, operation, fields):
        try:
            method_name = operation.replace('-', '_')
            self.LOG.debug('run %s', operation)
            response = etree.Element('response')
            if hasattr(self, method_name):
                response.append(getattr(self, method_name)(fields))

            for hndlr in self._handlers:
                hndlr(method_name, response, fields)

            return etree.tostring(response)
        except:
            exc_info = sys.exc_info()
            LOG.error('Queryenv error', exc_info=exc_info)
            raise


    def get_latest_version(self, fields):
        ret = etree.Element('version')
        ret.text = self.habibi.queryenv_version
        return ret

    def list_global_variables(self, fields):
        return etree.Element('variables')

    def get_global_config(self, fields):
        ret = etree.Element('settings')
        settings = {'dns.static.endpoint': 'scalr-dns.com',
                    'scalr.version': '4.5.0',
                    'scalr.id': '884c7c0'}
        for key, val in settings.items():
            setting = etree.Element('setting', key=key)
            setting.text = val
            ret.append(setting)
        return ret

    def list_roles(self, fields):
        with_init = fields.getvalue('showInitServers') == '1'
        ret = etree.Element('roles')
        for role in self.habibi.roles:

            role_el = etree.Element('role')
            role_el.attrib.update({'id': str(role.id),
                            'role-id': '1',
                            'behaviour': ','.join(role.behaviours),
                            'name': role.name})
            hosts = etree.Element('hosts')
            role_el.append(hosts)

            for server in role.servers:
                if not with_init and server.status in ('pending', 'initializing'):
                    continue
                host = etree.Element('host')
                host.attrib.update({'internal-ip': server.private_ip,
                                    'external-ip': server.public_ip,
                                    'status': server.status,
                                    'index': str(server.index),
                                    'cloud-location': server.zone})
                hosts.append(host)
            ret.append(role_el)
        return ret

    def get_service_configuration(self, fields):
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



def xml_strip(el):
    for child in list(el.childNodes):
        if child.nodeType == child.TEXT_NODE and child.nodeValue.strip() == '':
            el.removeChild(child)
        else:
            xml_strip(child)
    return el



"""
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
"""

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