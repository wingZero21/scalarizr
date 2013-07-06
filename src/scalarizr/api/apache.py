'''
Created on Jun 10, 2013

@author: Dmytro Korsakov
'''

from __future__ import with_statement

import os
import re
import pwd
import time
import shutil
import logging
from telnetlib import Telnet
from scalarizr import rpc
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.util import initdv2
from scalarizr.util import system2
from scalarizr.linux import iptables
from scalarizr.linux import LinuxError, coreutils
from scalarizr.util.initdv2 import InitdError
from scalarizr.util import disttool, wait_until, dynimp, firstmatched
from scalarizr.services import BaseConfig
from scalarizr.libs.metaconf import Configuration, NoPathError, strip_quotes

__apache__ = __node__['apache']

VHOSTS_PATH = 'private.d/vhosts'
VHOST_EXTENSION = '.vhost.conf'
LOGROTATE_CONF_PATH = '/etc/logrotate.d/scalarizr_app'
APACHE_CONF_PATH = '/etc/apache2/apache2.conf' if disttool.is_debian_based() else '/etc/httpd/conf/httpd.conf'

LOG = logging.getLogger(__name__)


class ApacheError(BaseException):
    pass


class ApacheWebServer(object):

    _main_config = None
    _instance = None


    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ApacheWebServer, cls).__new__(cls, *args, **kwargs)
        return cls._instance


    def __init__(self):
        self.service = initdv2.lookup('apache')


    def init_service(self):
        self.service.stop('Configuring Apache Web Server')

        _open_port(80)
        _open_port(443)

        if not os.path.exists(self.vhosts_dir):
            os.makedirs(self.vhosts_dir)

        inc_mask = self.vhosts_dir + '/*' + VHOST_EXTENSION
        if not inc_mask in self._apache_conf.list_includes():
            self._apache_conf.add_include(inc_mask)


        '''
        self.check_mod_ssl()

        if disttool.is_debian_based():
            self._patch_default_conf_deb()
        elif not self._apache.conf.list_name_virtual_hosts():
            self._apache_conf.add_name_virtual_host('*', 90)



        create_logrotate_conf(LOGROTATE_CONF_PATH)
        '''

        self.service.start()


    def clean_vhosts_dir(self):
        for fname in os.listdir(VHOSTS_PATH):
            path = os.path.join(VHOSTS_PATH, fname)
            if path.endswith(VHOST_EXTENSION):
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.islink(path):
                    os.unlink(path)


    def list_served_vhosts(self):
        binary_path = '/usr/sbin/apache2ctl' if disttool.is_debian_based() else 'usr/sbin/httpd'
        d = {}
        host = None
        s = system2((binary_path, '-S'))[0]
        s = s.split('VirtualHost configuration:\n')[1:]
        lines = s[0].split('\n')
        for line in lines:
            line = line.strip()
            if 'wildcard NameVirtualHosts and _default_ servers:' in line:
                pass
            elif 'is a NameVirtualHost' in line:
                host = line.split(' ')[0]
                d[host] = []
            elif line:
                vhost_path = line.split('(')[1]
                vhost_path = vhost_path.split(':')[0]
                if vhost_path not in d[host]:
                    d[host].append(vhost_path)
        return d


    @property
    def _apache_conf(self):
        if not self._main_config:
            self._main_config = HttpdConf(APACHE_CONF_PATH)
        return self._main_config


    @property
    def server_root(self):
        server_root = self._apache_conf.server_root
        if not server_root:
            server_root = os.path.dirname(APACHE_CONF_PATH)
            self.set_server_root(server_root)
        return server_root


    @property
    def vhosts_dir(self):
        return os.path.join(bus.etc_path, VHOSTS_PATH)


    @property
    def cert_path(self):
        return os.path.join(bus.etc_path, 'private.d/keys')


    @property
    def ssl_conf_path(self):
        return os.path.join(self.server_root, 'conf.d/ssl.conf' if disttool.is_redhat_based() else 'sites-available/default-ssl')


    def check_mod_ssl(self):
        if disttool.is_debian_based():
            self._check_mod_ssl_deb()
        elif disttool.is_redhat_based():
            self._check_mod_ssl_redhat()


    def _check_mod_ssl_deb(self):
        base = os.path.dirname(APACHE_CONF_PATH)
        ports_conf_path = os.path.join(base, 'ports.conf')
        ssl_load_path = os.path.join(base, 'mods-enabled', 'ssl.load')


        LOG.debug('Ensuring mod_ssl enabled')
        if not os.path.exists(ssl_load_path):
            LOG.info('Enabling mod_ssl')
            system2(('/usr/sbin/a2enmod', 'ssl'))

        LOG.debug('Ensuring NameVirtualHost *:443')
        if os.path.exists(ports_conf_path):
            conf = Configuration('apache')
            conf.read(ports_conf_path)
            i = 0
            for section in conf.get_dict('IfModule'):
                i += 1
                if section['value'] in ('mod_ssl.c', 'mod_gnutls.c'):
                    conf.set('IfModule[%d]/Listen' % i, '443', True)
                    conf.set('IfModule[%d]/NameVirtualHost' % i, '*:443', True)
            conf.write(ports_conf_path)


    def _check_mod_ssl_redhat(self):
        mod_ssl_file = os.path.join(self.server_root, 'modules', 'mod_ssl.so')

        if not os.path.exists(mod_ssl_file):
            inst_cmd = '/usr/bin/yum -y install mod_ssl'
            LOG.info('%s does not exist. Trying "%s" ' % (mod_ssl_file, inst_cmd))
            system2(inst_cmd, shell=True)

        #ssl.conf part
        ssl_conf_path = os.path.join(self.server_root, 'conf.d', 'ssl.conf')

        if not os.path.exists(ssl_conf_path):
            raise ApacheError("SSL config %s doesn`t exist", ssl_conf_path)

        ssl_conf = Configuration('apache')
        ssl_conf.read(ssl_conf_path)

        if ssl_conf.empty:
            LOG.error("SSL config file %s is empty. Filling in with minimal configuration.", ssl_conf_path)
            ssl_conf.add('Listen', '443')
            ssl_conf.add('NameVirtualHost', '*:443')
            
        else:
            if not ssl_conf.get_list('NameVirtualHost'):
                LOG.debug("NameVirtualHost directive not found in %s", ssl_conf_path)
                if not ssl_conf.get_list('Listen'):
                    LOG.debug("Listen directive not found in %s. ", ssl_conf_path)
                    LOG.debug("Patching %s with Listen & NameVirtualHost directives.",     ssl_conf_path)
                    ssl_conf.add('Listen', '443')
                    ssl_conf.add('NameVirtualHost', '*:443')
                else:
                    LOG.debug("NameVirtualHost directive inserted after Listen directive.")
                    ssl_conf.add('NameVirtualHost', '*:443', 'Listen')
        ssl_conf.write(ssl_conf_path)

        loaded_in_main = [module for module in self._main_config.get_list('LoadModule') if 'mod_ssl.so' in module]

        if not loaded_in_main:
            if os.path.exists(ssl_conf_path):
                loaded_in_ssl = [module for module in self._main_config.get_list('LoadModule') if 'mod_ssl.so' in module]
                if not loaded_in_ssl:
                    self._main_config.add('LoadModule', 'ssl_module modules/mod_ssl.so')
                    self._main_config.write(APACHE_CONF_PATH)


    def patch_ssl_conf(self, cert_id=None):
        #TODO: ADD SNI SUPPORT
        if not cert_id:
            cert_id = 'https'
        key_path = os.path.join(self.cert_path, '%s.key' % cert_id)
        crt_path = os.path.join(self.cert_path, '%s.crt' % cert_id)
        ca_crt_path = os.path.join(self.cert_path, '%s.crt' % cert_id)

        key_path_default = '/etc/pki/tls/private/localhost.key' if disttool.is_redhat_based() else '/etc/ssl/private/ssl-cert-snakeoil.key'
        crt_path_default = '/etc/pki/tls/certs/localhost.crt' if disttool.is_redhat_based() else '/etc/ssl/certs/ssl-cert-snakeoil.pem'


        if os.path.exists(self.ssl_conf_path):
            ssl_conf = Configuration('apache')
            ssl_conf.read(self.ssl_conf_path)

            #removing old paths
            old_crt_path = None
            old_key_path = None
            old_ca_crt_path = None

            try:
                old_crt_path = ssl_conf.get(".//SSLCertificateFile")
            except NoPathError, e:
                pass
            finally:
                if os.path.exists(crt_path):
                    ssl_conf.set(".//SSLCertificateFile", crt_path, force=True)
                elif old_crt_path and not os.path.exists(old_crt_path):
                    LOG.debug("Certificate file not found. Setting to default %s" % crt_path_default)
                    ssl_conf.set(".//SSLCertificateFile", crt_path_default, force=True)

            try:
                old_key_path = ssl_conf.get(".//SSLCertificateKeyFile")
            except NoPathError, e:
                pass
            finally:
                if os.path.exists(key_path):
                    ssl_conf.set(".//SSLCertificateKeyFile", key_path, force=True)
                elif old_key_path and not os.path.exists(old_key_path):
                    LOG.debug("Certificate key file not found. Setting to default %s" % key_path_default)
                    ssl_conf.set(".//SSLCertificateKeyFile", key_path_default, force=True)

            try:
                old_ca_crt_path = ssl_conf.get(".//SSLCertificateChainFile")
            except NoPathError, e:
                pass
            finally:
                if os.path.exists(ca_crt_path):
                    try:
                        ssl_conf.set(".//SSLCertificateChainFile", ca_crt_path)
                    except NoPathError:
                        # XXX: ugly hack
                        parent = ssl_conf.etree.find('.//SSLCertificateFile/..')
                        before_el = ssl_conf.etree.find('.//SSLCertificateFile')
                        ch = ssl_conf._provider.create_element(ssl_conf.etree, './/SSLCertificateChainFile', ca_crt_path)
                        ch.text = ca_crt_path
                        parent.insert(list(parent).index(before_el), ch)
                elif old_ca_crt_path and not os.path.exists(old_ca_crt_path):
                    ssl_conf.comment(".//SSLCertificateChainFile")

            ssl_conf.write(self.ssl_conf_path)


    def rpaf_modify_proxy_ips(self, ips, operation=None):
        LOG.debug('Modify RPAFproxy_ips (operation: %s, ips: %s)', operation, ','.join(ips))
        file = firstmatched(
                lambda x: os.access(x, os.F_OK),
                ('/etc/httpd/conf.d/mod_rpaf.conf', '/etc/apache2/mods-available/rpaf.conf')
        )
        if file:
            rpaf = Configuration('apache')
            rpaf.read(file)

            if operation == 'add' or operation == 'remove':
                proxy_ips = set(re.split(r'\s+', rpaf.get('.//RPAFproxy_ips')))
                if operation == 'add':
                    proxy_ips |= set(ips)
                else:
                    proxy_ips -= set(ips)
            elif operation == 'update':
                proxy_ips = set(ips)
            if not proxy_ips:
                proxy_ips.add('127.0.0.1')

            LOG.info('RPAFproxy_ips: %s', ' '.join(proxy_ips))
            rpaf.set('.//RPAFproxy_ips', ' '.join(proxy_ips))

            #fixing bug in rpaf 0.6-2
            if disttool.is_debian_based():
                pm = dynimp.package_mgr()
                if '0.6-2' == pm.installed('libapache2-mod-rpaf'):
                    try:
                        LOG.debug('Patching IfModule value in rpaf.conf')
                        rpaf.set("./IfModule[@value='mod_rpaf.c']", {'value': 'mod_rpaf-2.0.c'})
                    except NoPathError:
                        pass

            rpaf.write(file)
            st = os.stat(APACHE_CONF_PATH)
            os.chown(file, st.st_uid, st.st_gid)


            self.service.reload('Applying new RPAF proxy IPs list')
        else:
            LOG.debug('Nothing to do with rpaf: mod_rpaf configuration file not found')


class SSLCertificate(object):

    id = None

    def __init__(self, ssl_certificate_id=None):
        self.id = ssl_certificate_id
        self._queryenv = bus.queryenv_service


    def used_by(self):
        '''
        @return:
        list of ApacheVirtualHost objects which use given cert
        '''
        pass


    @property
    def is_orphaned(self):
        return [] == self.used_by()


    def update_ssl_certificate(self, ssl_certificate_id, cert, key, cacert=None):
        if cacert:
            cert = cert + '\n' + cacert

        with open(self.cert_path, 'w') as fp:
            fp.write(cert)

        with open(self.key_path, 'w') as fp:
            fp.write(key)


    def ensure(self):
        LOG.debug("Retrieving ssl cert and private key from Scalr.")
        cert_data = self._queryenv.get_https_certificate(self.id)
        cacert = cert_data[2] if len(cert_data) > 2 else None
        self.update_ssl_certificate(self.id,cert_data[0],cert_data[1],cacert)


    def delete(self):
        for path in (self.cert_path, self.pk_path):
            if os.path.exists(path):
                os.remove(path)


    @property
    def keys_dir(self):
        return os.path.join(bus.etc_path, "private.d/keys")


    @property
    def cert_path(self):
        return os.path.join(self.keys_dir, 'https%s.crt' % '_' + str(self.id) if self.id else '')


    @property
    def key_path(self):
        return os.path.join(self.keys_dir, 'https%s.key' % '_' + str(self.id) if self.id else '')


class ApacheVirtualHost(object):

    hostname = None
    body = None
    port = None
    cert = None

    _config = None
    _instances = None


    def __new__(cls, *args, **kwargs):
        hostname = args[0] if args else kwargs['hostname']
        port = args[1] if len(args) > 1 else kwargs['port']
        if not cls._instances:
            cls._instances = {}
        if (hostname,port) not in cls._instances:
            cls._instances[(hostname,port)] = super(ApacheVirtualHost, cls).__new__(cls,*args,**kwargs)
        return cls._instances[(hostname,port)]


    def __init__(self, hostname, port, body=None, cert=None):
        self.webserver = ApacheWebServer()
        self.hostname = hostname
        self.body = body
        self.port = port
        self.cert = cert


    @classmethod
    def from_file(cls, path):
        c = Configuration('apache')
        c.read(path)
        hostname = c.get('.//ServerName')
        port = c.get('VirtualHost')
        body = open(path).read()
        return ApacheVirtualHost(hostname, port, body)


    @property
    def vhost_path(self):
        end = VHOST_EXTENSION if not self.cert else '-ssl' + VHOST_EXTENSION
        return os.path.join(bus.etc_path, VHOSTS_PATH, self.hostname + end)


    @property
    def _configuration(self):
        if not self._config:
            self._config = Configuration('apache')
            self._config.read(self.vhost_path)
        return self._config


    def ensure(self):
        with open(self.vhost_path, 'w') as fp:
            fp.write(self.body)
        self.ensure_document_root()
        #TODO: check ssl.conf, debian.conf, etc. if needed


    def delete(self):
        os.remove(self.vhost_path)


    def is_deployed(self):
        return self.vhost_path in self.webserver.list_served_vhosts()['*:%d' % self.port]


    def is_like(self, hostname_pattern):
        pass


    def _get_log_directories(self):
        result = []
        error_logs = self._configuration.get_list('.//ErrorLog')
        custom_logs = self._configuration.get_list('.//CustomLog')
        if error_logs:
            result += error_logs
        if custom_logs:
            result += custom_logs
        return result


    def _get_document_root_paths(self):
        result = []
        for item in self._configuration.items('VirtualHost'):
            if item[0]=='DocumentRoot':
                doc_root = item[1][:-1] if item[1][-1]=='/' else item[1]
                result.append(doc_root)
        return result


    def ensure_document_root(self):
        for log_dir in self._get_log_directories():
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)

        for doc_root in self._get_document_root_paths():
            if not os.path.exists(doc_root):

                LOG.debug('Trying to create virtual host document root: %s'
                        % doc_root)

                if not os.path.exists(os.path.dirname(doc_root)):
                    os.makedirs(os.path.dirname(doc_root), 0755)

                shutil.copytree(os.path.join(bus.share_path,
                        'apache/html'), doc_root)
                LOG.debug('Copied documentroot files: %s'
                         % ', '.join(os.listdir(doc_root)))

                uname = get_apache_user()
                coreutils.chown_r(doc_root, uname)
                LOG.debug('Changed owner to %s: %s'
                         % (uname, ', '.join(os.listdir(doc_root))))


class HttpdConf(BaseConfig):

    config_name = os.path.basename(APACHE_CONF_PATH)
    config_type = 'apache'


    def set(self, option, value, append=False):
        self._init_configuration()
        if value:
            if append:
                self.data.add(option, str(value))
            else:
                self.data.set(option,str(value), force=True)
        else:
            self.data.comment(option)
        self._cleanup(True)


    def get_list(self, option):
        self._init_configuration()
        try:
            value = self.data.get_list(option)
        except NoPathError:
            try:
                value = getattr(self, option+'_default')
            except AttributeError:
                value = list()
        self._cleanup(False)
        return value


    def add_name_virtual_host(self, hostname='*', port=80):
        self.set('NameVirtualHost', '%s:%s' % (hostname, port))


    def list_name_virtual_hosts(self):
        return self.get('NameVirtualHost')


    def list_includes(self):
        return self.get_list('Include')


    def add_include(self, path):
        self.set('Include', path, append=True)


    def _set_server_root(self, path):
        self.set('ServerRoot', path)


    def _get_server_root(self):
        server_root = self.get('ServerRoot')
        return strip_quotes(server_root)


    def _list_modules(self):
        pass


    def _add_module(self, module_name):
        pass


    modules = property(_list_modules, _add_module)
    server_root = property(_get_server_root, _set_server_root)


class ApacheAPI(object):


    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ApacheAPI, cls).__new__(cls, *args, **kwargs)
        return cls._instance


    def __init__(self):
        self.service = initdv2.lookup('apache')
        self.webserver = ApacheWebServer()
        self._queryenv = bus.queryenv_service


    @rpc.service_method
    def create_vhost(self, hostname, port, template, ssl_certificate_id=None, reload=True):
        if ssl_certificate_id:
            cert = SSLCertificate(ssl_certificate_id)
            cert.ensure()

        body = template.replace('/etc/aws/keys/ssl', self.webserver.cert_path)
        vhost = ApacheVirtualHost(hostname, port, body, cert)
        vhost.ensure()

        if reload:
            self.reload_service()
            assert vhost in self.list_served_hosts()


    @rpc.service_method
    def delete_vhost(self, hostname_pattern, reload=True):
        for vhost in self.list_served_hosts:
            if vhost.is_like(hostname_pattern):
                vhost.delete()

        for certificate in self.list_webserver_ssl_certificates():
            if certificate.is_orphaned():
                certificate.delete()

        if reload:
            self.reload_service()


    @rpc.service_method
    def update_vhost(self, hostname, new_hostname=None, template=None, ssl_certificate_id=None, port=80, reload=True):
        pass


    @rpc.service_method
    def get_webserver_statistics(self):
        '''
        @return:
        dict of parsed mod_status data

        i.e.
        Current Time
        Restart Time
        Parent Server Generation
        Server uptime
        Total accesses
        CPU Usage
        '''
        pass


    @rpc.service_method
    def list_served_hosts(self, hostname_pattern=None, port=None):
        '''
        @param hostname_pattern: regexp
        @param port: filter by port
        @return: list of ApacheVirtualHost objects according to httpd -S output (apache2ctl -S on Ubuntu)
        #temporary returns dict of "ip:host" : list(vhosts)
        '''
        return self.webserver.list_served_vhosts()


    @rpc.service_method
    def list_webserver_ssl_certificates(self):
        pass


    @rpc.service_method
    def reload_vhosts(self):
        received_vhosts = self._queryenv.list_virtual_hosts()
        deployed_vhosts = []
        for vhost_data in received_vhosts:
            hostname = vhost_data.hostname
            port = 443 if vhost_data.https else 80
            body = vhost_data.raw.replace('/etc/aws/keys/ssl', self.webserver.cert_path)
            if vhost_data.https:
                #prepare SSL Cert
                cert = SSLCertificate()
                cert.ensure()
            else:
                vhost = ApacheVirtualHost(hostname, port, body)
                vhost.ensure()
                deployed_vhosts.append(vhost)

        #cleanup
        vhosts_dir = self.webserver.vhosts_dir
        for fname in os.listdir(vhosts_dir):
            old_vhost_path = os.path.join(vhosts_dir, fname)
            if old_vhost_path not in [vhost.vhost_path for vhost in deployed_vhosts]:
                LOG.debug('Removing old vhost file %s' % old_vhost_path)
                os.remove(old_vhost_path)

        self.service.reload()


    @rpc.service_method
    def start_service(self):
        self.servece.start()


    @rpc.service_method
    def stop_service(self):
        self.service.stop()


    @rpc.service_method
    def reload_service(self):
        self.servece.reload()


    @rpc.service_method
    def restart_service(self):
        self.service.restart()


class ApacheInitScript(initdv2.ParametrizedInitScript):
    _apachectl = None

    def __init__(self):
        if disttool.is_redhat_based():
            self._apachectl = '/usr/sbin/apachectl'
            initd_script    = '/etc/init.d/httpd'
            pid_file                = '/var/run/httpd/httpd.pid' if disttool.version_info()[0] == 6 else '/var/run/httpd.pid'
        elif disttool.is_debian_based():
            self._apachectl = '/usr/sbin/apache2ctl'
            initd_script    = '/etc/init.d/apache2'
            pid_file = None
            if os.path.exists('/etc/apache2/envvars'):
                pid_file = system2('/bin/sh', stdin='. /etc/apache2/envvars; echo -n $APACHE_PID_FILE')[0]
            if not pid_file:
                pid_file = '/var/run/apache2.pid'
        else:
            self._apachectl = '/usr/sbin/apachectl'
            initd_script    = '/etc/init.d/apache2'
            pid_file                = '/var/run/apache2.pid'

        initdv2.ParametrizedInitScript.__init__(
                self,
                'apache',
                initd_script,
                pid_file = pid_file
        )


    def reload(self):
        if self.running:
            self.configtest()
            out, err, retcode = system2(self._apachectl + ' graceful', shell=True)
            if retcode > 0:
                raise initdv2.InitdError('Cannot reload apache: %s' % err)
        else:
            raise InitdError('Service "%s" is not running' % self.name, InitdError.NOT_RUNNING)


    def status(self):
        status = initdv2.ParametrizedInitScript.status(self)
        # If 'running' and socks were passed
        if not status and self.socks:
            ip, port = self.socks[0].conn_address
            try:
                expected = 'server: apache'
                telnet = Telnet(ip, port)
                telnet.write('HEAD / HTTP/1.0\n\n')
                if expected in telnet.read_until(expected, 5).lower():
                    return initdv2.Status.RUNNING
            except EOFError:
                pass
            return initdv2.Status.NOT_RUNNING
        return status


    def configtest(self, path=None):
        args = self._apachectl +' configtest'
        if path:
            args += '-f %s' % path
        out = system2(args, shell=True)[1]
        if 'error' in out.lower():
            raise initdv2.InitdError("Configuration isn't valid: %s" % out)


    def start(self):
        ret = initdv2.ParametrizedInitScript.start(self)
        if self.pid_file:
            try:
                wait_until(lambda: os.path.exists(self.pid_file) or self._main_process_started(), sleep=0.2, timeout=30)
            except (Exception, BaseException), e:
                raise initdv2.InitdError("Cannot start Apache (%s)" % str(e))
        time.sleep(0.5)
        return True


    def stop(self, reason=None):
        if reason:
            LOG.debug('Stopping apache: %s' % str(reason))
        initdv2.ParametrizedInitScript.stop(self)


    def restart(self,reason=None):
        if reason:
            LOG.debug('Restarting apache: %s' % str(reason))
        self.configtest()
        ret = initdv2.ParametrizedInitScript.restart(self)
        if self.pid_file:
            try:
                wait_until(lambda: os.path.exists(self.pid_file), sleep=0.2, timeout=5,
                                error_text="Apache pid file %s doesn't exists" % self.pid_file)
            except:
                raise initdv2.InitdError("Cannot start Apache: pid file %s hasn't been created" % self.pid_file)
        time.sleep(0.5)
        return ret


    def _main_process_started(self):
        res = False
        bin = '/usr/sbin/apache2' if disttool.is_debian_based() else '/usr/sbin/httpd'
        group = 'www-data' if disttool.is_debian_based() else 'apache'
        try:
            out = system2(('ps', '-G', group, '-o', 'command', '--no-headers'), raise_exc=False)[0]
            res = True if len([p for p in out.split('\n') if bin in p]) else False
        except:
            pass
        return res


initdv2.explore('apache', ApacheInitScript)


def _open_port(port):
    if iptables.enabled():
        rule = {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(port)}
        iptables.FIREWALL.ensure([rule])


def _close_port(port):
    if iptables.enabled():
        rule = {"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(port)}
        try:
            iptables.FIREWALL.remove(rule)
        except LinuxError:
            pass


def patch_default_conf_deb():
    LOG.debug("Replacing NameVirtualhost and Virtualhost ports specifically for debian-based linux")
    default_vhost_path = os.path.join(
                            os.path.dirname(APACHE_CONF_PATH),
                            'sites-enabled',
                            '000-default')
    if os.path.exists(default_vhost_path):
        default_vhost = Configuration('apache')
        default_vhost.read(default_vhost_path)
        default_vhost.set('NameVirtualHost', '*:80', force=True)
        default_vhost.write(default_vhost_path)

        dv = None
        with open(default_vhost_path, 'r') as fp:
            dv = fp.read()
        vhost_regexp = re.compile('<VirtualHost\s+\*>')
        dv = vhost_regexp.sub( '<VirtualHost *:80>', dv)
        with open(default_vhost_path, 'w') as fp:
            fp.write(dv)

    else:
        LOG.debug('Cannot find default vhost config file %s. Nothing to patch' % default_vhost_path)


def get_apache_user():
    try:
        pwd.getpwnam('apache')
        uname = 'apache'
    except:
        uname = 'www-data'
    return uname


def create_logrotate_conf(path=LOGROTATE_CONF_PATH):

    LOGROTATE_CONF_REDHAT_RAW = """/var/log/http-*.log {
         missingok
         notifempty
         sharedscripts
         delaycompress
         postrotate
             /sbin/service httpd reload > /dev/null 2>/dev/null || true
         endscript
    }
    """

    LOGROTATE_CONF_DEB_RAW = """/var/log/http-*.log {
             weekly
             missingok
             rotate 52
             compress
             delaycompress
             notifempty
             create 640 root adm
             sharedscripts
             postrotate
                     if [ -f "`. /etc/apache2/envvars ; echo ${APACHE_PID_FILE:-/var/run/apache2.pid}`" ]; then
                             /etc/init.d/apache2 reload > /dev/null
                     fi
             endscript
    }
    """

    if not os.path.exists(path):
        if disttool.is_debian_based():
            with open(path, 'w') as fp:
                fp.write(LOGROTATE_CONF_DEB_RAW)
        else:
            with open(path, 'w') as fp:
                fp.write(LOGROTATE_CONF_REDHAT_RAW)


initdv2.explore('apache', ApacheInitScript)

