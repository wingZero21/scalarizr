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

from scalarizr import rpc
from scalarizr import linux
from telnetlib import Telnet
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.util import initdv2
from scalarizr.util import system2
from scalarizr.util.initdv2 import InitdError
from scalarizr.linux import coreutils, iptables, pkgmgr
from scalarizr.util import wait_until, dynimp, firstmatched
from scalarizr.libs.metaconf import Configuration, NoPathError, strip_quotes

LOG = logging.getLogger(__name__)


__apache__ = __node__['apache']
__apache__.update({
    'httpd.conf'         : '/etc/apache2/apache2.conf' if linux.os.debian_family else '/etc/httpd/conf/httpd.conf',
    'vhosts_dir'         : os.path.join(bus.etc_path, __apache__['vhosts_path']),
    'cert_path'          : os.path.join(bus.etc_path, 'private.d/keys'),
    'vhosts_path'        : 'private.d/vhosts',
    'vhosts_extension'   : '.vhost.conf',
    'logrotate_conf_path':'/etc/logrotate.d/scalarizr_app'})


class ApacheConfig(object):

    _cnf = None
    path = None

    def __init__(self, path):
        self._cnf = Configuration('apache')
        self.path = path

    def __enter__(self):
        self._cnf.read(self.path)
        return self._cnf

    def __exit__(self, type, value, traceback):
        self._cnf.write(self.path)


with ApacheConfig(__apache__['httpd.conf']) as apache_conf:
    server_root = None
    try:
        server_root = apache_conf.get('ServerRoot')
        server_root = strip_quotes(server_root)
        server_root = re.sub(r'^["\'](.+)["\']$', r'\1', server_root)
    except NoPathError,e:
        pass
    finally:
        if not server_root:
            server_root = os.path.dirname(__apache__['httpd.conf'])
            apache_conf.set('ServerRoot', server_root)
        __apache__.update({'server_root': server_root})


class ApacheError(BaseException):
    pass


class ApacheAPI(object):

    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ApacheAPI, cls).__new__(cls, *args, **kwargs)
        return cls._instance


    def __init__(self):
        self.service = initdv2.lookup('apache')
        self.mod_rpaf = ModRPAF()
        self.mod_ssl = ModSSL()


    @rpc.service_method
    def create_vhost(self, hostname, port, template, ssl_certificate_id=None, reload=True):
        if ssl_certificate_id:
            cert = SSLCertificate(ssl_certificate_id)
            cert.ensure()

        body = template.replace('/etc/aws/keys/ssl', __apache__['cert_path'])
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

        if reload:
            self.reload_service()


    @rpc.service_method
    def reload_vhosts(self):
        deployed_vhosts = []
        received_vhosts = self._queryenv.list_virtual_hosts()
        for vhost_data in received_vhosts:
            hostname = vhost_data.hostname
            port = 443 if vhost_data.https else 80

            if vhost_data.https:
                cert = SSLCertificate()
                cert.ensure()
                body = vhost_data.raw.replace('/etc/aws/keys/ssl', __apache__['cert_path'])
                vhost = ApacheVirtualHost(hostname, port, body, cert)
            else:
                vhost = ApacheVirtualHost(hostname, port, vhost_data.raw)
            vhost.ensure()
            deployed_vhosts.append(vhost)

        #cleanup
        vhosts_dir = __apache__['vhosts_dir']
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
    def list_webserver_ssl_certificates(self):
        pass


    def init_service(self):
        self.service.stop('Configuring Apache Web Server')

        self._open_ports(80,443)

        if not os.path.exists(__apache__['vhosts_dir']):
            os.makedirs(__apache__['vhosts_dir'])

        with ApacheConfig(__apache__['httpd.conf']) as apache_conf:
            inc_mask = __apache__['vhosts_dir'] + '/*' + __apache__['vhost_extension']
            if not inc_mask in apache_conf.get_list('Include'):
                apache_conf.add('Include', inc_mask)

        if linux.os.debian_family:
            self.patch_default_conf_deb()
            self.mod_rpaf.fix_module()
        else:
            with ApacheConfig(__apache__['httpd.conf']) as apache_conf:
                if not apache_conf.get_list('NameVirtualHost'):
                    apache_conf.set('NameVirtualHost', '*:80')

        self.create_logrotate_conf(__apache__['logrotate_conf_path'])
        self.mod_ssl.ensure()
        self.mod_rpaf.ensure_permissions()
        self.service.start()


    def clean_vhosts_dir(self):
        for fname in os.listdir(__apache__['vhosts_path']):
            path = os.path.join(__apache__['vhosts_path'], fname)
            if path.endswith(__apache__['vhost_extension']):
                if os.path.isfile(path):
                    os.remove(path)
                elif os.path.islink(path):
                    os.unlink(path)


    def list_served_vhosts(self):
        binary_path = '/usr/sbin/apache2ctl' if linux.os.debian_family else 'usr/sbin/httpd'
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


    def _open_ports(ports):
        if iptables.enabled():
            rules = []
            for port in ports:
                rules.append({"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(port)})
            iptables.FIREWALL.ensure(rules)


    def patch_default_conf_deb(self):
        LOG.debug("Replacing NameVirtualhost and Virtualhost ports specifically for debian-based linux")
        default_vhost_path = os.path.join(
                                os.path.dirname(__apache__['httpd.conf']),
                                'sites-enabled',
                                '000-default')
        if os.path.exists(default_vhost_path):
            with ApacheConfig(default_vhost_path) as default_vhost:
                default_vhost.set('NameVirtualHost', '*:80', force=True)

            dv = None
            with open(default_vhost_path, 'r') as fp:
                dv = fp.read()
            vhost_regexp = re.compile('<VirtualHost\s+\*>')
            dv = vhost_regexp.sub( '<VirtualHost *:80>', dv)
            with open(default_vhost_path, 'w') as fp:
                fp.write(dv)

        else:
            LOG.debug('Cannot find default vhost config file %s. Nothing to patch' % default_vhost_path)


    def create_logrotate_conf(self, path):

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
            if linux.os.debian_family:
                with open(path, 'w') as fp:
                    fp.write(LOGROTATE_CONF_DEB_RAW)
            else:
                with open(path, 'w') as fp:
                    fp.write(LOGROTATE_CONF_REDHAT_RAW)


class ModSSL(object):

    def __init__(self):
        if linux.os.redhat_family:
            base = 'conf.d/ssl.conf'
        else:
            base = 'sites-available/default-ssl'
        self.ssl_conf_path = os.path.join(__apache__['server_root'], base)


    def set_default_certificate(self, cert):
        key_path_default = '/etc/pki/tls/private/localhost.key' if linux.os.redhat_family else '/etc/ssl/private/ssl-cert-snakeoil.key'
        crt_path_default = '/etc/pki/tls/certs/localhost.crt' if linux.os.redhat_family else '/etc/ssl/certs/ssl-cert-snakeoil.pem'

        cert_path = cert.cert_path if cert else None
        pk_path = cert.pk_path  if cert else None
        ca_crt_path = cert.ca_crt_path if cert else None

        self._set('.//SSLCertificateFile', cert_path, crt_path_default)
        self._set('.//SSLCertificateKeyFile', pk_path, key_path_default)

        with ApacheConfig(self.ssl_conf_path) as ssl_conf:

            if not os.path.exists(ca_crt_path):
                try:
                    old_ca_crt_path = ssl_conf.get(".//SSLCertificateChainFile")
                except:
                    old_ca_crt_path = None
                else:
                    if old_ca_crt_path and not os.path.exists(old_ca_crt_path):
                        ssl_conf.comment(".//SSLCertificateChainFile")

            else:
                try:
                    self._set('.//SSLCertificateChainFile', ca_crt_path, force=False)
                except NoPathError:
                    parent = ssl_conf.etree.find('.//SSLCertificateFile/..')
                    before_el = ssl_conf.etree.find('.//SSLCertificateFile')
                    ch = ssl_conf._provider.create_element(ssl_conf.etree, './/SSLCertificateChainFile', ca_crt_path)
                    ch.text = ca_crt_path
                    parent.insert(list(parent).index(before_el), ch)


    def _set(self, section, path, default_path=None, force=True):
        if os.path.exists(self.ssl_conf_path):
            with ApacheConfig(self.ssl_conf_path) as ssl_conf:
                old_path = None
                try:
                    old_path = ssl_conf.get(section)
                except NoPathError, e:
                    pass
                if path and os.path.exists(path):
                    ssl_conf.set(".//SSLCertificateFile", path, force=force)
                elif default_path and old_path and not os.path.exists(old_path):
                    LOG.debug("Certificate file not found. Setting to default %s" % default_path)
                    ssl_conf.set(".//SSLCertificateFile", default_path, force=True)


    def ensure(self, ssl_port=443):
        if linux.os.debian_family:
            self._check_mod_ssl_deb(ssl_port)
        elif linux.os.redhat_family:
            self._check_mod_ssl_redhat(ssl_port)


    def _check_mod_ssl_deb(self, ssl_port=443):
        base = os.path.dirname(__apache__['httpd.conf'])
        ports_conf_path = os.path.join(base, 'ports.conf')
        ssl_load_path = os.path.join(base, 'mods-enabled', 'ssl.load')

        LOG.debug('Ensuring mod_ssl enabled')
        if not os.path.exists(ssl_load_path):
            LOG.info('Enabling mod_ssl')
            system2(('/usr/sbin/a2enmod', 'ssl'))

        LOG.debug('Ensuring NameVirtualHost *:%s' % ssl_port)
        if os.path.exists(ports_conf_path):
            with ApacheConfig(ports_conf_path) as conf:
                i = 0
                for section in conf.get_dict('IfModule'):
                    i += 1
                    if section['value'] in ('mod_ssl.c', 'mod_gnutls.c'):
                        conf.set('IfModule[%d]/Listen' % i, str(ssl_port), True)
                        conf.set('IfModule[%d]/NameVirtualHost' % i, '*:%s'% ssl_port, True)


    def _check_mod_ssl_redhat(self,ssl_port=443):
        mod_ssl_file = os.path.join(__apache__['server_root'], 'modules', 'mod_ssl.so')

        if not os.path.exists(mod_ssl_file):
            LOG.info('%s does not exist. Trying to install' % mod_ssl_file)
            pkgmgr.install('mod_ssl')

        #ssl.conf part
        if not os.path.exists(self.ssl_conf_path):
            raise ApacheError("SSL config %s doesn`t exist", self.ssl_conf_path)

        with ApacheConfig(self.ssl_conf_path) as ssl_conf:
            if ssl_conf.empty:
                LOG.error("SSL config file %s is empty. Filling in with minimal configuration.", self.ssl_conf_path)
                ssl_conf.add('Listen', str(ssl_port))
                ssl_conf.add('NameVirtualHost', '*:%s'% ssl_port)

            else:
                if not ssl_conf.get_list('NameVirtualHost'):
                    LOG.debug("NameVirtualHost directive not found in %s", self.ssl_conf_path)
                    if not ssl_conf.get_list('Listen'):
                        LOG.debug("Listen directive not found in %s. ", self.ssl_conf_path)
                        LOG.debug("Patching %s with Listen & NameVirtualHost directives.",     self.ssl_conf_path)
                        ssl_conf.add('Listen', str(ssl_port))
                        ssl_conf.add('NameVirtualHost', '*:%s'% ssl_port)
                    else:
                        LOG.debug("NameVirtualHost directive inserted after Listen directive.")
                        ssl_conf.add('NameVirtualHost', '*:%s'% ssl_port, 'Listen')

        with ApacheConfig(__apache__['httpd.conf']) as main_config:
            loaded_in_main = [module for module in main_config.get_list('LoadModule') if 'mod_ssl.so' in module]
            if not loaded_in_main:
                if os.path.exists(self.ssl_conf_path):
                    loaded_in_ssl = [module for module in main_config.get_list('LoadModule') if 'mod_ssl.so' in module]
                    if not loaded_in_ssl:
                        main_config.add('LoadModule', 'ssl_module modules/mod_ssl.so')


class ModRPAF(object):

    path = None

    def __init__(self):
        self.path = firstmatched(
                lambda x: os.access(x, os.F_OK),
                ('/etc/httpd/conf.d/mod_rpaf.conf', '/etc/apache2/mods-available/rpaf.conf')
        )
        if not os.path.exists(self.path):
            raise ApacheError('Nothing to do with rpaf: mod_rpaf configuration file not found')


    def add(self, ips):
        with ApacheConfig(self.path) as rpaf:
            proxy_ips = set(re.split(r'\s+', rpaf.get('.//RPAFproxy_ips')))
            proxy_ips |= set(ips)
            if not proxy_ips:
                    proxy_ips.add('127.0.0.1')
            rpaf.set('.//RPAFproxy_ips', ' '.join(proxy_ips))

    def remove(self, ips):
        with ApacheConfig(self.path) as rpaf:
            proxy_ips = set(re.split(r'\s+', rpaf.get('.//RPAFproxy_ips')))
            proxy_ips -= set(ips)
            if not proxy_ips:
                    proxy_ips.add('127.0.0.1')
            rpaf.set('.//RPAFproxy_ips', ' '.join(proxy_ips))


    def update(self, ips):
        with ApacheConfig(self.path) as rpaf:
            proxy_ips = set(ips)
            if not proxy_ips:
                    proxy_ips.add('127.0.0.1')
            rpaf.set('.//RPAFproxy_ips', ' '.join(proxy_ips))


    def fix_module(self):
        #fixing bug in rpaf 0.6-2
        pm = dynimp.package_mgr()
        if '0.6-2' == pm.installed('libapache2-mod-rpaf'):
            LOG.debug('Patching IfModule value in rpaf.conf')
            with ApacheConfig(self.path) as rpaf:
                try:
                    rpaf.set("./IfModule[@value='mod_rpaf.c']", {'value': 'mod_rpaf-2.0.c'})
                except NoPathError:
                    pass


    def ensure_permissions(self):
        st = os.stat(__apache__['httpd.conf'])
        os.chown(self.path, st.st_uid, st.st_gid)


class SSLCertificate(object):

    id = None

    def __init__(self, ssl_certificate_id=None, keys_dir=None):
        self.id = ssl_certificate_id
        self._queryenv = bus.queryenv_service
        self.keys_dir = keys_dir or os.path.join(bus.etc_path, "private.d/keys")


    def update(self, cert, key, cacert=None):

        with open(self.cert_path, 'w') as fp:
            fp.write(cert)

        with open(self.key_path, 'w') as fp:
            fp.write(key)

        if cacert:
            with open(self.cacert, 'w') as fp:
                fp.write(cacert)


    def ensure(self):
        if not os.path.exists(self.cert_path) or not os.path.exists(self.key_path):
            LOG.debug("Retrieving ssl cert and private key from Scalr.")
            cert_data = self._queryenv.get_ssl_certificate(self.id)
            cacert = cert_data[2] if len(cert_data) > 2 else None
            self.update(cert_data[0],cert_data[1],cacert)
        else:
            LOG.debug('Cert files are already in place')


    def delete(self):
        for path in (self.cert_path, self.pk_path):
            if os.path.exists(path):
                os.remove(path)


    @property
    def cert_path(self):
        id = '_' + str(self.id) if self.id else ''
        return os.path.join(self.keys_dir, 'https%s.crt' % id)


    @property
    def key_path(self):
        id = '_' + str(self.id) if self.id else ''
        return os.path.join(self.keys_dir, 'https%s.key' % id)

    @property
    def ca_cert_path(self):
        id = '_' + str(self.id) if self.id else ''
        return os.path.join(self.keys_dir, 'https%s-ca.crt' % id)


class ApacheVirtualHost(object):

    hostname = None
    body = None
    port = None
    cert = None

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
        self.mod_ssl = ModSSL()
        self.hostname = hostname
        self.body = body
        self.port = port
        self.cert = cert


    @property
    def vhost_path(self):
        ext = __apache__['vhost_extension']
        end = ext if not self.cert else '-ssl' + ext
        return os.path.join(bus.etc_path, __apache__['vhosts_path'], self.hostname + end)


    def ensure(self):
        with open(self.vhost_path, 'w') as fp:
            fp.write(self.body)
        self.ensure_document_root()
        if self.cert:
            self.mod_ssl.set_default_certificate(self.cert)
        #TODO: check ssl.conf, debian.conf, etc. if needed


    def delete(self):
        os.remove(self.vhost_path)


    def _get_log_directories(self):
        result = []
        with ApacheConfig(self.vhost_path) as c:
            error_logs = c.get_list('.//ErrorLog')
            custom_logs = c.get_list('.//CustomLog')
        for val in error_logs + custom_logs:
            path = os.path.dirname(val)
            if path not in result:
                result.append(path)
        return result


    def _get_document_root_paths(self):
        result = []
        with ApacheConfig(self.vhost_path) as c:
            for item in c.items('VirtualHost'):
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

                try:
                    pwd.getpwnam('apache')
                    uname = 'apache'
                except:
                    uname = 'www-data'

                coreutils.chown_r(doc_root, uname)
                LOG.debug('Changed owner to %s: %s'
                         % (uname, ', '.join(os.listdir(doc_root))))


class ApacheInitScript(initdv2.ParametrizedInitScript):

    _apachectl = None

    def __init__(self):
        if linux.os.redhat_family:
            self._apachectl = '/usr/sbin/apachectl'
            initd_script    = '/etc/init.d/httpd'
            pid_file        = '/var/run/httpd/httpd.pid' if linux.os["release"].startswith('6') else '/var/run/httpd.pid'
        elif linux.os.debian_family:
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
            pid_file        = '/var/run/apache2.pid'

        initdv2.ParametrizedInitScript.__init__(
                self,
                'apache',
                initd_script,
                pid_file = pid_file
        )


    def reload(self, reason=None):
        if reason:
            LOG.debug('Reloading apache: %s' % str(reason))
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
        bin = '/usr/sbin/apache2' if linux.os.debian_family else '/usr/sbin/httpd'
        group = 'www-data' if linux.os.debian_family else 'apache'
        try:
            out = system2(('ps', '-G', group, '-o', 'command', '--no-headers'), raise_exc=False)[0]
            res = bool([p for p in out.split('\n') if bin in p])
        except:
            pass
        return res


initdv2.explore('apache', ApacheInitScript)

