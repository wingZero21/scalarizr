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

    def restart(self):
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
            '''
            _first_ scalarizr start returns error:
            (ps (code: 1) <out>:  <err>:  <args>: ('ps', '-G', 'www-data', '-o', 'command', '--no-headers')
            '''
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


class ApacheWebServer(object):

    _main_config = None

    def __init__(self):
        self.service = initdv2.lookup('apache')


    @property
    def apache_conf(self):
        if not self._main_config:
            self._main_config = Configuration('apache')
            self._main_config.read(APACHE_CONF_PATH)
        return self._main_config


    @property
    def server_root(self):
        if disttool.is_debian_based():
            server_root = '/etc/apache2'

        elif disttool.is_redhat_based():
            LOG.debug("Searching in apache config file %s to find server root", APACHE_CONF_PATH)

            try:
                server_root = strip_quotes(self._config.get('ServerRoot'))
                server_root = re.sub(r'^["\'](.+)["\']$', r'\1', server_root)
            except NoPathError:
                LOG.warning("ServerRoot not found in apache config file %s", APACHE_CONF_PATH)
                server_root = os.path.dirname(APACHE_CONF_PATH)
                LOG.debug("Use %s as ServerRoot", server_root)
        return server_root


    @property
    def vhost_path(self):
        vhosts_path =  os.path.join(bus.etc_path, VHOSTS_PATH)
        if not os.path.exists(vhosts_path):
                os.makedirs(vhosts_path)
        return vhosts_path


    def _patch_default_conf_deb(self):
        LOG.debug("Replacing NameVirtualhost and Virtualhost ports especially for debian-based linux")
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


    def _check_mod_ssl(self):
        if disttool.is_debian_based():
            self._check_mod_ssl_deb()
        elif disttool.is_redhat_based():
            self._check_mod_ssl_redhat()


    def _check_mod_ssl_deb(self):
        base = os.path.dirname(APACHE_CONF_PATH)

        path = {}
        path['ports.conf'] = base + '/ports.conf'
        path['mods-available'] = base + '/mods-available'
        path['mods-enabled'] = base + '/mods-enabled'
        path['mods-available/ssl.conf'] = path['mods-available'] + '/ssl.conf'
        path['mods-available/ssl.load'] = path['mods-available'] + '/ssl.load'
        path['mods-enabled/ssl.conf'] = path['mods-enabled'] + '/ssl.conf'
        path['mods-enabled/ssl.load'] = path['mods-enabled'] + '/ssl.load'

        LOG.debug('Ensuring mod_ssl enabled')
        if not os.path.exists(path['mods-enabled/ssl.load']):
            LOG.info('Enabling mod_ssl')
            system2(('/usr/sbin/a2enmod', 'ssl'))

        LOG.debug('Ensuring NameVirtualHost *:443')
        if os.path.exists(path['ports.conf']):
            conf = Configuration('apache')
            conf.read(path['ports.conf'])
            i = 0
            for section in conf.get_dict('IfModule'):
                i += 1
                if section['value'] in ('mod_ssl.c', 'mod_gnutls.c'):
                    conf.set('IfModule[%d]/Listen' % i, '443', True)
                    conf.set('IfModule[%d]/NameVirtualHost' % i, '*:443', True)
            conf.write(path['ports.conf'])


    def _check_mod_ssl_redhat(self):
        mod_ssl_file = os.path.join(self.server_root, 'modules', 'mod_ssl.so')

        if not os.path.exists(mod_ssl_file):

            inst_cmd = '/usr/bin/yum -y install mod_ssl'
            LOG.info('%s does not exist. Trying "%s" ' % (mod_ssl_file, inst_cmd))
            system2(inst_cmd, shell=True)

        else:
            #ssl.conf part
            ssl_conf_path = os.path.join(self.server_root, 'conf.d', 'ssl.conf')

            if not os.path.exists(ssl_conf_path):
                LOG.error("SSL config %s doesn`t exist", ssl_conf_path)
            else:
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


    def _patch_ssl_conf(self, cert_path):
        #TODO: ADD SNI SUPPORT

        key_path = os.path.join(cert_path, 'https.key')
        crt_path = os.path.join(cert_path, 'https.crt')
        ca_crt_path = os.path.join(cert_path, 'https-ca.crt')

        key_path_default = '/etc/pki/tls/private/localhost.key' if disttool.is_redhat_based() else '/etc/ssl/private/ssl-cert-snakeoil.key'
        crt_path_default = '/etc/pki/tls/certs/localhost.crt' if disttool.is_redhat_based() else '/etc/ssl/certs/ssl-cert-snakeoil.pem'

        ssl_conf_path = os.path.join(self.server_root, 'conf.d/ssl.conf' if disttool.is_redhat_based() else 'sites-available/default-ssl')
        if os.path.exists(ssl_conf_path):
            ssl_conf = Configuration('apache')
            ssl_conf.read(ssl_conf_path)

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

            ssl_conf.write(ssl_conf_path)


    def _rpaf_modify_proxy_ips(self, ips, operation=None):
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
            st = os.stat(self.APACHE_CONF_PATH)
            os.chown(file, st.st_uid, st.st_gid)


            self.service.reload('Applying new RPAF proxy IPs list')
        else:
            LOG.debug('Nothing to do with rpaf: mod_rpaf configuration file not found')


class SSLCertificate(object):


    def __init__(self, ssl_certificate_id):
        pass

    def used_by(self):
        '''
        @return:
        list of ApacheVirtualHost objects which use given cert
        '''
        pass

    @property
    def is_orphaned(self):
        return [] == self.used_by()


    def ensure(self):
        pass


    def delete(self):
        pass


class ApacheVirtualHost(object):

    hostname = None
    template = None
    port = None
    cert = None

    _config = None

    def __init__(self, hostname, template, port, cert):
        self.hostname = hostname
        self.template = template
        self.port = port
        self.cert = cert


    @classmethod
    def from_file(cls):
        pass


    @property
    def vhost_path(self):
        vhosts_dir =  os.path.join(bus.etc_path, VHOSTS_PATH)
        end = VHOST_EXTENSION if not self.cert else '-ssl' + VHOST_EXTENSION
        return os.path.join(bus.etc_path, VHOSTS_PATH, self.hostname + end)


    @property
    def _configuration(self):
        if not self._configuration:
            self._config = Configuration('apache')
            self._config.read(self.vhost_path)
        return self._config


    def ensure(self):
        pass


    def delete(self):
        pass


    def is_like(self, hostname_pattern):
        pass


    def _create_vhost_paths(self):
        #TODO: Split method in two
        error_logs = self._configuration.get_list('.//ErrorLog')
        custom_logs = self._configuration.get_list('.//CustomLog')
        list_logs = error_logs + custom_logs

        dir_list = []
        for log_file in list_logs:
            log_dir = os.path.dirname(log_file)
            if (log_dir not in dir_list) and (not os.path.exists(log_dir)):
                dir_list.append(log_dir)

        for log_dir in dir_list:
            os.makedirs(log_dir)

            for item in self._configuration.items('VirtualHost'):
                if item[0]=='DocumentRoot':
                    doc_root = item[1][:-1] if item[1][-1]=='/' else item[1]
                    if not os.path.exists(doc_root):
                        LOG.debug('Trying to create virtual host: %s'
                                % doc_root)

                        uname = get_apache_user()

                        LOG.debug('User name: %s' % uname)
                        tmp = '/'.join(doc_root.split('/')[:-1])
                        LOG.debug('Trying to create directories:'
                                ' %s' % tmp)
                        if not os.path.exists(tmp):
                            os.makedirs(tmp, 0755)
                            LOG.debug('Created parent directories:'
                                    ' %s' % tmp)
                        shutil.copytree(os.path.join(bus.share_path,
                                'apache/html'), doc_root)
                        LOG.debug('Copied documentroot files: %s'
                                 % ', '.join(os.listdir(doc_root)))
                        coreutils.chown_r(doc_root, uname)
                        LOG.debug('Changed owner to %s: %s'
                                 % (uname, ', '.join(os.listdir(doc_root))))


class ApacheAPI(object):


    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ApacheAPI, cls).__new__(cls, *args, **kwargs)
        return cls._instance


    def __init__(self):
        self.service = initdv2.lookup('apache')
        self._queryenv = bus.queryenv_service


    @rpc.service_method
    def create_vhost(self, hostname, template, ssl_certificate_id=None, port=80, reload=True):
        if ssl_certificate_id:
            cert = SSLCertificate(ssl_certificate_id)
            cert.ensure()

        vhost = ApacheVirtualHost(hostname, template, port, cert)
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
    def update_vhost(self, hostname, template, new_template, ssl_certificate_id=None, port=80, reload=True):
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
    def list_served_hosts(hostname_pattern=None, port=None):
        '''

        @param hostname_pattern: regexp
        @param port: filter by port
        @return: list of ApacheVirtualHost objects according to httpd -S output (apache2ctl -S on Ubuntu)
        #temporary returns dict of "ip:host" : list(vhosts)
        '''
        d = {}
        host = None
        s  = system2('httpd -S') #apache2ctl -S on Debian
        s1 = s.split('\nSyntax OK')[0]
        s2 = s1.split('VirtualHost configuration:\n')[1:]
        lines = s2[0].split('\n')
        for line in lines:
            line = line.strip()
            if 'is a NameVirtualHost' in line:
                host = line.split(' ')[0]
                d[host] = []
            else:
                vhost_path = line.split('(')[1]
                vhost_path = vhost_path.split(':')[0]
                d[host].append(vhost_path)
        return d


    @rpc.service_method
    def list_webserver_ssl_certificates(self):
        pass


    @rpc.service_method
    def reload_vhosts(self):
        served_hosts = self.list_served_hosts()
        for queryenv_vhost in self._queryenv.list_virtualhosts():
            for apache_vhost in served_hosts:
                if apache_vhost.is_like(queryenv_vhost.hostname):
                    apache_vhost.ensure()
            else:
                if queryenv_vhost.ssl:
                    cert = SSLCertificate(queryenv_vhost.ssl_certificate_id)
                    cert.ensure()
                    vhost = ApacheVirtualHost(queryenv_vhost.hostname, ssl_template=queryenv_vhost.raw, ssl_port=queryenv_vhost.port, cert)
                else:
                    vhost = ApacheVirtualHost(queryenv_vhost.hostname, queryenv_vhost.raw, port=queryenv_vhost.port)
                vhost.ensure()



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


class HttpdConf(BaseConfig):

    config_name = os.path.basename(APACHE_CONF_PATH)

    def _list_name_virtual_hosts(self):
        pass

    def _add_name_virtual_host(self, nvhost):
        pass

    def _list_includes(self):
        pass

    def add_include(self, path):
        pass

    def _list_modules(self):
        pass

    def _add_module(self, module_name):
        pass

    name_virtual_hosts = property(_list_name_virtual_hosts, _add_name_virtual_host)
    includes = property(_list_includes, add_include)
    name_virtual_hosts = property(_list_modules, _add_module)


def get_apache_user():
    try:
        pwd.getpwnam('apache')
        uname = 'apache'
    except:
        uname = 'www-data'
    return uname


def create_logrotate_conf():

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

    if not os.path.exists(LOGROTATE_CONF_PATH):
        if disttool.is_debian_based():
            with open(LOGROTATE_CONF_PATH, 'w') as fp:
                fp.write(LOGROTATE_CONF_DEB_RAW)
        else:
            with open(LOGROTATE_CONF_PATH, 'w') as fp:
                fp.write(LOGROTATE_CONF_REDHAT_RAW)

initdv2.explore('apache', ApacheInitScript)