"""
Created on Jun 10, 2013

@author: Dmytro Korsakov
"""

from __future__ import with_statement

import os
import re
import sys
import pwd
import time
import shutil
import logging
import urllib2

try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO

from scalarizr import rpc
from scalarizr import linux
from telnetlib import Telnet
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.util import initdv2
from scalarizr.util import system2
from scalarizr.util.initdv2 import InitdError
from scalarizr.linux import coreutils, iptables, pkgmgr
from scalarizr.util import wait_until, dynimp
from scalarizr.libs.metaconf import Configuration, NoPathError


LOG = logging.getLogger(__name__)

etc_path = bus.etc_path or '/etc/scalr'

apache = {
    'vhosts_dir':           os.path.join(etc_path, 'private.d/vhosts'),
    'cert_path':            os.path.join(etc_path, 'private.d/keys'),
    'keys_dir':             os.path.join(etc_path, "private.d/keys"),
    'vhost_extension':      '.vhost.conf',
    'logrotate_conf_path':  '/etc/logrotate.d/scalarizr_app'}

if linux.os.debian_family:
    apache.update({
        'httpd.conf':       '/etc/apache2/apache2.conf',
        'ssl_conf_path':    '/etc/apache2/sites-available/default-ssl',
        'default_vhost':    '/etc/apache2/sites-enabled/000-default',
        'ports_conf_deb':   '/etc/apache2/ports.conf',
        'ssl_load_deb':     '/etc/apache2/mods-enabled/ssl.load',
        'mod_rpaf_path':    '/etc/apache2/mods-available/rpaf.conf',
        'default-ssl_path': '/etc/apache2/sites-enabled/default-ssl',
        'key_path_default': '/etc/ssl/private/ssl-cert-snakeoil.key',
        'crt_path_default': '/etc/ssl/certs/ssl-cert-snakeoil.pem',
        'apachectl':        '/usr/sbin/apache2ctl',
        'bin_path':         '/usr/sbin/apache2',
        'a2enmod_path':     '/usr/sbin/a2enmod',
        'a2ensite_path':    '/usr/sbin/a2ensite',
        'initd_script':     '/etc/init.d/apache2',
        'group':            'www-data',

        'logrotate_conf':   """/var/log/http-*.log {
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
        """})

else:
    apache.update({
        'httpd.conf':       '/etc/httpd/conf/httpd.conf',
        'ssl_conf_path':    '/etc/httpd/conf.d/ssl.conf',
        'default_vhost':    '/etc/httpd/sites-enabled/000-default',
        'mod_ssl_file':     '/etc/httpd/modules/mod_ssl.so',
        'mod_rpaf_path':    '/etc/httpd/conf.d/mod_rpaf.conf',
        'key_path_default': '/etc/pki/tls/private/localhost.key',
        'crt_path_default': '/etc/pki/tls/certs/localhost.crt',
        'apachectl':        '/usr/sbin/apachectl',
        'bin_path': '/usr/sbin/httpd',
        'initd_script':     '/etc/init.d/httpd',
        'group':            'apache',

        'logrotate_conf':   """/var/log/http-*.log {
         missingok
         notifempty
         sharedscripts
         delaycompress
         postrotate
             /sbin/service httpd reload > /dev/null 2>/dev/null || true
         endscript
        }
        """})

__apache__ = __node__['apache']
__apache__.update(apache)


class ApacheError(BaseException):
    pass


class ApacheAPI(object):

    service = None
    mod_ssl = None
    current_open_ports = None

    def __init__(self):
        #TODO: KEEP ALL PORTS IN MEMORY
        self.service = initdv2.lookup('apache')
        self.mod_ssl = ModSSL()
        self.current_open_ports = [80, 443]
        self._query_env = bus.queryenv_service

    @rpc.service_method
    def create_vhost(self, hostname, port, template, ssl, ssl_certificate_id=None, reload=True):
        #TODO: add Listen and NameVirtualHost directives to httpd.conf or ports.conf if needed

        v_host = VirtualHost(template)

        if ssl:
            ssl_certificate = SSLCertificate(ssl_certificate_id)

            if not ssl_certificate.exists():
                ssl_certificate.ensure()

            v_host.use_certificate(
                ssl_certificate.cert_path,
                ssl_certificate.key_path,
                ssl_certificate.chain_path if os.path.exists(ssl_certificate.chain_path) else None
            )

        assert port == v_host.port
        assert hostname == v_host.server_name

        for directory in v_host.document_root_paths:
            path = os.path.dirname(directory)

            if not os.path.exists(path):
                os.makedirs(path, 0755)
                LOG.debug('Created document root %s for %s' % (directory, v_host))

                shutil.copytree(os.path.join(bus.share_path, 'apache/html'), directory)
                files = ', '.join(os.listdir(directory))
                LOG.debug('Copied document root files: %s' % files)

                try:
                    pwd.getpwnam('apache')
                    uname = 'apache'
                except KeyError:
                    uname = 'www-data'

                coreutils.chown_r(directory, uname)
                LOG.debug('Changed owner to %s: %s' % (
                    uname, ', '.join(os.listdir(directory))))

        try:
            path = os.path.dirname(v_host.custom_log_path)
            if not os.path.exists(path):
                os.makedirs(path, 0755)
                LOG.debug('Created CustomLog directory for VirtualHost %s:%s: %s' % (
                    hostname,
                    port,
                    path,
                ))
        except NoPathError:
            LOG.debug('CustomLog directive not found in %s' % v_host)

        try:
            path = os.path.dirname(v_host.error_log_path)
            if not os.path.exists(path):
                os.makedirs(path, 0755)
                LOG.debug('Created ErrorLog directory for VirtualHost %s:%s: %s' % (
                    hostname,
                    port,
                    path,
                ))
        except NoPathError:
            LOG.debug('ErrorLog directive not found in %s' % v_host)

        path = get_virtual_host_path(v_host.server_name, v_host.port)

        if os.path.exists(path) and open(path).read() == v_host.body:
            LOG.debug("Skipping VirtualHost %s: No changes." % v_host)
            return path

        with open(path, 'w') as fp:
            fp.write(v_host.body)
            LOG.debug('VirtualHost %s saved to %s' % (v_host, path))

        if port not in self.current_open_ports:
            try:
                _open_ports([port])
                self.current_open_ports += port
            except (Exception, BaseException):
                pass

        if reload:
            self.reload_service()

        return path

    @rpc.service_method
    def delete_vhosts(self, vhosts, reload=True):
        """
        @param vhosts: [(hostname:password),]
        @param reload: indicates if immediate service reload is needed
        @return:
        """
        for signature in vhosts:
            v_host_path = get_virtual_host_path(*signature)

            if os.path.exists(v_host_path):
                os.remove(v_host_path)
                LOG.debug('Removed VirtualHost %s:%s' % signature)

            else:
                LOG.warning('VirtualHost %s:%s not found.' % signature)

        if reload:
            self.reload_service()

    @rpc.service_method
    def reconfigure(self, vhosts):
        """
        @param vhosts: [(hostname, port, template, ssl, ssl_certificate_id),]
        @return:
        """
        applied_vhosts = []
        for vh_data in vhosts:
            self.create_vhost(*vh_data, reload=False)

            hostname = vh_data[0]
            port = vh_data[1]
            path = get_virtual_host_path(hostname, port)
            applied_vhosts.append(path)

        #cleanup
        vhosts_dir = __apache__['vhosts_dir']
        for fname in os.listdir(vhosts_dir):
            old_vhost_path = os.path.join(vhosts_dir, fname)
            if old_vhost_path not in applied_vhosts:
                LOG.debug('Removing old vhost file %s' % old_vhost_path)
                os.remove(old_vhost_path)
        self.service.reload()

    @rpc.service_method
    def get_webserver_statistics(self):
        """
        @return:
        dict of parsed mod_status data

        i.e.
        Current Time
        Restart Time
        Parent Server Generation
        Server uptime
        Total accesses
        CPU Usage

        The machine readable file can be accessed by using the following link:
        http://your.server.name/server-status?auto

        Available only when mod_stat is enabled
        """
        d = dict()
        try:
            f = urllib2.urlopen('http://127.0.0.1/server-status?auto')
            data = f.read()
        except urllib2.HTTPError, e:
            if '404' in str(e):
                return {'errmsg': 'mod_status is not enabled'}
            return {'errmsg': str(e)}
        except urllib2.URLError, e:
            if '111' in str(e):
                return {'errmsg': 'Connection refused'}
            return {'errmsg': str(e)}
        if data:
            for line in data.split('\n'):
                pairs = line.split(':')
                if len(pairs) > 1:
                    key, value = pairs
                    d[key.strip()] = value.strip()
        return d

    @rpc.service_method
    def start_service(self):
        self.service.start()

    @rpc.service_method
    def stop_service(self):
        self.service.stop()

    @rpc.service_method
    def reload_service(self):
        self.service.reload()

    @rpc.service_method
    def restart_service(self):
        self.service.restart()

    def _fetch_virtual_hosts(self):
        """
        Combines list of virtual hosts in unified format
        regardless of Scalr version.
        @return: list(VirtualHost)
        """
        result = []
        scalr_version = bus.scalr_version or (4, 4, 0)

        if scalr_version < (4, 4):
            raw_data = self._query_env.list_virtual_hosts()

            for virtual_host_data in raw_data:
                v = VirtualHost(virtual_host_data.raw, SSLCertificate())
                assert v.server_name == virtual_host_data.hostname
                assert v.port == 443 if virtual_host_data.https else 80
                result.append(v)

        else:
            raw_data = self._query_env.list_farm_role_params()

            if not 'apache' in raw_data:
                return []

            for virtual_host_data in raw_data['apache']:
                template = virtual_host_data['template']
                ssl = bool(int(virtual_host_data['ssl']))

                if ssl:
                    ssl_certificate_id = virtual_host_data['ssl_certificate_id']
                    ssl_certificate = SSLCertificate(ssl_certificate_id)
                v = VirtualHost(template, ssl_certificate)

                assert v.server_name == virtual_host_data['hostname']
                assert v.port == virtual_host_data['port']
                result.append(v)

        return result

    def reload_virtual_hosts(self):
        vh_data = [(
            virtual_host.server_name,
            virtual_host.port,
            virtual_host.template,
            virtual_host.ssl,
            virtual_host.ssl_certificate_id,
        ) for virtual_host in self._fetch_virtual_hosts()]
        self.reconfigure(vh_data)

    def init_service(self):
        self.service.stop('Configuring Apache Web Server')

        self._open_ports(self.current_open_ports)

        if not os.path.exists(__apache__['vhosts_dir']):
            os.makedirs(__apache__['vhosts_dir'])

        with ApacheConfig(__apache__['httpd.conf']) as apache_config:
            inc_mask = __apache__['vhosts_dir'] + '/*' + __apache__['vhost_extension']
            if not inc_mask in apache_config.get_list('Include'):
                apache_config.add('Include', inc_mask)

        if linux.os.debian_family:
            self.patch_default_conf_deb()
            self.mod_rpaf.fix_module()
        else:
            with ApacheConfig(__apache__['httpd.conf']) as apache_config:
                if not apache_config.get_list('NameVirtualHost'):
                    apache_config.set('NameVirtualHost', '*:80', force=True)

        self.create_logrotate_conf(__apache__['logrotate_conf_path'])
        self.mod_ssl.ensure()
        self.mod_rpaf.ensure_permissions()
        self.service.start()

    def rename_old_virtual_hosts(self):
        vhosts_dir = __apache__['vhosts_dir']
        for fname, new_fname in self.get_updated_file_names(os.listdir(vhosts_dir)):
            os.rename(os.path.join(vhosts_dir, fname), os.path.join(vhosts_dir, new_fname))

    @rpc.service_method
    def update_vhost(self,
                     signature,
                     hostname=None,
                     template=None,
                     ssl=False,
                     ssl_certificate_id=None,
                     port=80,
                     reload=True):

        assert len(signature) == 2
        old_hostname = signature[0]
        old_port = signature[1]

        if hostname:

            old_path = get_virtual_host_path(old_hostname, port)
            new_path = get_virtual_host_path(hostname, port)

            if os.path.exists(old_path):
                os.rename(old_path, new_path)

            if template:
                v_host = VirtualHost(template, ssl, ssl_certificate_id)
                with open(v_host.path, 'w') as fp:
                    fp.write(v_host.body)

            if port:
                with ApacheConfig(new_path) as apache_config:
                    apache_config.set('.//VirtualHost', {'value': '*:%s' % port})

            if ssl_certificate_id:
                cert = SSLCertificate(ssl_certificate_id)
                cert.ensure()
                with ApacheConfig(new_path) as apache_config:
                    apache_config.set('.//SSLCertificateFile', cert.cert_path)
                    apache_config.set('.//SSLCertificateKeyFile', cert.key_path)

                    if not os.path.exists(cert.ca_crt_path):
                        try:
                            old_ca_crt_path = apache_config.get(".//SSLCertificateChainFile")
                        except:
                            old_ca_crt_path = None
                        else:
                            if old_ca_crt_path and not os.path.exists(old_ca_crt_path):
                                apache_config.comment(".//SSLCertificateChainFile")

                    else:
                        try:
                            self._set('.//SSLCertificateChainFile', cert.ca_crt_path, force=False)
                        except NoPathError:
                            parent = apache_config.etree.find('.//SSLCertificateFile/..')
                            before_el = apache_config.etree.find('.//SSLCertificateFile')
                            ch = apache_config._provider.create_element(
                                apache_config.etree,
                                './/SSLCertificateChainFile',
                                cert.ca_crt_path)
                            ch.text = cert.ca_crt_path
                            parent.insert(list(parent).index(before_el), ch)


class VirtualHost(object):

    body = None

    def __init__(self, template):
        self.body = template

    def __repr__(self):
        return "%s:%s" % (self.server_name, self.port)

    def __cmp__(self, other):
        return self.body == other.body

    @property
    def error_log_path(self):
        raw_value = self._cnf.get('.//ErrorLog')
        return raw_value.split(' ')[0]

    @property
    def custom_log_path(self):
        raw_value = self._cnf.get('.//CustomLog')
        return raw_value.split(' ')[0]

    @property
    def ssl_cert_path(self):
        return self._cnf.get('.//SSLCertificateFile')

    @property
    def ssl_key_path(self):
        return self._cnf.get('.//SSLCertificateKeyFile')

    @property
    def ssl_chain_path(self):
        return self._cnf.get('.//SSLCertificateChainFile')

    @property
    def document_root_paths(self):
        doc_roots = []
        for item in self._cnf.items('.//VirtualHost'):
            if 'DocumentRoot' == item[0]:
                doc_root = item[1][:-1] if item[1][-1] == '/' else item[1]
                doc_roots.append(doc_root)
        return doc_roots

    def use_certificate(self, cert_path, key_path, chain_path=None):
        mem_config = self._cnf

        assert mem_config.get('.//SSLCertificateFile')

        mem_config.set('.//SSLCertificateFile', cert_path)
        mem_config.set('.//SSLCertificateKeyFile', key_path)

        if chain_path:
            try:
                mem_config.set('.//SSLCertificateChainFile', chain_path, force=False)
            except NoPathError:
                parent = mem_config.etree.find('.//SSLCertificateFile/..')
                before_el = mem_config.etree.find('.//SSLCertificateFile')
                ch = mem_config._provider.create_element(
                    mem_config.etree,
                    './/SSLCertificateChainFile',
                    chain_path)
                ch.text = chain_path
                parent.insert(list(parent).index(before_el), ch)
        else:
            mem_config.comment('.//SSLCertificateChainFile')

        self._update_body(mem_config)

    @property
    def _cnf(self):
        cnf = Configuration('apache')
        cnf.readfp(StringIO(self.body.strip()))
        return cnf

    def _update_body(self, config_obj):
        tmp_obj = StringIO()
        config_obj.write_fp(tmp_obj, close=False)
        self.body = tmp_obj.getvalue()

    def _get_port(self):
        raw_host = self._cnf.get(".//VirtualHost").split(':')
        if len(raw_host) > 1 and raw_host[1].isdigit():
            return int(raw_host[1])
        elif self.ssl_cert_path:
            return 443
        else:
            return 80

    def _set_port(self, port):
        mem_config = self._cnf
        old_value = mem_config.get('.//VirtualHost')
        host = old_value.split(':')[0]
        new_value = '%s:%s' % (host, port)
        mem_config.set('.//VirtualHost', new_value)
        self._update_body(mem_config)

    def _get_server_name(self):
        return self._cnf.get('.//ServerName')

    def _set_server_name(self, new_name):
        mem_config = self._cnf
        mem_config.set('.//ServerName', new_name)
        self._update_body(mem_config)

    server_name = property(_get_server_name, _set_server_name)

    port = property(_get_port, _set_port)


class ModRPAF(object):

    path = None

    @staticmethod
    def add(ips):
        with ApacheConfig(__apache__['mod_rpaf_path']) as rpaf:
            proxy_ips = set(re.split(r'\s+', rpaf.get('.//RPAFproxy_ips')))
            proxy_ips |= set(ips)
            if not proxy_ips:
                    proxy_ips.add('127.0.0.1')
            rpaf.set('.//RPAFproxy_ips', ' '.join(proxy_ips))

    @staticmethod
    def remove(ips):
        with ApacheConfig(__apache__['mod_rpaf_path']) as rpaf:
            proxy_ips = set(re.split(r'\s+', rpaf.get('.//RPAFproxy_ips')))
            proxy_ips -= set(ips)
            if not proxy_ips:
                    proxy_ips.add('127.0.0.1')
            rpaf.set('.//RPAFproxy_ips', ' '.join(proxy_ips))

    @staticmethod
    def update(ips):
        with ApacheConfig(__apache__['mod_rpaf_path']) as rpaf:
            proxy_ips = set(ips)
            if not proxy_ips:
                    proxy_ips.add('127.0.0.1')
            rpaf.set('.//RPAFproxy_ips', ' '.join(proxy_ips))

    @staticmethod
    def fix_module():
        """
        fixing bug in rpaf 0.6-2
        """
        pm = dynimp.package_mgr()
        if '0.6-2' == pm.installed('libapache2-mod-rpaf'):
            LOG.debug('Patching IfModule value in rpaf.conf')
            with ApacheConfig(__apache__['mod_rpaf_path']) as rpaf:
                try:
                    rpaf.set("./IfModule[@value='mod_rpaf.c']", {'value': 'mod_rpaf-2.0.c'})
                except NoPathError:
                    pass

    @staticmethod
    def ensure_permissions():
        st = os.stat(__apache__['httpd.conf'])
        os.chown(__apache__['mod_rpaf_path'], st.st_uid, st.st_gid)


class SSLCertificate(object):

    id = None

    def __init__(self, ssl_certificate_id=None):
        self.id = ssl_certificate_id

    def update(self, cert, key, authority=None):

        with open(self.cert_path, 'w') as fp:
            fp.write(cert)

        with open(self.key_path, 'w') as fp:
            fp.write(key)

        if authority:
            with open(self.chain_path, 'w') as fp:
                fp.write(authority)

    def ensure(self):
        #TODO: check if certificate files exist and contain the same data
        LOG.debug("Retrieving ssl cert and private key from Scalr.")
        query_env = bus.queryenv_service
        cert_data = query_env.get_ssl_certificate(self.id)
        print cert_data
        authority = cert_data[2] if len(cert_data) > 2 else None
        self.update(cert_data[0], cert_data[1], authority)

    def delete(self):
        for path in (self.cert_path, self.key_path):
            if os.path.exists(path):
                os.remove(path)

    @property
    def cert_path(self):
        id = '_' + str(self.id) if self.id else ''
        return os.path.join(__apache__['keys_dir'], 'https%s.crt' % id)

    @property
    def key_path(self):
        id = '_' + str(self.id) if self.id else ''
        return os.path.join(__apache__['keys_dir'], 'https%s.key' % id)

    @property
    def chain_path(self):
        id = '_' + str(self.id) if self.id else ''
        return os.path.join(__apache__['keys_dir'], 'https%s-ca.crt' % id)

    def exists(self):
        return os.path.exists(self.cert_path) and os.path.exists(self.key_path)


class ModSSL(object):

    def set_default_certificate(self, cert):
        #TODO: TRY TO REWRITE USING ApacheVirtualHost()
        cert_path = cert.cert_path if cert else None
        key_path = cert.key_path if cert else None
        ca_crt_path = cert.ca_crt_path if cert else None

        self._set('.//SSLCertificateFile', cert_path, __apache__['crt_path_default'])
        self._set('.//SSLCertificateKeyFile', key_path, __apache__['key_path_default'])

        with ApacheConfig(__apache__['ssl_conf_path']) as ssl_conf:

            if not os.path.exists(ca_crt_path):
                try:
                    old_ca_crt_path = ssl_conf.get(".//SSLCertificateChainFile")
                except NoPathError:
                    pass
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
        if os.path.exists(__apache__['ssl_conf_path']):
            with ApacheConfig(__apache__['ssl_conf_path']) as ssl_conf:
                old_path = None
                try:
                    old_path = ssl_conf.get(section)
                except NoPathError:
                    pass
                if path and os.path.exists(path):
                    ssl_conf.set(section, path, force=force)
                elif default_path and old_path and not os.path.exists(old_path):
                    LOG.debug("Certificate file not found. Setting to default %s" % default_path)
                    ssl_conf.set(section, default_path, force=True)

    def ensure(self, ssl_port=443):
        if linux.os.debian_family:
            self._check_mod_ssl_deb(ssl_port)
        elif linux.os.redhat_family:
            self._check_mod_ssl_redhat(ssl_port)

    def _check_mod_ssl_deb(self, ssl_port=443):

        LOG.debug('Ensuring mod_ssl enabled')
        if not os.path.exists(__apache__['ssl_load_deb']):
            LOG.info('Enabling mod_ssl')
            system2((__apache__['a2enmod_path'], 'ssl'))

        if not os.path.exists(__apache__['default-ssl_path']):
            LOG.debug('Enabling default SSL virtualhost')
            system2((__apache__['a2ensite_path'], 'default-ssl'))

        LOG.debug('Ensuring NameVirtualHost *:%s' % ssl_port)
        if os.path.exists(__apache__['ports_conf_deb']):
            with ApacheConfig(__apache__['ports_conf_deb']) as conf:
                i = 0
                for section in conf.get_dict('IfModule'):
                    i += 1
                    if section['value'] in ('mod_ssl.c', 'mod_gnutls.c'):
                        conf.set('IfModule[%d]/Listen' % i, str(ssl_port), True)
                        conf.set('IfModule[%d]/NameVirtualHost' % i, '*:%s' % ssl_port, True)

    def _check_mod_ssl_redhat(self, ssl_port=443):
        ssl_conf_path = __apache__['ssl_conf_path']

        if not os.path.exists(__apache__['mod_ssl_file']):
            LOG.info('%s does not exist. Trying to install' % __apache__['mod_ssl_file'])
            pkgmgr.install('mod_ssl')

        #ssl.conf part
        if not os.path.exists(ssl_conf_path):
            raise ApacheError("SSL config %s doesn`t exist", ssl_conf_path)

        with ApacheConfig(ssl_conf_path) as ssl_conf:
            if ssl_conf.empty:
                LOG.error("SSL config file %s is empty. Filling in with minimal configuration.", ssl_conf_path)
                ssl_conf.add('Listen', str(ssl_port))
                ssl_conf.add('NameVirtualHost', '*:%s' % ssl_port)

            else:
                if not ssl_conf.get_list('NameVirtualHost'):
                    LOG.debug("NameVirtualHost directive not found in %s", ssl_conf_path)
                    if not ssl_conf.get_list('Listen'):
                        LOG.debug("Listen directive not found in %s. ", ssl_conf_path)
                        LOG.debug("Patching %s with Listen & NameVirtualHost directives.",     ssl_conf_path)
                        ssl_conf.add('Listen', str(ssl_port))
                        ssl_conf.add('NameVirtualHost', '*:%s' % ssl_port)
                    else:
                        LOG.debug("NameVirtualHost directive inserted after Listen directive.")
                        ssl_conf.add('NameVirtualHost', '*:%s' % ssl_port, 'Listen')

        with ApacheConfig(__apache__['httpd.conf']) as main_config:
            loaded_in_main = [module for module in main_config.get_list('LoadModule') if 'mod_ssl.so' in module]
            if not loaded_in_main:
                if os.path.exists(ssl_conf_path):
                    loaded_in_ssl = [module for module in main_config.get_list('LoadModule') if 'mod_ssl.so' in module]
                    if not loaded_in_ssl:
                        main_config.add('LoadModule', 'ssl_module modules/mod_ssl.so')


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


class ApacheInitScript(initdv2.ParametrizedInitScript):

    _apachectl = None

    def __init__(self):
        pid_file = None
        if linux.os.redhat_family:
            pid_file = '/var/run/httpd/httpd.pid' if linux.os["release"].version[0] == 6 else '/var/run/httpd.pid'
        elif linux.os.debian_family:
            pid_file = None
            if os.path.exists('/etc/apache2/envvars'):
                pid_file = system2('/bin/sh', stdin='. /etc/apache2/envvars; echo -n $APACHE_PID_FILE')[0]
            if not pid_file:
                pid_file = '/var/run/apache2.pid'

        initdv2.ParametrizedInitScript.__init__(
            self,
            'apache',
            __apache__['initd_script'],
            pid_file=pid_file,
        )

    def reload(self, reason=None):
        if reason:
            LOG.debug('Reloading apache: %s' % str(reason))
        if self.running:
            self.configtest()
            out, err, retcode = system2(__apache__['apachectl'] + ' graceful', shell=True)
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
        args = __apache__['apachectl'] + ' configtest'
        if path:
            args += '-f %s' % path
        out = system2(args, shell=True)[1]
        if 'error' in out.lower():
            raise initdv2.InitdError("Configuration isn't valid: %s" % out)

    def start(self):
        initdv2.ParametrizedInitScript.start(self)
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

    def restart(self, reason=None):
        if reason:
            LOG.debug('Restarting apache: %s' % str(reason))
        self.configtest()
        ret = initdv2.ParametrizedInitScript.restart(self)
        if self.pid_file:
            try:
                wait_until(
                    lambda: os.path.exists(self.pid_file),
                    sleep=0.2,
                    timeout=5,
                    error_text="Apache pid file %s doesn't exists" % self.pid_file)
            except:
                raise initdv2.InitdError("Cannot start Apache: pid file %s hasn't been created" % self.pid_file)
        time.sleep(0.5)
        return ret

    @staticmethod
    def _main_process_started():
        res = False
        try:
            out = system2(('ps', '-G', __apache__['group'], '-o', 'command', '--no-headers'), raise_exc=False)[0]
            res = __apache__['bin_path'] in out
        except (Exception, BaseException):
            pass
        return res


initdv2.explore('apache', ApacheInitScript)


def list_served_virtual_hosts():
    """
    @return: [file_name,]
    """
    #TODO: make it API call
    text = system2((__apache__['apachectl'], '-S'))[0]
    directory = __apache__['vhosts_dir']
    ext = __apache__['vhost_extension']
    result = []

    for file_name in os.listdir(directory):
        path = os.path.join(directory, file_name)
        if path.endswith(ext) and path in text:
            result.append(file_name)

    return result


def _open_ports(ports):
    if iptables.enabled():
        rules = []
        for port in ports:
            rules.append({"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(port)})
        iptables.FIREWALL.ensure(rules)


def patch_default_conf_deb():
    LOG.debug("Replacing NameVirtualhost and Virtualhost ports specifically for debian-based linux")
    if os.path.exists(__apache__['default_vhost']):
        with ApacheConfig(__apache__['default_vhost']) as default_vhost:
            default_vhost.set('NameVirtualHost', '*:80', force=True)

        with open(__apache__['default_vhost'], 'r') as fp:
            dv = fp.read()
        vhost_regexp = re.compile('<VirtualHost\s+\*>')
        dv = vhost_regexp.sub('<VirtualHost *:80>', dv)
        with open(__apache__['default_vhost'], 'w') as fp:
            fp.write(dv)

    else:
        LOG.debug('Cannot find default vhost config file %s. Nothing to patch' % __apache__['default_vhost'])


def create_logrotate_conf(path):
    if not os.path.exists(path):
        with open(path, 'w') as fp:
            fp.write(__apache__['logrotate_conf'])


def get_virtual_host_path(hostname, port):
    ext = __apache__['vhost_extension']
    end = '%s-%s%s' % (hostname, port, ext)
    return os.path.join(__apache__['vhosts_dir'], end)


def get_updated_file_names(virtual_host_file_names):
    ext = __apache__['vhost_extension']

    plaintext_pattern = re.compile('(.+)\.vhost.conf')
    ssl_pattern = re.compile('(.+)-ssl%s' % ext)
    newstyle_pattern = re.compile('(\d+)%s' % ext)

    pairs = {}
    for fname in virtual_host_file_names:
        new_fname = None

        if fname.endswith('-ssl%s' % ext):
            res = ssl_pattern.search(fname)
            if res:
                new_fname = res.group(1) + '-443' + ext

        elif fname.endswith(ext):
            res = newstyle_pattern.search(fname)
            if res:
                continue
            else:
                res = plaintext_pattern.search(fname)
                new_fname = res.group(1) + '-80' + ext
        else:
            continue
        pairs[fname] = new_fname
    return pairs
