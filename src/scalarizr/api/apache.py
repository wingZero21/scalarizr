"""
Created on Jun 10, 2013

@author: Dmytro Korsakov
"""

from __future__ import with_statement

import os
import re
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

etc_path = bus.etc_path or "/etc/scalr"

apache = {
    "vhosts_dir":           os.path.join(etc_path, "private.d/vhosts"),
    "keys_dir":             os.path.join(etc_path, "private.d/keys"),
    "vhost_extension":      ".vhost.conf",
    "logrotate_conf_path":  "/etc/logrotate.d/scalarizr_app"}

if linux.os.debian_family:
    apache.update({
        "httpd.conf":       "/etc/apache2/apache2.conf",
        "ssl_conf_path":    "/etc/apache2/sites-available/default-ssl",
        "default_vhost":    "/etc/apache2/sites-enabled/000-default",
        "ports_conf_deb":   "/etc/apache2/ports.conf",
        "ssl_load_deb":     "/etc/apache2/mods-enabled/ssl.load",
        "mod_rpaf_path":    "/etc/apache2/mods-available/rpaf.conf",
        "default-ssl_path": "/etc/apache2/sites-enabled/default-ssl",
        "key_path_default": "/etc/ssl/private/ssl-cert-snakeoil.key",
        "crt_path_default": "/etc/ssl/certs/ssl-cert-snakeoil.pem",
        "apachectl":        "/usr/sbin/apache2ctl",
        "bin_path":         "/usr/sbin/apache2",
        "a2enmod_path":     "/usr/sbin/a2enmod",
        "a2ensite_path":    "/usr/sbin/a2ensite",
        "initd_script":     "/etc/init.d/apache2",
        "group":            "www-data",

        "logrotate_conf":   """/var/log/http-*.log {
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
        "httpd.conf":       "/etc/httpd/conf/httpd.conf",
        "ssl_conf_path":    "/etc/httpd/conf.d/ssl.conf",
        "default_vhost":    "/etc/httpd/sites-enabled/000-default",  # Not used
        "mod_ssl_file":     "/etc/httpd/modules/mod_ssl.so",
        "mod_rpaf_path":    "/etc/httpd/conf.d/mod_rpaf.conf",
        "key_path_default": "/etc/pki/tls/private/localhost.key",
        "crt_path_default": "/etc/pki/tls/certs/localhost.crt",
        "apachectl":        "/usr/sbin/apachectl",
        "bin_path": "/usr/sbin/httpd",
        "initd_script":     "/etc/init.d/httpd",
        "group":            "apache",

        "logrotate_conf":   """/var/log/http-*.log {
         missingok
         notifempty
         sharedscripts
         delaycompress
         postrotate
             /sbin/service httpd reload > /dev/null 2>/dev/null || true
         endscript
        }
        """})

__apache__ = __node__["apache"]
__apache__.update(apache)


class ApacheError(BaseException):
    pass


class ApacheAPI(object):

    service = None
    mod_ssl = None
    current_open_ports = None

    def __init__(self):
        self.service = initdv2.lookup("apache")
        self.mod_ssl = DebianBasedModSSL() if linux.os.debian_family else RedHatBasedModSSL()
        self.current_open_ports = []
        self._query_env = bus.queryenv_service

    @rpc.service_method
    def create_vhost(self, hostname, port, template, ssl, ssl_certificate_id=None, reload=True):
        """
        Creates Name-Based Apache VirtualHost

        @param hostname: Server Name
        @param port: port to listen to
        @param template: VirtualHost body with no certificate paths
        @param ssl: True if VirtualHost uses SSL certificate
        @param ssl_certificate_id: ID of SSL certificate
        @param reload: True if immediate apache reload is required.
        @return: path to VirtualHost file
        """
        #TODO: add Listen and NameVirtualHost directives to httpd.conf or ports.conf if needed

        v_host_path = get_virtual_host_path(hostname, port)
        assert not os.path.exists(v_host_path)

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

            #Compatibility with old apache handler
            if self.mod_ssl.is_system_certificate_used():
                self.mod_ssl.set_default_certificate(ssl_certificate)

        assert int(port) == int(v_host.port)
        assert hostname == v_host.server_name

        for directory in v_host.document_root_paths:
            docroot_parent_path = os.path.dirname(directory)

            if not os.path.exists(docroot_parent_path):
                os.makedirs(docroot_parent_path, 0755)
                LOG.info("Created parent directory of document root %s for %s" % (directory, v_host))

            if not os.path.exists(directory) or not os.listdir(directory):
                shutil.copytree(os.path.join(bus.share_path, "apache/html"), directory)
                files = ", ".join(os.listdir(directory))
                LOG.debug("Copied document root files: %s" % files)

                try:
                    pwd.getpwnam("apache")
                    uname = "apache"
                except KeyError:
                    uname = "www-data"

                coreutils.chown_r(directory, uname)
                LOG.debug("Changed owner to %s: %s" % (
                    uname, ", ".join(os.listdir(directory))))
            else:
                LOG.debug("Document root %s already exists." % directory)

        try:
            clog_path = os.path.dirname(v_host.custom_log_path)
            if not os.path.exists(clog_path):
                os.makedirs(clog_path, 0755)
                LOG.info("Created CustomLog directory for VirtualHost %s:%s: %s" % (
                    hostname,
                    port,
                    clog_path,
                ))
        except NoPathError:
            LOG.info("CustomLog directive not found in %s" % v_host)

        try:
            errlog_path = os.path.dirname(v_host.error_log_path)
            if not os.path.exists(errlog_path):
                os.makedirs(errlog_path, 0755)
                LOG.info("Created ErrorLog directory for VirtualHost %s:%s: %s" % (
                    hostname,
                    port,
                    errlog_path,
                ))
        except NoPathError:
            LOG.debug("ErrorLog directive not found in %s" % v_host)

        if os.path.exists(v_host_path) and open(v_host_path).read() == v_host.body:
            LOG.info("Skipping VirtualHost %s: No changes found." % v_host)
            return v_host_path

        with BackupManager(v_host_path):
            with open(v_host_path, "w") as fp:
                fp.write(v_host.body)
        LOG.info("VirtualHost %s saved to %s" % (v_host, v_host_path))

        self._open_ports([port])

        if reload:
            try:
                self.configtest()
            except initdv2.InitdError, e:
                LOG.error("ConfigTest failed with error: '%s'." % str(e))
                BackupManager.restore(v_host_path)
                raise

            else:
                BackupManager.free(v_host_path)
                self.reload_service()

        return v_host_path

    @rpc.service_method
    def update_vhost(self,
                     signature,
                     hostname=None,
                     port=80,
                     template=None,
                     ssl=False,
                     ssl_certificate_id=None,
                     reload=True):
        """
        Changes settings of VirtualHost defined by @signature

        @param signature: tuple, (hostname,port)
        @param hostname: String, new hostname
        @param port: int, new port
        @param: ssl: bool, indicates if the updated VirtualHost is going to be ssl-based.
        @param: ssl_certificate_id: int, ID of the new certificate to fetch from Scalr
        @param: reload: bool, indicates if immediate reload is required.
        @param template: String, new template. If new template is passed,
            all other changes (e.g. hostname, port, cert) will be applied to it.
            Otherwice changes will be applied to old VirtualHost`s body.
        """

        old_hostname, old_port = signature
        old_path = get_virtual_host_path(old_hostname, old_port)

        if not os.path.exists(old_path):
            raise ApacheError("Cannot update VirtualHost %s:%s: %s not found." % (
                old_hostname,
                old_port,
                old_path
            ))

        old_body = open(old_path, "r").read()
        template = template or old_body
        v_host = VirtualHost(template)

        if hostname:
            v_host.server_name = hostname

        if port:
            v_host.port = port

        if ssl and ssl_certificate_id:
            ssl_certificate = SSLCertificate(ssl_certificate_id)
            ssl_certificate.ensure()
            v_host.use_certificate(ssl_certificate)

        new_path = get_virtual_host_path(hostname or old_hostname, port or old_port)
        with BackupManager(new_path):
            if old_path != new_path:
                os.remove(old_body)
            with open(new_path, "w") as fp:
                fp.write(v_host.body)

        if reload:
            try:
                self.configtest()
            except initdv2.InitdError, e:
                LOG.error("ConfigTest failed with error: '%s'." % str(e))
                BackupManager.restore([old_path, new_path])
                raise
            else:
                BackupManager.free(old_path)
                self.reload_service()

    @rpc.service_method
    def delete_vhosts(self, vhosts, reload=True):
        """
        Deletes VirtualHost
        @param vhosts: list, [(hostname:password),]
        @param reload: indicates if immediate service reload is needed
        @return: None
        """
        backup_list = []
        for signature in vhosts:
            v_host_path = get_virtual_host_path(*signature)
            assert os.path.exists(v_host_path)
            backup_list.add(v_host_path)

            with BackupManager(v_host_path):
                os.remove(v_host_path)
                LOG.info("Removed VirtualHost %s:%s" % signature)

        if reload:
            try:
                self.configtest()
            except initdv2.InitdError, e:
                LOG.error("ConfigTest failed with error: '%s'." % str(e))
                BackupManager.restore(backup_list)
                raise
            else:
                BackupManager.free(backup_list)
                self.reload_service()

    @rpc.service_method
    def reconfigure(self, vhosts):
        """
        Deploys multiple VirtualHosts and removes odds.
        @param vhosts: list(dict(vhost_data),)
        @return: list, paths to reconfigured VirtualHosts
        """
        applied_vhosts = []
        new_vhosts = []
        backup_list = []
        reload = False

        for vh_data in vhosts:

            host, port = vh_data["hostname"], vh_data["port"]
            v_host_path = get_virtual_host_path(host, port)

            with BackupManager(v_host_path):
                if os.path.exists(v_host_path):
                    backup_list.append(v_host_path)
                    os.remove(v_host_path)

                path = self.create_vhost(
                    vh_data["hostname"],
                    vh_data["port"],
                    vh_data["template"],
                    vh_data["ssl"],
                    vh_data["ssl_certificate_id"],
                    reload=False
                )
                applied_vhosts.append(path)

                if path in backup_list:
                    with open(path) as fp:
                        new_body = fp.read()
                        try:
                            old_body = BackupManager.backup[path].copy()
                        except (AttributeError, IndexError):
                            old_body = None
                    if old_body != new_body:
                        reload = True
                else:
                    new_vhosts.append(path)
                    reload = True

        #cleanup
        vhosts_dir = __apache__["vhosts_dir"]
        for fname in os.listdir(vhosts_dir):
            old_vhost_path = os.path.join(vhosts_dir, fname)

            if old_vhost_path not in applied_vhosts:
                with BackupManager(old_vhost_path):
                    backup_list.append(old_vhost_path)
                    os.remove(old_vhost_path)
                LOG.info("Removed old vhost file %s" % old_vhost_path)
                reload = True

        if reload:
            try:
                self.configtest()
            except initdv2.InitdError, e:
                LOG.error("ConfigTest failed with error: '%s'." % str(e))
                BackupManager.restore(new_vhosts + backup_list)
                raise
            else:
                BackupManager.free(new_vhosts + backup_list)
                self.reload_service()
        else:
            LOG.info("No changes were made in apache configuration.")

        return applied_vhosts

    @rpc.service_method
    def get_webserver_statistics(self):
        """
        @return: dict, parsed mod_status data

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
            f = urllib2.urlopen("http://127.0.0.1/server-status?auto")
            data = f.read()
        except urllib2.HTTPError, e:
            if "404" in str(e):
                return {"errmsg": "mod_status is not enabled"}
            return {"errmsg": str(e)}
        except urllib2.URLError, e:
            if "111" in str(e):
                return {"errmsg": "Connection refused"}
            return {"errmsg": str(e)}
        if data:
            for line in data.split("\n"):
                pairs = line.split(":")
                if len(pairs) > 1:
                    key, value = pairs
                    d[key.strip()] = value.strip()
        return d

    @rpc.service_method
    def list_served_virtual_hosts(self):
        """
        Returns all VirtualHosts deployed by Scalr
        and available on web server

        @return: list, paths to available VirtualHosts
        """
        text = system2((__apache__["apachectl"], "-S"))[0]
        directory = __apache__["vhosts_dir"]
        ext = __apache__["vhost_extension"]
        result = []

        for file_name in os.listdir(directory):
            path = os.path.join(directory, file_name)
            if path.endswith(ext) and path in text:
                result.append(file_name)

        return result

    @rpc.service_method
    def set_default_ssl_certificate(self, id):
        """
        If the certificate with given ID already exists on disk
        this method adds it to the default SSL virtual host.
        Otherwice default system certificate will be used.
        """
        cert = SSLCertificate(id)
        self.mod_ssl.set_default_certificate(cert)

    @rpc.service_method
    def start_service(self):
        self.service.start()

    @rpc.service_method
    def stop_service(self, reason=None):
        self.service.stop(reason)

    @rpc.service_method
    def restart_service(self, reason=None):
        self.service.restart(reason)

    @rpc.service_method
    def reload_service(self, reason=None):
        self.service.reload(reason)

    @rpc.service_method
    def configtest(self):
        self.service.configtest()

    def init_service(self):
        """
        Configures apache service
        """
        self.stop_service("Configuring Apache Web Server")

        self._open_ports([80, 443])

        self.enable_virtual_hosts_directory()

        self.fix_default_virtual_host()

        self.update_log_rotate_config()

        self.mod_ssl.ensure()

        if linux.os.debian_family:
            mod_rpaf_path = __apache__["mod_rpaf_path"]

            with open(mod_rpaf_path, "r") as fp:
                body = fp.read()

            mod_rpaf = ModRPAF(body)
            mod_rpaf.fix_module()

            with open(mod_rpaf_path, "w") as fp:
                fp.write(mod_rpaf.body)

        ModRPAF.ensure_permissions()

        self.start_service()

    def enable_virtual_hosts_directory(self):
        if not os.path.exists(__apache__["vhosts_dir"]):
            os.makedirs(__apache__["vhosts_dir"])
            LOG.info("Created new directory for VirtualHosts: %s" % __apache__["vhosts_dir"])

        with ApacheConfigManager(__apache__["httpd.conf"]) as apache_config:
            inc_mask = __apache__["vhosts_dir"] + "/*" + __apache__["vhost_extension"]

            if not inc_mask in apache_config.get_list("Include"):
                apache_config.add("Include", inc_mask)
                LOG.info("VirtualHosts directory included in %s" % __apache__["httpd.conf"])

    def fix_default_virtual_host(self):
        if linux.os.debian_family:
            if os.path.exists(__apache__["default_vhost"]):

                with ApacheConfigManager(__apache__["default_vhost"]) as default_vhost:
                    default_vhost.set("NameVirtualHost", "*:80", force=True)

                with open(__apache__["default_vhost"], "r") as fp:
                    dv = fp.read()

                vhost_regexp = re.compile("<VirtualHost\s+\*>")
                dv = vhost_regexp.sub("<VirtualHost *:80>", dv)

                with open(__apache__["default_vhost"], "w") as fp:
                    fp.write(dv)

                LOG.info("Replaced NameVirtualhost and Virtualhost port values in the default virtual host file.")
            else:
                LOG.warning("Cannot find default vhost config file %s." % __apache__["default_vhost"])

        else:
            with ApacheConfigManager(__apache__["httpd.conf"]) as apache_config:
                if not apache_config.get_list("NameVirtualHost"):
                    apache_config.set("NameVirtualHost", "*:80", force=True)

    def update_log_rotate_config(self):
        if not os.path.exists(__apache__["logrotate_conf_path"]):
            with open(__apache__["logrotate_conf_path"], "w") as fp:
                fp.write(__apache__["logrotate_conf"])
            LOG.info("LogRorate config updated.")

    def reload_virtual_hosts(self):
        """
        Reloads all VirtualHosts assigned to the server
        @return: list(virtual_host_path,)
        """
        vh_data = self._fetch_virtual_hosts()
        return self.reconfigure(vh_data)

    def rename_old_virtual_hosts(self):
        vhosts_dir = __apache__["vhosts_dir"]
        for fname, new_fname in get_updated_file_names(os.listdir(vhosts_dir)):
            os.rename(os.path.join(vhosts_dir, fname), os.path.join(vhosts_dir, new_fname))

    def _fetch_virtual_hosts(self):
        """
        Combines list of virtual hosts in unified format
        regardless of Scalr version.
        @return: list(dict(vhost_data))
        """
        result = []
        scalr_version = bus.scalr_version or (4, 4, 0)

        if scalr_version < (4, 4):
            raw_data = self._query_env.list_virtual_hosts()

            for virtual_host_data in raw_data:
                data = dict()
                data["hostname"] = virtual_host_data.hostname
                data["template"] = virtual_host_data.raw
                ssl = bool(int(virtual_host_data.https))
                data["port"] = 443 if ssl else 80
                data["ssl"] = ssl
                if ssl:
                    data["ssl_certificate_id"] = virtual_host_data["ssl_certificate_id"]
                else:
                    data["ssl_certificate_id"] = None
                result.append(data)

        else:
            raw_data = self._query_env.list_farm_role_params(__node__["farm_role_id"])
            params = raw_data.get("params", {})
            LOG.debug("QueryEnv returned list of farmrole params: %s" % params)
            app_data = params.get("app", {})
            virtual_hosts_section = app_data.get("virtual_hosts", [])
            if virtual_hosts_section:
                for virtual_host_data in virtual_hosts_section:
                    virtual_host_data["ssl"] = bool(int(virtual_host_data["ssl"]))
                    if not virtual_host_data["ssl"]:
                        virtual_host_data["ssl_certificate_id"] = None  # Handling "0"
                    result.append(virtual_host_data)

        return result

    def _open_ports(self, ports):
        if iptables.enabled():
            LOG.info("Allowing ports %s in IPtables" % str(ports))
            rules = []
            for port in ports:
                if port not in self.current_open_ports:
                    rules.append({"jump": "ACCEPT", "protocol": "tcp", "match": "tcp", "dport": str(port)})
                    self.current_open_ports.append(port)
            if rules:
                iptables.FIREWALL.ensure(rules)
        else:
            LOG.warning("Cannot open ports %s: IPtables disabled" % str(ports))


class BasicApacheConfiguration(object):

    body = None

    def __init__(self, body):
        self.body = str(body)  # [SCALARIZR-1226]

    @property
    def _cnf(self):
        cnf = Configuration("apache")
        cnf.reads(self.body)
        return cnf

    def _update_body(self, config_obj):
        self.body = config_obj.dumps()


class VirtualHost(BasicApacheConfiguration):

    def __repr__(self):
        return "%s:%s" % (self.server_name, self.port)

    def __cmp__(self, other):
        return self.body == other.body

    @property
    def error_log_path(self):
        raw_value = self._cnf.get(".//ErrorLog")
        return raw_value.split(" ")[0]

    @property
    def custom_log_path(self):
        raw_value = self._cnf.get(".//CustomLog")
        return raw_value.split(" ")[0]

    @property
    def ssl_cert_path(self):
        return self._cnf.get(".//SSLCertificateFile")

    @property
    def ssl_key_path(self):
        return self._cnf.get(".//SSLCertificateKeyFile")

    @property
    def ssl_chain_path(self):
        return self._cnf.get(".//SSLCertificateChainFile")

    @property
    def document_root_paths(self):
        doc_roots = []
        for item in self._cnf.items(".//VirtualHost"):
            if "DocumentRoot" == item[0]:
                doc_root = item[1][:-1] if item[1][-1] == "/" else item[1]
                doc_roots.append(doc_root)
        return doc_roots

    def use_certificate(self, cert_path, key_path, chain_path=None):
        mem_config = self._cnf

        assert mem_config.get(".//SSLCertificateFile")

        mem_config.set(".//SSLCertificateFile", cert_path)
        mem_config.set(".//SSLCertificateKeyFile", key_path)

        if chain_path:
            try:
                mem_config.set(".//SSLCertificateChainFile", chain_path, force=False)
            except NoPathError:
                parent = mem_config.etree.find(".//SSLCertificateFile/..")
                before_el = mem_config.etree.find(".//SSLCertificateFile")
                ch = mem_config._provider.create_element(
                    mem_config.etree,
                    ".//SSLCertificateChainFile",
                    chain_path)
                ch.text = chain_path
                parent.insert(list(parent).index(before_el), ch)
        else:
            mem_config.comment(".//SSLCertificateChainFile")

        self._update_body(mem_config)

    def _get_port(self):
        raw_host = self._cnf.get(".//VirtualHost").split(":")
        if len(raw_host) > 1 and raw_host[1].isdigit():
            return int(raw_host[1])
        elif self.ssl_cert_path:
            return 443
        else:
            return 80

    def _set_port(self, port):
        mem_config = self._cnf
        old_value = mem_config.get(".//VirtualHost")
        host = old_value.split(":")[0]
        new_value = "%s:%s" % (host, port)
        mem_config.set(".//VirtualHost", dict(value=new_value))
        self._update_body(mem_config)

    def _get_server_name(self):
        try:
            server_name = self._cnf.get(".//ServerName")
        except NoPathError:
            server_name = ''
        return server_name

    def _set_server_name(self, new_name):
        mem_config = self._cnf
        mem_config.set(".//ServerName", new_name)
        self._update_body(mem_config)

    server_name = property(_get_server_name, _set_server_name)

    port = property(_get_port, _set_port)


class ModRPAF(BasicApacheConfiguration):

    def list_proxy_ips(self):
        raw_value = self._cnf.get(".//RPAFproxy_ips")
        ips = set(re.split(r"\s+", raw_value))
        return ips

    def add(self, ips):
        proxy_ips = self.list_proxy_ips()
        proxy_ips |= set(ips)

        mem_config = self._cnf
        mem_config.set(".//RPAFproxy_ips", " ".join(proxy_ips))
        self._update_body(mem_config)

    def remove(self, ips):
        proxy_ips = self.list_proxy_ips()
        proxy_ips -= set(ips)

        mem_config = self._cnf
        mem_config.set(".//RPAFproxy_ips", " ".join(proxy_ips))
        self._update_body(mem_config)

    def update(self, ips):
        proxy_ips = set(ips)

        mem_config = self._cnf
        mem_config.set(".//RPAFproxy_ips", " ".join(proxy_ips))
        self._update_body(mem_config)

    def fix_module(self):
        """
        fixing bug in rpaf 0.6-2
        """
        pm = dynimp.package_mgr()
        if "0.6-2" == pm.installed("libapache2-mod-rpaf"):
            mem_config = self._cnf
            try:
                mem_config.set('./IfModule[@value="mod_rpaf.c"]', {"value": "mod_rpaf-2.0.c"})
            except NoPathError:
                pass
            else:
                self._update_body(mem_config)
                LOG.info("Patched IfModule value in rpaf.conf")

    @staticmethod
    def ensure_permissions():
        st = os.stat(__apache__["httpd.conf"])
        os.chown(__apache__["mod_rpaf_path"], st.st_uid, st.st_gid)


class ApacheConfigManager(object):

    _cnf = None
    path = None

    def __init__(self, path):
        self._cnf = Configuration("apache")
        self.path = path

    def __enter__(self):
        self._cnf.read(self.path)
        return self._cnf

    def __exit__(self, type, value, traceback):
        self._cnf.write(self.path)


class BackupManager(object):

    path = None
    before = None
    backup = {}

    def __init__(self, path):
        self.path = path
        self.before = None

    def _get_content(self):
        if os.path.exists(self.path):
            with open(self.path, "r") as fp:
                text = fp.read()
            st = os.stat(self.path)
            return text, st.st_uid, st.st_gid, st.st_mode

    def __enter__(self):
        self.before = self._get_content()

    def __exit__(self, type, value, traceback):
        after = self._get_content()
        if self.before != after:
            BackupManager.backup[self.path] = self.before

    @classmethod
    def restore(cls, paths):
        try:
            paths = iter(paths)
        except TypeError:
            paths = [paths, ]

        for path in paths:
            if path in cls.backup:
                data = cls.backup[path]
                if not data and os.path.exists(path):
                    os.remove(path)
                    LOG.debug("%s removed to restore its previous state")
                else:
                    text, st_uid, st_gid, st_mode = data
                    with open(path, "w") as fp:
                        fp.write(text)
                    os.chown(path, st_uid, st_gid)
                    os.chmod(path, st_mode)
                    LOG.debug("%s restored to its previous state")
            else:
                LOG.debug("Cannot restore %s: file wasn`t changed")
            cls.free(path)

    @classmethod
    def free(cls, paths):
        try:
            paths = iter(paths)
        except TypeError:
            paths = [paths, ]

        for path in paths:
            if path in cls.backup:
                del cls.backup[path]


class SSLCertificate(object):

    id = None

    def __init__(self, ssl_certificate_id=None):
        self.id = ssl_certificate_id

    def update(self, cert, key, authority=None):
        """
        Dumps certificate on disk.
        @param cert: String, certificate pem
        @param key: String, certificate key
        @param authority: String, CA Cert
        """
        st = os.stat(__apache__["httpd.conf"])

        with open(self.cert_path, "w") as fp:
            fp.write(cert)
        os.chown(self.cert_path, st.st_uid, st.st_gid)

        with open(self.key_path, "w") as fp:
            fp.write(key)
        os.chown(self.key_path, st.st_uid, st.st_gid)

        if authority:
            with open(self.chain_path, "w") as fp:
                fp.write(authority)
            os.chown(self.chain_path, st.st_uid, st.st_gid)

    def ensure(self):
        """
        Fetches SSL Certificate from Scalr and dumps data on disk.
        """
        LOG.info("Retrieving ssl cert %s from Scalr." % self.id or "'default'")
        query_env = bus.queryenv_service
        cert_data = query_env.get_ssl_certificate(self.id)
        authority = cert_data[2] if len(cert_data) > 2 else None
        self.update(cert_data[0], cert_data[1], authority)

    def delete(self):
        """
        Removes SSL Certificate files from disk.
        @return:
        """
        for path in (self.cert_path, self.key_path, self.chain_path):
            if os.path.exists(path):
                os.remove(path)

    @property
    def cert_path(self):
        id = "_" + str(self.id) if self.id else ''
        return os.path.join(__apache__["keys_dir"], "https%s.crt" % id)

    @property
    def key_path(self):
        id = "_" + str(self.id) if self.id else ''
        return os.path.join(__apache__["keys_dir"], "https%s.key" % id)

    @property
    def chain_path(self):
        id = "_" + str(self.id) if self.id else ''
        return os.path.join(__apache__["keys_dir"], "https%s-ca.crt" % id)

    def exists(self):
        return os.path.exists(self.cert_path) and os.path.exists(self.key_path)


class ModSSL(object):

    def set_default_certificate(self, cert):
        """
        If certificate files exist on disk
        this method adds this certificate to the default SSL virtual host.
        Otherwice default system certificate will be used.
        """
        ssl_conf_path = __apache__["ssl_conf_path"]

        if os.path.exists(cert.cert_path):
            cert_path = cert.cert_path
            cert_str = cert.id
        else:
            cert_path = __apache__["crt_path_default"]
            cert_str = "System default"

        if os.path.exists(cert.key_path):
            key_path = cert.key_path
        else:
            key_path = __apache__["key_path_default"]

        if os.path.exists(cert.chain_path):
            ca_crt_path = cert.chain_path
        else:
            ca_crt_path = None

        with open(ssl_conf_path, "r") as fp:
            body = fp.read()

        v_host = VirtualHost(body)
        v_host.use_certificate(cert_path, key_path, ca_crt_path)

        with open(ssl_conf_path, "w") as fp:
            fp.write(v_host.body)

        LOG.info("Certificate %s is set to %s" % (cert_str, ssl_conf_path))

    def is_system_certificate_used(self):
        with open(__apache__["ssl_conf_path"], "r") as fp:
            body = fp.read()

        v_host = VirtualHost(body)

        has_certificate = v_host.ssl_cert_path and v_host.ssl_key_path
        system_crt = v_host.ssl_cert_path == __apache__["crt_path_default"]
        system_pkey = v_host.ssl_key_path == __apache__["key_path_default"]

        return has_certificate and system_crt and system_pkey

    def ensure(self):
        raise NotImplementedError


class DebianBasedModSSL(ModSSL):

    def ensure(self, ssl_port=443):
        """
        Enables mod_ssl and default SSL-based virtual host.
        Sets NameVirtualHost and Listen values in this virtual host.
        @param ssl_port: int, port number
        the default SSL-based virtual host will listen to.
        """
        self._enable_mod_ssl()
        self._enable_default_ssl_virtual_host()
        self._set_name_virtual_host(ssl_port)

    def _enable_mod_ssl(self):
        if not os.path.exists(__apache__["ssl_load_deb"]):
            system2((__apache__["a2enmod_path"], "ssl"))
            LOG.info("mod_ssl enabled.")

    def _enable_default_ssl_virtual_host(self):
        if not os.path.exists(__apache__["ssl_load_deb"]):
            system2((__apache__["a2ensite_path"], "default-ssl"))
            LOG.info("Default SSL virtualhost enabled.")

    def _set_name_virtual_host(self, ssl_port):
        if os.path.exists(__apache__["ports_conf_deb"]):
            with ApacheConfigManager(__apache__["ports_conf_deb"]) as conf:
                i = 0
                for section in conf.get_dict("IfModule"):
                    i += 1
                    if section["value"] in ("mod_ssl.c", "mod_gnutls.c"):
                        conf.set("IfModule[%d]/Listen" % i, str(ssl_port), True)
                        conf.set("IfModule[%d]/NameVirtualHost" % i, "*:%s" % ssl_port, True)
            LOG.info("NameVirtualHost *:%s added to %s" % (ssl_port, __apache__["ports_conf_deb"]))


class RedHatBasedModSSL(ModSSL):

    def ensure(self, ssl_port=443):
        """
        Installs and enables mod_ssl. Then enables default SSL-based virtual host
        by adding module path to the main apache2 config.
        Sets NameVirtualHost and Listen values in this virtual host.
        @param ssl_port: int, port number
        the default SSL-based virtual host will listen to.
        """
        self._install_mod_ssl()
        self._ensure_ssl_conf()
        self._enable_mod_ssl()
        self._set_name_virtual_host(ssl_port)

    def _install_mod_ssl(self):
        if not os.path.exists(__apache__["mod_ssl_file"]):
            LOG.info("%s does not exist. Trying to install mod_ssl." % __apache__["mod_ssl_file"])
            pm = pkgmgr.PackageMgr()
            pm.install("mod_ssl")

    def _ensure_ssl_conf(self):
        ssl_conf_path = __apache__["ssl_conf_path"]
        if not os.path.exists(ssl_conf_path):
            LOG.warning("SSL config %s doesn`t exist", ssl_conf_path)
            open(ssl_conf_path, "w").close()
            st = os.stat(__apache__["httpd.conf"])
            os.chown(ssl_conf_path, st.st_uid, st.st_gid)

    def _enable_mod_ssl(self):
        with ApacheConfigManager(__apache__["httpd.conf"]) as main_config:
            loaded_in_main = [module for module in main_config.get_list("LoadModule") if "mod_ssl.so" in module]
            if not loaded_in_main:
                if os.path.exists(__apache__["ssl_conf_path"]):
                    loaded_in_ssl = [module for module in main_config.get_list("LoadModule") if "mod_ssl.so" in module]
                    if not loaded_in_ssl:
                        main_config.add("LoadModule", "ssl_module modules/mod_ssl.so")
                        LOG.info("Default SSL virtualhost enabled.")

    def _set_name_virtual_host(self, ssl_port=443):
        ssl_conf_path = __apache__["ssl_conf_path"]
        with ApacheConfigManager(ssl_conf_path) as ssl_conf:
            if ssl_conf.empty:
                LOG.error("SSL config file %s is empty. Filling in with minimal configuration.", ssl_conf_path)
                ssl_conf.add("Listen", str(ssl_port))
                ssl_conf.add("NameVirtualHost", "*:%s" % ssl_port)

            else:
                if not ssl_conf.get_list("NameVirtualHost"):
                    LOG.info("NameVirtualHost directive not found in %s", ssl_conf_path)
                    if not ssl_conf.get_list("Listen"):
                        LOG.info("Listen directive not found in %s. ", ssl_conf_path)
                        LOG.info("Patching %s with Listen & NameVirtualHost directives.",     ssl_conf_path)
                        ssl_conf.add("Listen", str(ssl_port))
                        ssl_conf.add("NameVirtualHost", "*:%s" % ssl_port)
                    else:
                        ssl_conf.add("NameVirtualHost", "*:%s" % ssl_port, before_path="Listen")
                        LOG.info("NameVirtualHost directive inserted after Listen directive.")


class ApacheInitScript(initdv2.ParametrizedInitScript):

    _apachectl = None

    def __init__(self):

        pid_file = self._get_pid_file_path()
        initdv2.ParametrizedInitScript.__init__(
            self,
            "apache",
            __apache__["initd_script"],
            pid_file=pid_file,
        )

    def _get_pid_file_path(self):
        #TODO: fix assertion when platform becomes an object (commit 58921b6303a96c8975e417fd37d70ddc7be9b0b5)

        if "gce" == __node__["platform"]:
            gce_pid_dir = "/var/run/httpd"
            if not os.path.exists(gce_pid_dir):
                os.makedirs(gce_pid_dir)

        pid_file = None
        if linux.os.redhat_family:
            pid_file = "/var/run/httpd/httpd.pid" if linux.os["release"].version[0] == 6 else "/var/run/httpd.pid"
        elif linux.os.debian_family:
            if os.path.exists("/etc/apache2/envvars"):
                pid_file = system2("/bin/sh", stdin=". /etc/apache2/envvars; echo -n $APACHE_PID_FILE")[0]
            if not pid_file:
                pid_file = "/var/run/apache2.pid"
        return pid_file

    def reload(self, reason=None):
        LOG.info("Reloading apache: %s" % str(reason) if reason else '')
        if self.running:
            out, err, retcode = system2(__apache__["apachectl"] + " graceful", shell=True)
            if retcode > 0:
                raise initdv2.InitdError("Cannot reload apache: %s" % err)
        else:
            raise InitdError("Service '%s' is not running" % self.name, InitdError.NOT_RUNNING)

    def status(self):
        status = initdv2.ParametrizedInitScript.status(self)
        # If "running" and socks were passed
        if not status and self.socks:
            ip, port = self.socks[0].conn_address
            try:
                expected = "server: apache"
                telnet = Telnet(ip, port)
                telnet.write("HEAD / HTTP/1.0\n\n")
                if expected in telnet.read_until(expected, 5).lower():
                    return initdv2.Status.RUNNING
            except EOFError:
                pass
            return initdv2.Status.NOT_RUNNING
        return status

    def configtest(self, path=None):
        args = __apache__["apachectl"] + " configtest"
        if path:
            args += "-f %s" % path
        out = system2(args, shell=True)[1]
        if "error" in out.lower():
            raise initdv2.InitdError("Configuration isn`t valid: %s" % out)

    def start(self):
        if not self._main_process_started() and not self.running:
            initdv2.ParametrizedInitScript.start(self)
            if self.pid_file:
                try:
                    wait_until(lambda: os.path.exists(self.pid_file) or self._main_process_started(), sleep=0.2, timeout=30)
                except (Exception, BaseException), e:
                    raise initdv2.InitdError("Cannot start Apache (%s)" % str(e))
            time.sleep(0.5)

    def stop(self, reason=None):
        if self._main_process_started() and self.running:
            LOG.info("Stopping apache: %s" % str(reason) if reason else '')
            initdv2.ParametrizedInitScript.stop(self)

    def restart(self, reason=None):
        LOG.info("Restarting apache: %s" % str(reason) if reason else '')

        if not self._main_process_started():
            self.start()
        else:
            initdv2.ParametrizedInitScript.restart(self)
        if self.pid_file:
            try:
                wait_until(
                    lambda: os.path.exists(self.pid_file),
                    sleep=0.2,
                    timeout=5,
                    error_text="Apache pid file %s doesn`t exists" % self.pid_file)
            except:
                raise initdv2.InitdError("Cannot start Apache: pid file %s hasn`t been created" % self.pid_file)
        time.sleep(0.5)

    @staticmethod
    def _main_process_started():
        res = False
        try:
            out = system2(("ps", "-G", __apache__["group"], "-o", "command", "--no-headers"), raise_exc=False)[0]
            res = __apache__["bin_path"] in out
        except (Exception, BaseException):
            pass
        return res


initdv2.explore("apache", ApacheInitScript)


def get_virtual_host_path(hostname, port):
    ext = __apache__["vhost_extension"]
    end = "%s-%s%s" % (hostname, port, ext)
    return os.path.join(__apache__["vhosts_dir"], end)


def get_updated_file_names(virtual_host_file_names):
    ext = __apache__["vhost_extension"]

    plaintext_pattern = re.compile("(.+)\.vhost.conf")
    ssl_pattern = re.compile("(.+)-ssl%s" % ext)
    newstyle_pattern = re.compile("(\d+)%s" % ext)

    pairs = {}
    for fname in virtual_host_file_names:
        new_fname = None

        if fname.endswith("-ssl%s" % ext):
            res = ssl_pattern.search(fname)
            if res:
                new_fname = res.group(1) + "-443" + ext

        elif fname.endswith(ext):
            res = newstyle_pattern.search(fname)
            if res:
                continue
            else:
                res = plaintext_pattern.search(fname)
                new_fname = res.group(1) + "-80" + ext
        else:
            continue
        pairs[fname] = new_fname
    return pairs
