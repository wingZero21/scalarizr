import os
import re
import shutil
import logging


from scalarizr import handlers, linux
from scalarizr.bus import bus
from scalarizr.linux import pkgmgr, execute
from scalarizr.messaging import Messages
from scalarizr.util import initdv2
from scalarizr.node import __node__


LOG = logging.getLogger(__name__)

def get_handlers():
    return [TomcatHandler()]

class KeytoolExec(execute.BaseExec):
    executable = '/usr/bin/keytool'

    # keytool uses long args with a short prefix
    def _default_handler(self, key, value, cmd_args):
        cmd_args.append('-{0}'.format(key))
        if value is not True:
            cmd_args.append(str(value))

    # last param is a keytool command that should be first
    def _after_all_handlers(self, cmd_args):
        return ['-{0}'.format(cmd_args[-1])] + cmd_args[0:-1]


class TomcatHandler(handlers.Handler, handlers.FarmSecurityMixin):

    def __init__(self):
        handlers.Handler.__init__(self)
        handlers.FarmSecurityMixin.__init__(self, [8080, 8443])
        bus.on(
            init=self.on_init, 
            start=self.on_start
        )
        self.tomcat_version = None
        self.config_dir = None
        self.init_script_path = None

        if linux.os.debian_family:
            if (linux.os['name'] == 'Ubuntu' and linux.os['version'] >= (12, 4)) or \
                (linux.os['name'] == 'Debian' and linux.os['version']  >= (7, 0)):
                self.tomcat_version = 7
            else:
                self.tomcat_version = 6
        else:
            self.tomcat_version = 6
        self.config_dir = '/etc/tomcat{0}'.format(self.tomcat_version)
        self.init_script_path = '/etc/init.d/tomcat{0}'.format(self.tomcat_version)  

        self.service = initdv2.ParametrizedInitScript('tomcat', self.init_script_path)

    def on_init(self):
        bus.on(
            host_init_response=self.on_host_init_response,
            before_host_up=self.on_before_host_up
        )

    def on_start(self):
        if __node__['state'] == 'running':
            self.service.start()

    def accept(self, message, queue, behaviour=None, **kwds):
        return message.name in (
                Messages.HOST_INIT, 
                Messages.HOST_DOWN) and 'tomcat' in behaviour

    def on_host_init_response(self, hir_message):
        if not os.path.exists(self.service.initd_script):
            tomcat = 'tomcat{0}'.format(self.tomcat_version)
            pkgs = [tomcat]
            if linux.os.debian_family:
                pkgs.append('{0}-admin'.format(tomcat))
            elif linux.os.redhat_family or linux.os.oracle_family:
                pkgs.append('{0}-admin-webapps'.format(tomcat))
            for pkg in pkgs:
                pkgmgr.installed(pkg)
        pkgmgr.installed('augeas-tools' if linux.os.debian_family else 'augeas')

    def on_before_host_up(self, message):
        load_lens = [
            'set /augeas/load/Xml/incl[last()+1] "{0}/*.xml"'.format(self.config_dir),
            'load',
            'defvar service /files{0}/server.xml/Server/Service'.format(self.config_dir)                       
        ]

        # Fix XML prolog in server.xml
        fp = open(self.config_dir + '/server.xml')
        prolog = fp.readline()
        fp.close()
        if prolog.startswith("<?xml version='1.0'"):
            LOG.info('Making xml prolog in server.xml compatible with augtool')
            shutil.copy(self.config_dir + '/server.xml', self.config_dir + '/server.xml.0')
            with open(self.config_dir + '/server.xml.0') as fpr:
                with open(self.config_dir + '/server.xml', 'w') as fpw:
                    fpr.readline()  # skip xml prolog
                    fpw = open(self.config_dir + '/server.xml', 'w')
                    fpw.write('<?xml version="1.0" encoding="utf-8"?>\n')
                    for line in fpr:
                        fpw.write(line)
            os.remove(self.config_dir + '/server.xml.0')


        # Enable SSL
        augscript = '\n'.join(load_lens + [
            'print $service/Connector/*/port'
        ])
        LOG.debug('augscript: %s', augscript)

        out = linux.system(('augtool',), stdin=augscript)[1]
        if not '8443' in out:
            self.service.stop()

            keystore_path = self.config_dir + '/keystore'
            if not os.path.exists(keystore_path):
                LOG.info('Initializing keystore in %s', keystore_path)
                keytool = KeytoolExec(wait=True)
                keytool.start('genkey', 
                    alias='tomcat', 
                    keystore=keystore_path, 
                    storepass='changeit', 
                    keypass='changeit', 
                    dname='CN=John Smith')

            # Detect keystore type
            keytool = KeytoolExec(wait=True)
            out = keytool.start('list', 
                keystore=keystore_path, 
                storepass='changeit')['stdout']
            keystore_type = 'jks'
            for line in out.splitlines():
                m = re.search(r'^Key store type:: (.+)$', line)
                if m:
                    keystore_type = m.group(1)
                    break
            LOG.info('Keystore type: %s', keystore_type)

            LOG.info('Enabling HTTPS on 8443')
            augscript = '\n'.join(load_lens + [
                'set $service/Connector[last()+1]/#attribute/port 8443',
                'defvar attrs $service/Connector[last()]/#attribute',
                'set $attrs/protocol org.apache.coyote.http11.Http11NioProtocol',
                'set $attrs/SSLEnabled true',
                'set $attrs/maxThreads 150',
                'set $attrs/scheme https',
                'set $attrs/keystoreFile {0}'.format(keystore_path),
                'set $attrs/keystoreType {0}'.format(keystore_type),
                'set $attrs/secure true',
                'set $attrs/clientAuth false',
                'set $attrs/sslProtocol TLS',
                'save'
            ])
            LOG.debug('augscript: %s', augscript)
            linux.system(('augtool', ), stdin=augscript)

        # TODO: Import PEM cert/pk into JKS
        # openssl pkcs12 -export -in cert.pem -inkey key.pem > server.p12
        # keytool -importkeystore -srckeystore server.p12 -destkeystore server.jks -srcstoretype pkcs12

        self.service.start()

