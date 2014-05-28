import os
import socket
import glob
import logging

from scalarizr import rpc
from scalarizr.node import __node__
from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import Singleton
from scalarizr import exceptions
from scalarizr.util import initdv2
from scalarizr.util import firstmatched
from scalarizr.api import BehaviorAPI

LOG = logging.getLogger(__name__)


__tomcat__ = __node__['tomcat']
__tomcat__.update({
    'catalina_home_dir': None,
    'java_home': firstmatched(lambda path: os.access(path, os.X_OK), [
            linux.system('echo $JAVA_HOME', shell=True)[0].strip(),
            '/usr/java/default'], 
            '/usr'),
    'config_dir': None,
    'install_type': None
})


def augload():
    path = __tomcat__['config_dir']
    return [
        'set /augeas/load/Xml/incl[last()+1] "{0}/*.xml"'.format(path),
        'load',
        'defvar service /files{0}/server.xml/Server/Service'.format(path)                       
    ]


def augtool(script_lines):
    augscript = augload() + script_lines
    augscript = '\n'.join(augscript)
    LOG.debug('augscript: %s', augscript)
    return linux.system(('augtool', ), stdin=augscript)[0].strip()


class CatalinaInitScript(initdv2.ParametrizedInitScript):
    def __init__(self):
        initdv2.ParametrizedInitScript.__init__(self, 'tomcat', 
                __tomcat__['catalina_home_dir'] + '/bin/catalina.sh')
        self.server_port = None

    def status(self):
        if not self.server_port:
            out = augtool(['print /files{0}/server.xml/Server/#attribute/port'.format(__tomcat__['config_dir'])])
            self.server_port = out.split(' = ')[-1]
            self.server_port = int(self.server_port.strip('"'))

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect(('', self.server_port))
            return initdv2.Status.RUNNING
        except socket.error:
            return initdv2.Status.NOT_RUNNING
        finally:
            try:
                sock.close()
            except:
                pass


class TomcatAPI(BehaviorAPI):
    """
    Basic API for managing Tomcat service.

    Namespace::

        tomcat
    """
    __metaclass__ = Singleton

    behavior = 'tomcat'

    def _find_service(self):
        # try to read CATALINA_HOME from environment
        __tomcat__['catalina_home_dir'] = linux.system('echo $CATALINA_HOME', shell=True)[0].strip()
        if not __tomcat__['catalina_home_dir']:
            # try to locate CATALINA_HOME in /opt/apache-tomcat*
            try:
                __tomcat__['catalina_home_dir'] = glob.glob('/opt/apache-tomcat*')[0]
            except IndexError:
                pass

        if __tomcat__['catalina_home_dir']:
            __tomcat__['install_type'] = 'binary'
            __tomcat__['config_dir'] = '{0}/conf'.format(__tomcat__['catalina_home_dir'])
            init_script_path = '/etc/init.d/tomcat'
            if os.path.exists(init_script_path):
                return initdv2.ParametrizedInitScript('tomcat', init_script_path)
            else:
                return CatalinaInitScript()
        else:
            __tomcat__['install_type'] = 'package'
            if linux.os.debian_family:
                if (linux.os['name'] == 'Ubuntu' and linux.os['version'] >= (12, 4)) or \
                    (linux.os['name'] == 'Debian' and linux.os['version'] >= (7, 0)):
                    tomcat_version = 7
                else:
                    tomcat_version = 6
            else:
                tomcat_version = 6
            __tomcat__['config_dir'] = '/etc/tomcat{0}'.format(tomcat_version)
            init_script_path = '/etc/init.d/tomcat{0}'.format(tomcat_version)  
            return initdv2.ParametrizedInitScript('tomcat', init_script_path)

    def __init__(self):
        self.service = self._find_service()

    @rpc.command_method
    def start_service(self):
        """
        Starts Tomcat service.

        Example::

            api.tomcat.start_service()
        """
        self.service.start()

    @rpc.command_method
    def stop_service(self):
        """
        Stops Tomcat service.

        Example::

            api.tomcat.stop_service()
        """
        self.service.stop()

    @rpc.command_method
    def reload_service(self):
        """
        Reloads Tomcat configuration.

        Example::

            api.tomcat.reload_service()
        """
        self.service.reload()

    @rpc.command_method
    def restart_service(self):
        """
        Restarts Tomcat service.

        Example::

            api.tomcat.restart_service()
        """
        self.service.restart()

    @rpc.command_method
    def get_service_status(self):
        """
        Checks Tomcat service status.

        RUNNING = 0
        DEAD_PID_FILE_EXISTS = 1
        DEAD_VAR_LOCK_EXISTS = 2
        NOT_RUNNING = 3
        UNKNOWN = 4

        :return: Status num.
        :rtype: int


        Example::

            >>> api.tomcat.get_service_status()
            0
        """
        return self.service.status()

    @classmethod
    def do_check_software(cls, installed_packages=None):
        os_name = linux.os['name'].lower()
        os_vers = linux.os['version']
        if os_name == 'ubuntu':
            if os_vers >= '12':
                pkgmgr.check_dependency(['tomcat7', 'tomcat7-admin'], installed_packages)
            elif os_vers >= '10':
                pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin'], installed_packages)
        elif os_name == 'debian':
            if os_vers >= '7':
                pkgmgr.check_dependency(['tomcat7', 'tomcat7-admin'], installed_packages)
            elif os_vers >= '6':
                pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin'], installed_packages)
        elif linux.os.redhat_family or linux.os.oracle_family:
            pkgmgr.check_dependency(['tomcat6', 'tomcat6-admin-webapps'], installed_packages)
        else:
            raise exceptions.UnsupportedBehavior(cls.behavior, (
                "Unsupported operating system '{os}'").format(os=linux.os['name'])
            )

    @classmethod
    def do_handle_check_software_error(cls, e):
        if isinstance(e, pkgmgr.VersionMismatchError):
            pkg, ver, req_ver = e.args[0], e.args[1], e.args[2]
            msg = (
                '{pkg}-{ver} is not supported on {os}. Supported:\n'
                '\tUbuntu 10.04, CentOS, RedHat, Oracle, Amazon: ==6\n'
                '\tUbuntu 12.04, Debian: ==7').format(
                        pkg=pkg, ver=ver, os=linux.os['name'], req_ver=req_ver)
            raise exceptions.UnsupportedBehavior(cls.behavior, msg)
        else:
            raise exceptions.UnsupportedBehavior(cls.behavior, e)

