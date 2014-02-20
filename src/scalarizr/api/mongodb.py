'''
Created on Feb 25, 2011

@author: uty
'''

from __future__ import with_statement

import os
import re
import subprocess as subps

from scalarizr import rpc
from scalarizr import util
from scalarizr import linux
from scalarizr.node import __node__
from scalarizr.util.cryptotool import pwgen
from scalarizr.util import Singleton, software
from scalarizr.linux import pkgmgr
from scalarizr.services import mongodb as mongo_svc
from scalarizr import exceptions


class _MMSAgent(object):
    """
    Private class for mms-agent

    .. _a link: http://www.10gen.com/products/mongodb-monitoring-service
    """

    url = 'https://mms.10gen.com/settings/10gen-mms-agent.tar.gz'
    install_dir = '/opt'
    ps = None

    def _download(self):
        if not os.path.isfile('/tmp/10gen-mms-agent.tar.gz'):
            out, err, returncode = util.system2(
                    ['wget', '-O', '/tmp/10gen-mms-agent.tar.gz', _MMSAgent.url])


    def install(self):
        """"
        Download and install MMS agent
        """

        if not os.path.exists('%s/mms-agent' % _MMSAgent.install_dir):
            self._download()
            out, err, returncode = util.system2(
                    ['tar', '-xf', '/tmp/10gen-mms-agent.tar.gz', '-C', _MMSAgent.install_dir])


    def configure(self, api_key, secret_key):
        """
        Set user, password, api_key and secret_key

        :type api_key: string
        :param api_key: MMS api key

        :type secret_key: string
        :param secret_key: MMS secret key
        """

        user = 'scalr'
        password = __node__['mongodb']['password']

        with open('%s/mms-agent/settings.py' % _MMSAgent.install_dir, 'r') as f:
            content = f.read()

        content = re.sub(r'\nmms_key\b[ ]*=[ ]*".*"', '\nmms_key = "%s"'\
                % api_key, content)
        content = re.sub(r'\nsecret_key\b[ ]*=[ ]*".*"', '\nsecret_key = "%s"'\
                % secret_key, content)
        content = re.sub(r'\nglobalAuthUsername\b[ ]*=[ ]*".*"', '\nglobalAuthUsername = """%s"""'\
                % user, content)
        content = re.sub(r'\nglobalAuthPassword\b[ ]*=[ ]*".*"', '\nglobalAuthPassword = """%s"""'\
                % password, content)

        with open('%s/mms-agent/settings.py' % _MMSAgent.install_dir, 'w+') as f:
            f.write(content)


    def start(self):
        """
        Start MMS agent
        """

        if not _MMSAgent.ps:
            _MMSAgent.ps = subps.Popen(['python', '%s/mms-agent/agent.py' % _MMSAgent.install_dir],
                    close_fds=True, preexec_fn=os.setsid, stdout=None, stderr=None)


    def stop(self):
        """
        Stop MMS agent
        """

        if _MMSAgent.ps:
            util.kill_childs(_MMSAgent.ps.pid)
            _MMSAgent.ps.terminate()
            _MMSAgent.ps = None


class MongoDBAPI:
    """
    Basic API for managing MongoDB 2.x service.

    Namespace::

        mongodb
    """

    __metaclass__ = Singleton
    last_check = False

    @rpc.command_method
    def reset_password(self):
        """
         Resets password for MongoDB user 'scalr'.

         :return: new 10-char password.
         :rtype: str
        """
        #TODO: review and finish this method
        new_password = pwgen(10)
        mdb = mongo_svc.MongoDB()
        mdb.cli.create_or_update_admin_user(mongo_svc.SCALR_USER,
                                            new_password)
        return new_password


    @rpc.command_method
    def enable_mms(self, api_key, secret_key):
        """
        Enables MongoDB Management Service (MMS).

        :type api_key: string
        :param api_key: MMS api key

        :type secret_key: string
        :param secret_key: MMS secret key

        :rtype: dict
        :return: dictionary {'status':Ok|Fail, 'error':ErrorString}
        """

        status = 'Ok'
        error = ''

        mms_agent = _MMSAgent()
        try:
            mms_agent.install()
            mms_agent.configure(api_key, secret_key)
            mms_agent.start()
        except Exception, e:
            status = 'Fail'
            error = str(e)

        return {'status':status, 'error':error}


    @rpc.command_method
    def disable_mms(self):
        """
        Disables MongoDB Management Service (MMS).

        :rtype: dict
        :return: dictionary {'status':Ok|Fail, 'error':ErrorString}
        """

        status = 'Ok'
        error = ''

        mms_agent = _MMSAgent()
        try:
            mms_agent.stop()
        except Exception, e:
            status = 'Fail'
            error = str(e)

        return {'status':status, 'error':error}

    @classmethod
    def check_software(cls, installed_packages=None):
        """
        Asserts MongoDB version.
        :raises: scalarizr.exceptions.UnsupportedBehavior
        """
        try:
            MongoDBAPI.last_check = False
            os_name = linux.os['name'].lower()
            if os_name == 'ubuntu':
                if linux.os['version'] >= '12':
                    required_list = [
                        ['mongodb-10gen>=2.0,<2.5'],
                        ['mongodb>=2.0,<2.5']
                    ]
                elif linux.os['version'] >= '10':
                    required_list = [
                        ['mongodb-10gen>=2.0,<2.1'],
                        ['mongodb>=2.0,<2.1']
                    ]
            elif os_name == 'debian':
                required_list = [
                    ['mongodb-10gen>=2.4,<2.5'],
                    ['mongodb>=2.4,<2.5']
                ]
            elif os_name == 'centos':
                if linux.os['version'] >= '6':
                    required_list = [
                        ['mongodb-10gen-server>=2.0,<2.5'],
                        ['mongodb-server>=2.0,<2.5']
                    ]
                elif linux.os['version'] >= '5':
                    required_list = [
                        ['mongodb-10gen-server>=2.0,<2.1'],
                        ['mongodb-server>=2.0,<2.1']
                    ]
            elif linux.os.redhat_family:
                required_list = [
                    ['mongodb-10gen-server>=2.4,<2.5'],
                    ['mongodb-server>=2.4,<2.5']
                ]
            elif linux.os.oracle_family:
                required_list = [
                    ['mongodb-10gen-server>=2.0,<2.1'],
                    ['mongodb-server>=2.0,<2.1']
                ]
            else:
                raise exceptions.UnsupportedBehavior('mongodb',
                    "'mongodb' behavior is only supported on " +\
                    "Debian, RedHat or Oracle operating system family"
                )

            pkgmgr.check_any_dependency(required_list, installed_packages)
            MongoDBAPI.last_check = True
        except pkgmgr.DependencyError as e:
            software.handle_dependency_error(e, 'mongodb')

