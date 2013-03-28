'''
Created on Feb 25, 2011

@author: uty
'''

from __future__ import with_statement

import os
import subprocess as subps

from scalarizr import rpc
from scalarizr import util
from scalarizr.node import __node__
from scalarizr.util.cryptotool import pwgen
from scalarizr.services import mongodb as mongo_svc


class _MMSAgent(object):
    url = 'https://mms.10gen.com/settings/10gen-mms-agent.tar.gz'
    install_dir = '/opt'
    ps = None

    @staticmethod
    def _download():
        if not os.path.isfile('/tmp/10gen-mms-agent.tar.gz'):
            out, err, returncode = util.system2(
                    ['wget', '-O', '/tmp/10gen-mms-agent.tar.gz', _MMSAgent.url])


    @staticmethod
    def install():
        """"
        Download and install MMS agent
        """
        if not os.path.exists('%s/mms-agent' % _MMSAgent.install_dir):
            _MMSAgent._download()
            out, err, returncode = util.system2(
                    ['tar', '-xf', '/tmp/10gen-mms-agent.tar.gz', '-C', _MMSAgent.install_dir])


    @staticmethod
    def configure(mms_key, secret_key):
        """
        Set user, password, mms_key and secret_key
        """
        user = 'scalr'
        password = __node__['mongodb']['password']
        
        with open('%s/mms-agent/settings.py' % _MMSAgent.install_dir, 'r') as f:
            content = f.read()

        for line in content.split('\n'):
            if line.startswith('mms_key ='):
                content = content.replace(line, 'mms_key = "%s"' % mms_key)
            if line.startswith('secret_key ='):
                content = content.replace(line, 'secret_key = "%s"' % secret_key)
            if line.startswith('globalAuthUsername'):
                content = content.replace(line, 'globalAuthUsername = """%s"""' % user)
            if line.startswith('globalAuthPassword'):
                content = content.replace(line, 'globalAuthPassword = """%s"""' % password)

        with open('%s/mms-agent/settings.py' % _MMSAgent.install_dir, 'w+') as f:
            f.write(content)


    @staticmethod
    def start():
        """
        Start MMS
        """
        if not _MMSAgent.ps:
            _MMSAgent.ps = subps.Popen(['python', '%s/mms-agent/agent.py' % _MMSAgent.install_dir],
                    close_fds=True, preexec_fn=os.setsid, stdout=None, stderr=None)


    @staticmethod
    def stop():
        """
        Stop MMS
        """
        if _MMSAgent.ps:
            util.kill_childs(_MMSAgent.ps.pid)
            _MMSAgent.ps.terminate()
            _MMSAgent.ps = None


class MongoDBAPI:

    @rpc.service_method
    def reset_password(self):
        """ Reset password for Mongo user 'scalr'. Return new password  """
        #TODO: review and finish this method
        new_password = pwgen(10)
        mdb = mongo_svc.MongoDB()
        mdb.cli.create_or_update_admin_user(mongo_svc.SCALR_USER,
                                            new_password)
        return new_password


    @rpc.service_method
    def enable_mms(self, mms_key, secret_key):
        status = 'OK'
        error = ''

        try:
            _MMSAgent.install()
            _MMSAgent.configure(mms_key, secret_key)
            _MMSAgent.start()
        except Exception, e:
            status = 'Fail'
            error = str(e)

        return {'status':status, 'error':error}


    @rpc.service_method
    def disable_mms(self):
        status = 'OK'
        error = ''

        try:
            _MMSAgent.stop()
        except Exception, e:
            status = 'Fail'
            error = str(e)

        return {'status':status, 'error':error}
