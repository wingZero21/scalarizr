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

    def _download():
        if not os.path.isfile('/tmp/10gen-mms-agent.tar.gz'):
            out, err, returncode = util.system2(
                    ['wget', '-O', '/tmp/10gen-mms-agent.tar.gz', _MMSAgent.url])


    def install():
        """"
        Download and install MMS agent
        """
        if not os.path.exists('%s/mms-agent' % _MMSAgent.install_dir):
            _MMSAgent._download()
            out, err, returncode = util.system2(
                    ['tar', '-xf', '/tmp/10gen-mms-agent.tar.gz', '-C', _MMSAgent.install_dir])


    def configure(api_key, secret_key):
        """
        Set user, password, api_key and secret_key
        """
        user = 'scalr'
        password = __node__['mongodb']['password']
        
        with open('%s/mms-agent/settings.py' % _MMSAgent.install_dir, 'r') as f:
            content = f.read()

        for line in content.split('\n'):
            if line.startswith('mms_key ='):
                content = content.replace(line, 'mms_key = "%s"' % api_key)
            if line.startswith('secret_key ='):
                content = content.replace(line, 'secret_key = "%s"' % secret_key)
            if line.startswith('globalAuthUsername'):
                content = content.replace(line, 'globalAuthUsername = """%s"""' % user)
            if line.startswith('globalAuthPassword'):
                content = content.replace(line, 'globalAuthPassword = """%s"""' % password)

        with open('%s/mms-agent/settings.py' % _MMSAgent.install_dir, 'w+') as f:
            f.write(content)


    def start():
        """
        Start MMS
        """
        if not _MMSAgent.ps:
            _MMSAgent.ps = subps.Popen(['python', '%s/mms-agent/agent.py' % _MMSAgent.install_dir],
                    close_fds=True, preexec_fn=os.setsid, stdout=None, stderr=None)


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
    def enable_mms(self, api_key, secret_key):
        status = 'OK'
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


    @rpc.service_method
    def disable_mms(self):
        status = 'OK'
        error = ''

        mms_agent = _MMSAgent()
        try:
            mms_agent.stop()
        except Exception, e:
            status = 'Fail'
            error = str(e)

        return {'status':status, 'error':error}
