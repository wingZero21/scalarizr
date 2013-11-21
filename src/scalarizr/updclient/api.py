'''
Created on Jan 23, 2012

@author: marat
'''

import logging
import urllib2
import string
import json
import time
import sys
import os
import re
import binascii
import shutil

from scalarizr import linux, queryenv, rpc
from scalarizr.util import metadata
from scalarizr.linux import pkgmgr
from scalarizr.api import operation
from scalarizr.api.binding import jsonrpc_http

if linux.os.windows_family:
    import win32com
    import win32com.client


LOG = logging.getLogger(__name__)
DATE_FORMAT = '%a %d %b %Y %H:%M:%S UTC'


class UpdateError(Exception):
    pass


class Daemon(object):
    def __init__(self, name):
        self.name = name
        if linux.os.name == 'Ubuntu' and linux.os.release >= (10, 4):
            self.init_script = 'service %s' % self.name
        else:
            self.init_script = '/etc/init.d/%s' % self.name
    
    if linux.os.windows_family:
        def ctl(self, command):
            return linux.system('sc %s "%s"' % (command, self.name), 
                        shell=True)
    else:
        def ctl(self, command):
            return linux.system('%s %s' % (self.init_script, command), 
                    close_fds=True, preexec_fn=os.setsid)
    
    def restart(self):
        LOG.info('Restarting %s', self.name)
        if linux.os.windows_family:
            self.ctl('stop')
            time.sleep(1)
            self.ctl('start')
        else:
            self.ctl('restart')
    
    def forcerestart(self):
        LOG.info('Forcefully restarting %s', self.name)
        self.ctl('stop')
        try:
            out = linux.system('ps -C %s --noheaders -o pid' % self.name)[0]
            for pid in out.strip().splitlines():
                LOG.debug('Killing process %s', pid)
                os.kill(pid, 9)
        finally:
            self.ctl('start')
    
    def condrestart(self):
        LOG.info('Conditional restarting %s', self.name)
        self.ctl('condrestart')
    
    def start(self):
        LOG.info('Starting %s', self.name)
        self.ctl('start')
    
    def stop(self):
        LOG.info('Stopping %s', self.name)
        self.ctl('stop')
    
    @property
    def running(self):
        if linux.os.windows_family:
            out = self.ctl('query')[0]
            lines = filter(None, map(string.strip, out.splitlines()))
            for line in lines:
                name, value = map(string.strip, line.split(':', 1))
                if name.lower() == 'state':
                    return value.lower().endswith('running')
        else:
            return not self.ctl('status')[2] 


def norm_user_data(data):
    data['server_id'] = data['serverid']
    return data


def system_uuid():
    if linux.os.windows_family:
        wmi = win32com.client.GetObject('winmgmts:')
        result = wmi.ExecQuery('SELECT SerialNumber FROM Win32_BIOS')
        uuid = result.SerialNumber
        if not uuid:
            LOG.debug('WMI returns empty UUID')
    else:
        uuid = linux.system('dmidecode -s system-uuid', shell=True)[0].strip()
        if not uuid:
            LOG.debug('dmidecide returns empty UUID')

    if not uuid:
        try:
            meta = metadata.meta()
        except:
            LOG.debug("Failed to init metadata (in system_uuid() method): %s", sys.exc_info()[1])
        else:
            if meta.platform == 'ec2':
                uuid = meta['instance-id']
            elif meta.platform == 'gce':
                uuid = meta['id']
            else:
                LOG.debug("Don't know how to get instance-id on '%s' platform", meta.platform)
    if not uuid:
        LOG.debug('System UUID not detected')
        uuid = '00000000-0000-0000-0000-000000000000'
    return uuid


class UpdClientAPI(object):

    package = 'scalarizr'
    client_mode = 'client'
    api_port = 8008
    server_url = 'http://update.scalr.net/'

    server_id = platform = repository = queryenv_url = repo_url = None
    scalr_id = scalr_version = None
    update_info = None

    update_server = None
    scalarizr = None
    queryenv = None
    pkgmgr = None
    daemon = None

    if linux.os.windows_family:
        etc_path = r'C:\Program Files\Scalarizr\etc'
    else:
        etc_path = '/etc/scalr'
    private_path = os.path.join(etc_path, 'private.d')
    updatelock_file = os.path.join(private_path, '.update.lock')
    crypto_file = os.path.join(private_path, 'keys', 'default')

    def __init__(self, **kwds):
        self.__dict__.update(kwds)
        self.pkgmgr = pkgmgr.package_mgr()
        self.daemon = Daemon('scalarizr')
        self._op_api = operation.OperationAPI()


    def bootstrap(self):
        system_id = system_uuid()
        system_matches = False
        if os.path.exists(self.updatelock_file):
            LOG.debug('Checking %s', self.updatelock_file)
            with open(self.updatelock_file) as fp:
                updatelock = json.load(fp)
            system_matches = updatelock['system_uuid'] == system_id
            if not system_matches:
                LOG.debug('Serial number in lock file and machine one not matched: %s != %s', 
                        updatelock['system_uuid'], system_id)
                os.unlink(self.updatelock_file)

        if system_matches:
            self.__dict__.update(updatelock)
        else:
            meta = metadata.meta(timeout=60)
            user_data = meta.user_data()
            norm_user_data(user_data)
            self.__dict__.update(user_data)
            self.package = 'scalarizr-' + self.platform
            crypto_dir = os.path.dirname(self.crypto_file)
            if not os.path.exists(crypto_dir):
                os.makedirs(crypto_dir)
            with open(self.crypto_file, 'w+') as fp:
                fp.write(user_data['szr_key'])

        args = (self.queryenv_url, 
                self.server_id, 
                self.crypto_file)
        self.queryenv = queryenv.QueryEnvService(*args)
        self.queryenv = queryenv.QueryEnvService(*args, 
                        api_version=self.queryenv.get_latest_version())

        self.update_server = jsonrpc_http.HttpServiceProxy(self.server_url, self.crypto_file, 
                        server_id=self.server_id)

        self.scalarizr = jsonrpc_http.HttpServiceProxy('http://0.0.0.0:8010/', self.crypto_file)

        if not system_matches:
            self.update(force=True, reporting=False)
        self.daemon.start()




    def sync(self):
        globs = self.queryenv.get_global_config()['params']
        self.__dict__.update(dict((key[7:].replace('.', '_')) 
                    for key, value in globs.items() 
                    if key.startswith('update.')))
        if linux.os.windows_family:
            self.repo_url = globs['update.win.repo_url']
        elif linux.os.linux_family in ('RedHat', 'Oracle'):
            self.repo_url = globs['update.rpm.repo_url']
        else:
            self.repo_url = globs['update.deb.repo_url']
        self.scalr_id = globs['scalr.id']
        self.scalr_version = globs['scalr.version']
        
        repo = pkgmgr.repository(self.repository, self.repo_url)
        repo.ensure()
        self.pkgmgr.updatedb()      


    @property
    def is_solo_mode(self):
        return self.client_mode == 'solo'


    @property
    def is_client_mode(self):
        return self.client_mode == 'client'


    @rpc.command_method
    def update(self, force=True, reporting=True, async=False, **kwds):

        def do_update(op):
            if not self.is_client_mode:
                reporting = False
            self.sync()
            self.update_info = {
                'repository': self.repository,
                'package': self.package,
                'scalr_id': self.scalr_id,
                'scalr_version': self.scalr_version,
                'executed_at': time.strftime(DATE_FORMAT, time.gmtime()),
                'dist': '{name} {release} {codename}'.format(**linux.os),
                'phase': None,
                'error': None
            }
            try:
                self.update_info['version'] = self.pkgmgr.info(self.package)['candidate']

                if not self.update_info['version']:
                    msg = 'No new version available ({0})'.format(self.package)
                    raise UpdateError(msg)
                        
                if not force:
                    if self.scalarizr.operation.has_in_progress():
                        msg = ('Update denied ({0}={1}), '
                                'cause Scalarizr is performing log-term operation').format(
                                self.package, self.update_info['version'])
                        raise UpdateError(msg)
                
                    if self.is_client_mode:
                        try:
                            ok = self.update_server.update_allowed(**self.update_info)
                        except urllib2.URLError:
                            raise UpdateError('Update server is down for maintenance')
                        if not ok:
                            msg = ('Update denied ({0}={1}), possible issues detected in '
                                    'later version. Blocking all upgrades until Scalr support '
                                    'overrides.').format(self.package, self.update_info['version'])
                            raise UpdateError(msg)
                
                try:
                    op.logger.info('Installing {0}={1}'.format(
                            self.package, self.update_info['version']))

                    self.update_info['phase'] = 'install'
                    self.pkgmgr.install(self.package, self.update_info['version'])

                    if not self.daemon.running:
                        self.update_info['phase'] = 'start'
                        self.daemon.start()
                        time.sleep(1)  # wait a second to start
                        if not self.daemon.running:
                            msg = 'Restart failed ({0})'.format(self.daemon.name)
                            raise UpdateError(msg)

                    with open(self.updatelock_file, 'w') as fp:
                        json.dump(self.update_info, fp)

                    if reporting:
                        self.report(True)

                    op.logger.info('Done')
                except:
                    if reporting:
                        self.report(False)
                    raise
            except:
                e = sys.exc_info()[1]
                self.update_info['error'] = str(e)
                if isinstance(e, UpdateError):
                    op.logger.warn(str(e))
                else:
                    raise

        return self._op_api.run('scalarizr.update', do_update, exclusive=True, async=async)

    
    def report(self, ok):
        if not self.is_client_mode:
            LOG.debug('Reporting is not enabled in {0} mode'.format(self.client_mode))

        self.update_info['ok'] = ok
        if not ok:
            self.update_info['error'] = str(sys.exc_info()[1])
                
        self.update_server.report(**self.update_info)   


    @rpc.command_method
    def restart(self, force=False):
        getattr(self.daemon, 'forcerestart' if force else 'restart')()
        if not self.daemon.running:
            raise Exception('Service restart failed')
    

    @rpc.query_method
    def status(self):
        status = self.pkgmgr.info(self.package)
        status.update(self.update_info)
        status['candidate'] = status['candidate'] or status['installed']
        status['service_status'] = 'running' if self.daemon.running else 'stopped'
        return status
            

    @rpc.service_method
    def execute(self, command=None):
        out, err, ret = linux.system(command, shell=True)
        return {
            'stdout': out,
            'stderr': err,
            'return_code': ret
        }
    
    
    @rpc.service_method
    def put_file(self, name=None, content=None, makedirs=False):
        if not re.search(r'^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)\n?$', content):
            raise ValueError('File content is not a valid BASE64 encoded string')

        content = binascii.a2b_base64(content)

        directory = os.path.dirname(name)
        if makedirs and not os.path.exists(directory):
            os.makedirs(directory)
        
        tmpname = '%s.tmp' % name
        try:
            with open(tmpname, 'w') as dst:
                dst.write(content)
            shutil.move(tmpname, name)
        except:
            if os.path.exists(tmpname):
                os.remove(tmpname)
            raise
