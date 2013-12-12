'''
Created on Jan 23, 2012

@author: marat
'''

import logging
import urllib2
import glob
import json
import time
import sys
import os
import re
import binascii
import shutil
import uuid

from scalarizr import linux, queryenv, rpc
from scalarizr.bus import bus
from scalarizr.util import metadata, initdv2
from scalarizr.linux import pkgmgr
from scalarizr.messaging import p2p as messaging
from scalarizr.api import operation
from scalarizr.api.binding import jsonrpc_http


if linux.os.windows:
    import win32com
    import win32com.client


LOG = logging.getLogger(__name__)
DATE_FORMAT = '%a %d %b %Y %H:%M:%S UTC'


class UpdateError(Exception):
    pass

class NoSystemUUID(Exception):
    pass


def norm_user_data(data):
    data['server_id'] = data['serverid']
    data['messaging_url'] = data['p2p_producer_endpoint']
    return data


def value_for_repository(deb=None, rpm=None, win=None):
    if linux.os.windows:
        return win
    elif linux.os.redhat_family or linux.os.oracle_family:
        return rpm
    else:
        return deb


class UpdClientAPI(object):

    package = 'scalarizr'
    client_mode = 'client'
    api_port = 8008
    talk_timeout = 60
    server_url = 'http://update.scalr.net/'
    repository = 'latest'
    repo_url = value_for_repository(
        deb='http://apt.scalr.net/debian scalr/',
        rpm='http://rpm.scalr.net/rpm/rhel/$releasever/$basearch',
        win='http://win.scalr.net'
    )

    server_id = system_id = platform = queryenv_url = messaging_url = None
    scalr_id = scalr_version = None
    update_status = None

    update_server = None
    scalarizr = None
    queryenv = None
    pkgmgr = None
    daemon = None
    meta = None

    system_matches = False

    if linux.os.windows:
        _etc_path = r'C:\Program Files\Scalarizr\etc'
    else:
        _etc_path = '/etc/scalr'
    _private_path = os.path.join(_etc_path, 'private.d')
    lock_file = os.path.join(_private_path, 'update.lock')
    crypto_file = os.path.join(_private_path, 'keys', 'default')
    del _etc_path, _private_path

    @property
    def is_solo_mode(self):
        return self.client_mode == 'solo'

    @property
    def is_client_mode(self):
        return self.client_mode == 'client'


    def update_state():
        # pylint: disable=E0211, E0202, W0612
        def fget(self):
            return self.update_status['state']
        def fset(self, value):
            self.update_status['state'] = value
        return locals()
    update_state = property(**update_state())


    def __init__(self, **kwds):
        self.__dict__.update(kwds)
        self.pkgmgr = pkgmgr.package_mgr()
        self.daemon = initdv2.Daemon('scalarizr')
        self.op_api = operation.OperationAPI()
        self.update_status = {'state': 'unknown'}
        self.meta = metadata.meta()


    def _init_queryenv(self):
        args = (self.queryenv_url, 
                self.server_id, 
                self.crypto_file)
        self.queryenv = queryenv.QueryEnvService(*args)
        self.queryenv = queryenv.QueryEnvService(*args, 
                        api_version=self.queryenv.get_latest_version())        


    def _init_services(self):
        self._init_queryenv()

        bus.messaging_service = messaging.P2pMessageService(
                server_id=self.server_id,
                crypto_key_path=self.crypto_file,
                producer_url=self.messaging_url,
                producer_retries_progression='1,2,5,10,20,30,60')

        if self.is_client_mode:
            self.update_server = jsonrpc_http.HttpServiceProxy(self.server_url, self.crypto_file, 
                            server_id=self.server_id)

        self.scalarizr = jsonrpc_http.HttpServiceProxy('http://0.0.0.0:8010/', self.crypto_file) 


    def _try_init_devel(self, user_data):
        if user_data.get('realrolename', '').endswith('-devel') \
                and user_data.get('env_id') == '3414' \
                and user_data.get('custom.scm_branch'):
            LOG.info('Devel branch: %s', user_data['custom.scm_branch'])
            norm_branch = user_data['custom.scm_branch'].replace('/','-').replace('.','').strip()
            repo_url = value_for_repository(
                deb='http://buildbot.scalr-labs.com/apt/debian {0}/'.format(norm_branch),
                rpm='http://buildbot.scalr-labs.com/rpm/{0}/rhel/$releasever/$basearch'.format(
                        norm_branch),
                win='http://buildbot.scalr-labs.com/win/{0}/'.format(norm_branch)
            )
            if not linux.os.windows:
                devel_repo = pkgmgr.repository('dev-scalr', repo_url)
                # Pin repository
                if linux.os.redhat_family or linux.os.oracle_family:
                    devel_repo.config += 'protected=1\n'
                else:
                    if os.path.isdir('/etc/apt/preferences.d'):
                        prefile = '/etc/apt/preferences.d/dev-scalr'
                    else:
                        prefile = '/etc/apt/preferences'
                    with open(prefile, 'w+') as fp:
                        fp.write((
                            'Package: *\n'
                            'Pin: release a={0}\n'
                            'Pin-Priority: 990\n'
                        ).format(norm_branch))

                # for RPM it's important to uninstall package before protect devel repository
                self.uninstall() 
                devel_repo.ensure()
            else:
                self.repo_url = repo_url

    def get_system_id(self):
        if linux.os.windows:
            wmi = win32com.client.GetObject('winmgmts:')
            ret = wmi.ExecQuery('SELECT SerialNumber FROM Win32_BIOS').SerialNumber
            if not ret:
                LOG.debug('WMI returns empty UUID')
        else:
            ret = linux.system('dmidecode -s system-uuid', shell=True)[0].strip()
            if not ret:
                LOG.debug('dmidecide returns empty UUID')

        if not ret:
            if self.meta.platform == 'ec2':
                ret = self.meta['instance-id']
            elif self.meta.platform == 'gce':
                ret = self.meta['id']
            else:
                LOG.debug("Don't know how to get instance-id on '%s' platform", self.meta.platform)
        if not ret:
            LOG.debug('System UUID not detected')
            raise NoSystemUUID()
        return ret

    def bootstrap(self, dry_run=False):
        try:
            self.system_id = self.get_system_id()
        except:
            # This will force updclient to perform check updates each startup, 
            # this is the optimal behavior cause that's ensure latest available package
            self.system_id = str(uuid.uuid4())
        system_matches = False
        if os.path.exists(self.lock_file):
            LOG.debug('Checking %s', self.lock_file)
            with open(self.lock_file) as fp:
                updatelock = json.load(fp)
            system_matches = updatelock['system_id'] == self.system_id
            if not system_matches:
                LOG.debug('System ID in lock file and machine one not matched: %s != %s', 
                        updatelock['system_id'], self.system_id)
            else:
                LOG.debug('Serial number in lock file matches machine one')

        if system_matches:
            self.__dict__.update(updatelock)
            self.update_status = updatelock
        else:
            user_data = self.meta.user_data()
            norm_user_data(user_data)

            self.__dict__.update(user_data)
            self._try_init_devel(user_data)

            crypto_dir = os.path.dirname(self.crypto_file)
            if not os.path.exists(crypto_dir):
                os.makedirs(crypto_dir)
            if os.path.exists(self.crypto_file):  
                try:
                    self._init_queryenv()
                except queryenv.InvalidSignatureError:
                    pass
            if not self.queryenv:
                with open(self.crypto_file, 'w+') as fp:
                    fp.write(user_data['szr_key'])


        if not linux.os.windows:
            self.package = 'scalarizr-' + self.platform
        self._init_services()

        self.system_matches = system_matches
        if not self.system_matches:
            if not dry_run:
                self.update(bootstrap=True)
        else:
            if self.update_state == 'completed/wait-ack':
                self.update_state = 'completed'
            else:
                self.update_state = 'noop'
        self.save_lock()


    def uninstall(self):
        pkgmgr.removed(self.package)
        if not linux.os.windows:
            pkgmgr.removed('scalarizr-base', purge=True)
        if linux.os.debian_family:
            self.pkgmgr.apt_get_command('autoremove')     


    def sync(self):
        globs = self.queryenv.get_global_config()['params']
        self.__dict__.update((key[7:].replace('.', '_'), value) 
                    for key, value in globs.items() 
                    if key.startswith('update.'))
        self.repo_url = value_for_repository(
            deb=globs.get('update.deb.repo_url'),
            rpm=globs.get('update.rpm.repo_url'),
            win=globs.get('update.win.repo_url')
            ) or self.repo_url
        self.scalr_id = globs['scalr.id']
        self.scalr_version = globs['scalr.version']
        
        repo = pkgmgr.repository('scalr-{0}'.format(self.repository), self.repo_url)
        # Delete previous repository 
        for filename in glob.glob(os.path.dirname(repo.filename) + os.path.sep + 'scalr-*'):
            if os.path.isfile(filename):
                os.remove(filename)
        # Ensure new repository
        repo.ensure()
        LOG.info('Updating packages cache')
        self.pkgmgr.updatedb()      


    @rpc.command_method
    def update(self, force=False, bootstrap=False, async=False, **kwds):
        # pylint: disable=R0912
        if bootstrap:
            force = True
        notifies = not bootstrap
        reports = self.is_client_mode and not bootstrap

        def do_update(op):
            self.sync()
            self.update_status = {
                # object state
                'server_id': self.server_id,
                'system_id': self.system_id,
                'platform': self.platform,
                'queryenv_url': self.queryenv_url,
                'messaging_url': self.messaging_url,
                'scalr_id': self.scalr_id,
                'scalr_version': self.scalr_version,
                # update info
                'repository': self.repository,
                'package': self.package,
                'executed_at': time.strftime(DATE_FORMAT, time.gmtime()),
                'dist': '{name} {release} {codename}'.format(**linux.os),
                'state': 'in-progress/prepare',
                'error': None
            }
            old_pkgmgr_logger = pkgmgr.LOG
            try:
                pkgmgr.LOG = op.logger
                if bootstrap:
                    self.update_state = 'in-progress/uninstall'
                    self.uninstall()

                pkginfo = self.pkgmgr.info(self.package)

                if not pkginfo['candidate']:
                    msg = 'No new version available ({0})'.format(self.package)
                    raise UpdateError(msg)
                self.update_status['version'] = pkginfo['candidate']

                if not force:
                    self.update_state = 'in-progress/check-allowed'
                    if self.scalarizr.operation.has_in_progress():
                        msg = ('Update denied ({0}={1}), '
                                'cause Scalarizr is performing log-term operation').format(
                                self.package, self.update_status['version'])
                        raise UpdateError(msg)
            
                    if self.is_client_mode:
                        try:
                            ok = self.update_server.update_allowed(**self.update_status)
                        except urllib2.URLError:
                            raise UpdateError('Update server is down for maintenance')
                        if not ok:
                            msg = ('Update denied ({0}={1}), possible issues detected in '
                                    'later version. Blocking all upgrades until Scalr support '
                                    'overrides.').format(
                                    self.package, self.update_status['version'])
                            raise UpdateError(msg)

                try:
                    self.update_state = 'in-progress/install'
                    self.pkgmgr.install(self.package, self.update_status['version'])

                    self.update_state = 'completed/wait-ack'
                    self.save_lock()

                    if not self.daemon.running:
                        self.daemon.start()

                    if reports:
                        self.report(True)
                    op.logger.info('Done')
                except:
                    if reports:
                        self.report(False)
                    raise
            except:
                e = sys.exc_info()[1]
                self.update_status['error'] = str(e)
                if isinstance(e, UpdateError):
                    op.logger.warn(str(e))
                else:
                    raise
            finally:
                pkgmgr.LOG = old_pkgmgr_logger

        return self.op_api.run('scalarizr.update', do_update, async=async, 
                    exclusive=True, notifies=notifies)

    def save_lock(self):
        with open(self.lock_file, 'w') as fp:
            json.dump(self.update_status, fp)     

    def report(self, ok):
        if not self.is_client_mode:
            LOG.debug('Reporting is not enabled in {0} mode'.format(self.client_mode))

        self.update_status['ok'] = ok
        if not ok:
            self.update_status['error'] = str(sys.exc_info()[1])
                
        self.update_server.report(**self.update_status)   


    @rpc.command_method
    def restart(self, force=False):
        getattr(self.daemon, 'forcerestart' if force else 'restart')()
        if not self.daemon.running:
            raise Exception('Service restart failed')
    

    @rpc.query_method
    def status(self):
        status = self.pkgmgr.info(self.package)
        status.update(self.update_status)
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
        if not re.search(r'^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{4}|'
                        '[A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)\n?$', content):
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

