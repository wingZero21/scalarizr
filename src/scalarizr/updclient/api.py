'''
Created on Jan 23, 2012

@author: marat
'''

import binascii
import glob
import json
import logging
import os
import pprint
import re
import shutil
import sqlite3 as sqlite
import subprocess
import sys
import time
import urllib2
import uuid

from scalarizr import linux, queryenv, rpc, config
from scalarizr.api import operation
from scalarizr.api.binding import jsonrpc_http
from scalarizr.bus import bus
from scalarizr.linux import pkgmgr
from scalarizr.messaging import p2p as messaging
from scalarizr.util import metadata, initdv2, sqlite_server


if linux.os.windows:
    import win32com
    import win32com.client
    import win32process


LOG = logging.getLogger(__name__)
DATE_FORMAT = '%a %d %b %Y %H:%M:%S UTC'


class UpdateError(Exception):
    pass

class NoSystemUUID(Exception):
    pass


def norm_user_data(data):
    data['server_id'] = data.pop('serverid')
    data['messaging_url'] = data.pop('p2p_producer_endpoint')
    data['farm_role_id'] = data.pop('farm_roleid')
    return data


def value_for_repository(deb=None, rpm=None, win=None):
    if linux.os.windows:
        return win
    elif linux.os.redhat_family or linux.os.oracle_family:
        return rpm
    else:
        return deb


def devel_repo_url_for_branch(branch):
    norm_branch = branch.replace('/','-').replace('.','').strip()
    return value_for_repository(
        deb='http://buildbot.scalr-labs.com/apt/debian {0}/'.format(norm_branch),
        rpm='http://buildbot.scalr-labs.com/rpm/{0}/rhel/$releasever/$basearch'.format(norm_branch),
        win='http://buildbot.scalr-labs.com/win/{0}/'.format(norm_branch))


class UpdClientAPI(object):

    package = 'scalarizr'
    client_mode = 'client'
    api_port = 8008
    win_update_timeout = 300
    server_url = 'http://update.scalr.net/'
    repository = 'latest'
    repo_url = value_for_repository(
        deb='http://apt.scalr.net/debian scalr/',
        rpm='http://rpm.scalr.net/rpm/rhel/$releasever/$basearch',
        win='http://win.scalr.net'
    )

    server_id = farm_role_id = system_id = platform = queryenv_url = messaging_url = None
    scalr_id = scalr_version = None
    state = installed = candidate = executed_at = error = dist = None

    update_server = None
    messaging_service = None
    scalarizr = None
    queryenv = None
    pkgmgr = None
    daemon = None
    meta = None

    system_matches = False

    if linux.os.windows:
        _base = r'C:\Program Files\Scalarizr'
        etc_path = os.path.join(_base, 'etc')
        share_path = os.path.join(_base, 'share')
        log_file = os.path.join(_base, r'var\log\scalarizr_update.log')
        win_status_file = os.path.join(etc_path, 'private.d\win-update.status')
        del _base
    else:
        etc_path = '/etc/scalr'
        share_path = '/usr/share/scalr'
        log_file = '/var/log/scalarizr_update.log'

    _private_path = os.path.join(etc_path, 'private.d')
    status_file = os.path.join(_private_path, 'update.status')
    crypto_file = os.path.join(_private_path, 'keys', 'default')
    db_file = os.path.join(_private_path, 'db.sqlite')
    del _private_path

    @property
    def is_solo_mode(self):
        return self.client_mode == 'solo'

    @property
    def is_client_mode(self):
        return self.client_mode == 'client'


    def __init__(self, **kwds):
        self.__dict__.update(kwds)
        self.pkgmgr = pkgmgr.package_mgr()
        self.daemon = initdv2.Daemon('scalarizr')
        self.op_api = operation.OperationAPI()
        self.dist = '{name} {release} {codename}'.format(**linux.os)
        self.state = 'unknown'
        self.meta = metadata.meta()


    def _init_queryenv(self):
        LOG.debug('Initializing QueryEnv')
        args = (self.queryenv_url, 
                self.server_id, 
                self.crypto_file)
        self.queryenv = queryenv.QueryEnvService(*args)
        self.queryenv = queryenv.QueryEnvService(*args, 
                        api_version=self.queryenv.get_latest_version())  
        bus.queryenv_service = self.queryenv      


    def _init_db(self):
        def connect_db():
            conn = sqlite.connect(self.db_file, 5.0)
            conn.row_factory = sqlite.Row
            conn.text_factory = sqlite.OptimizedUnicode  
            return conn

        if not os.path.exists(self.db_file) or not os.stat(self.db_file).st_size:
            LOG.debug('Creating SQLite database')
            conn = connect_db()
            try:
                with open(os.path.join(self.share_path, 'db.sql')) as fp:
                    conn.executescript(fp.read())
                conn.commit()
            finally:
                conn.close()

        # Configure database connection pool
        LOG.debug('Initializing database connection')
        t = sqlite_server.SQLiteServerThread(connect_db)
        t.setDaemon(True)
        t.start()
        sqlite_server.wait_for_server_thread(t)
        bus.db = t.connection


    def _init_services(self):
        if not self.queryenv:
            self._init_queryenv()

        if not bus.db:
            self._init_db()

        if not bus.cnf:
            bus.cnf = config.ScalarizrCnf(self.etc_path)
            bus.cnf.bootstrap()

        if not self.messaging_service:
            LOG.debug('Initializing messaging')
            bus.messaging_service = messaging.P2pMessageService(
                    server_id=self.server_id,
                    crypto_key_path=self.crypto_file,
                    producer_url=self.messaging_url,
                    producer_retries_progression='1,2,5,10,20,30,60')

        if self.is_client_mode and not self.update_server:
            self.update_server = jsonrpc_http.HttpServiceProxy(self.server_url, None)

        if not self.scalarizr:
            self.scalarizr = jsonrpc_http.HttpServiceProxy('http://0.0.0.0:8010/', self.crypto_file) 


    def get_system_id(self):
        if linux.os.windows:
            wmi = win32com.client.GetObject('winmgmts:')
            for row in wmi.ExecQuery('SELECT SerialNumber FROM Win32_BIOS'):
                ret = row.SerialNumber
                break
            else:
                LOG.debug('WMI returns empty UUID')
        else:
            ret = None
            try:
                ret = linux.system('dmidecode -s system-uuid', shell=True)[0].strip()
                if not ret:
                    LOG.debug('dmidecide returns empty UUID')
            except:
                LOG.debug(sys.exc_info()[1])

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
            LOG.debug('get system-id failed: %s', sys.exc_info()[1])
            self.system_id = str(uuid.uuid4())
        system_matches = False
        status_data = None
        if os.path.exists(self.status_file):
            LOG.debug('Checking %s', self.status_file)
            with open(self.status_file) as fp:
                status_data = json.load(fp)
            system_matches = status_data['system_id'] == self.system_id
            if not system_matches:
                LOG.debug('System ID in lock file and machine one not matched: %s != %s', 
                        status_data['system_id'], self.system_id)
            else:
                LOG.debug('Serial number in lock file matches machine one')

        if system_matches:
            LOG.debug('Apply %s settings', self.status_file)
            self.__dict__.update(status_data)
        else:
            LOG.debug('Getting cloud user-data')
            user_data = self.meta.user_data()
            norm_user_data(user_data)
            LOG.debug('Apply user-data settings')
            self.__dict__.update(user_data)

            crypto_dir = os.path.dirname(self.crypto_file)
            if not os.path.exists(crypto_dir):
                os.makedirs(crypto_dir)
            if os.path.exists(self.crypto_file): 
                LOG.debug('Testing that crypto key works (file: %s)', self.crypto_file) 
                try:
                    self._init_queryenv()
                    LOG.debug('Crypto key works')
                except queryenv.InvalidSignatureError:
                    LOG.debug("Crypto key doesn't work")
            if not self.queryenv:
                LOG.debug("Use crypto key from user-data")
                with open(self.crypto_file, 'w+') as fp:
                    fp.write(user_data['szr_key'])

        if not linux.os.windows:
            self.package = 'scalarizr-' + self.platform
        self._init_services()

        self.system_matches = system_matches
        if not self.system_matches:
            if dry_run:
                self._sync()  
                self.state = 'noop'        
            else:
                self.update(bootstrap=True)
        else:
            if self.state == 'completed/wait-ack':
                self.state = 'completed'
            elif linux.os.windows and self.state == 'in-progress/install' and \
                    os.path.exists(self.win_status_file):
                # todo: 
                # merge status and win_status
                # report status to update server
                self.state = 'completed'
            else:
                self.state = 'noop'
        if self.state != 'unknown':
            self.store(self.status(cached=True))


    def uninstall(self):
        pid = None
        if not linux.os.windows:
            # Prevent scalr-upd-client restart when updating from old versions 
            # package 'scalr-upd-client' replaced with 'scalarizr'
            pid_file = '/var/run/scalr-upd-client.pid'
            with open(pid_file) as fp:
                pid = fp.read().strip()
            with open(pid_file, 'w') as fp:
                fp.write('0')
        try:
            self.pkgmgr.removed(self.package)
            if not linux.os.windows:
                self.pkgmgr.removed('scalarizr-base', purge=True)
            if linux.os.debian_family:
                self.pkgmgr.apt_get_command('autoremove') 
        finally:
            if pid:
                with open(pid_file, 'w') as fp:
                    fp.write(pid)    


    def _sync(self):
        params = self.queryenv.list_farm_role_params(self.farm_role_id)
        update = params.get('params', {}).get('base', {}).get('update', {})
        self.__dict__.update(update)
        self.repo_url = value_for_repository(
            deb=update.get('deb_repo_url'),
            rpm=update.get('rpm_repo_url'),
            win=update.get('win_repo_url')
            ) or self.repo_url

        globs = self.queryenv.get_global_config()['params']
        self.scalr_id = globs['scalr.id']
        self.scalr_version = globs['scalr.version']
        

    def _ensure_repos(self):
        repo = pkgmgr.repository('scalr-{0}'.format(self.repository), self.repo_url)
        # Delete previous repository 
        for filename in glob.glob(os.path.dirname(repo.filename) + os.path.sep + 'scalr-*'):
            if os.path.isfile(filename):
                os.remove(filename)
        if 'buildbot.scalr-labs.com' in self.repo_url and not linux.os.windows:
            self._configure_devel_repo(repo)
        # Ensure new repository
        repo.ensure()
        LOG.info('Updating packages cache')
        self.pkgmgr.updatedb() 


    def _configure_devel_repo(self, repo):
        # Pin repository
        if linux.os.redhat_family or linux.os.oracle_family:
            pkg = 'yum-priorities' \
                    if linux.os['release'] < (6, 0) else \
                    'yum-plugin-priorities'
            self.pkgmgr.installed(pkg)
            repo.config += 'priority=10\n'
        else:
            if os.path.isdir('/etc/apt/preferences.d'):
                prefile = '/etc/apt/preferences.d/scalr'
            else:
                prefile = '/etc/apt/preferences'
            with open(prefile, 'w+') as fp:
                fp.write((
                    'Package: *\n'
                    'Pin: release a={0}\n'
                    'Pin-Priority: 990\n'
                ).format(self.repository))

        # Scalr repo has all required dependencies (like python-* libs, etc), 
        # while Branch repository has only scalarizr package
        release_repo = pkgmgr.repository('scalr-release', devel_repo_url_for_branch('scalr'))
        release_repo.ensure()


    @rpc.command_method
    def update(self, force=False, bootstrap=False, async=False, **kwds):
        # pylint: disable=R0912
        if bootstrap:
            force = True
        notifies = not bootstrap
        reports = self.is_client_mode and not bootstrap

        def check_allowed():
            if not force:
                self.state = 'in-progress/check-allowed'
                if self.scalarizr.operation.has_in_progress():
                    msg = ('Update denied ({0}={1}), '
                            'cause Scalarizr is performing log-term operation').format(
                            self.package, self.candidate)
                    raise UpdateError(msg)
        
                if self.is_client_mode:
                    try:
                        ok = self.update_server.update_allowed(
                                package=self.package,
                                version=self.candidate,
                                server_id=self.server_id,
                                scalr_id=self.scalr_id,
                                scalr_version=self.scalr_version)

                    except urllib2.URLError:
                        raise UpdateError('Update server is down for maintenance')
                    if not ok:
                        msg = ('Update denied ({0}={1}), possible issues detected in '
                                'later version. Blocking all upgrades until Scalr support '
                                'overrides.').format(
                                self.package, self.candidate)
                        raise UpdateError(msg)            

        def update_windows():
            package_url = self.pkgmgr.index[self.package]
            if os.path.exists(self.win_status_file):
                os.unlink(self.win_status_file)
            LOG.info('Invoke powershell script "update.ps1 -URL %s"', package_url)
            linux.system([
                    'powershell.exe', 
                    '-NoProfile', 
                    '-NonInteractive', 
                    '-ExecutionPolicy', 'RemoteSigned', 
                    '-File', os.path.join(os.path.dirname(__file__), 'update.ps1'),
                    '-URL', package_url
                ], 
                env=os.environ, 
                #creationflags=win32process.DETACHED_PROCESS,
                stdout=open(self.log_file, 'a+'),
                stderr=subprocess.STDOUT)
            LOG.debug('Waiting for interruption (Timeout: %s)', self.win_update_timeout)
            time.sleep(self.win_update_timeout)
            msg = ('UpdateClient expected to be terminated by update.ps1, '
                    'but never happened')
            raise UpdateError(msg)


        def update_linux():
            self.pkgmgr.install(self.package, self.candidate)
            self.state = 'completed/wait-ack'
            self.installed = self.candidate
            self.store()

            if not self.daemon.running:
                self.daemon.start()

            if reports:
                self.report(True)
            return self.status(cached=True)


        def do_update(op):
            self.executed_at = time.strftime(DATE_FORMAT, time.gmtime())
            self.state = 'in-progress/prepare'
            self.error = ''
            self._sync()
            self._ensure_repos()

            old_pkgmgr_logger = pkgmgr.LOG
            try:
                pkgmgr.LOG = op.logger
                if bootstrap and not linux.os.windows:
                    self.state = 'in-progress/uninstall'
                    self.uninstall()

                self.__dict__.update(self.pkgmgr.info(self.package))
                if not self.candidate:
                    msg = 'No new version available ({0})'.format(self.package)
                    raise UpdateError(msg)

                check_allowed()
                
                try:
                    self.state = 'in-progress/install'
                    if linux.os.windows:
                        # raises KeyboardInterrupt
                        update_windows()
                    else:
                        return update_linux()
                except KeyboardInterrupt:
                    if not linux.os.windows:
                        op.cancel()
                    return
                except:
                    if reports:
                        self.report(False)
                    raise
            except:
                e = sys.exc_info()[1]
                self.error = str(e)
                self.state = 'error'
                if isinstance(e, UpdateError):
                    op.logger.warn(str(e))
                    return self.status(cached=True)
                else:
                    raise
            finally:
                pkgmgr.LOG = old_pkgmgr_logger

        return self.op_api.run('scalarizr.update', do_update, async=async, 
                    exclusive=True, notifies=notifies)


    def store(self, status=None):
        status = status or self.status()
        with open(self.status_file, 'w') as fp:
            LOG.debug('Saving status: %s', pprint.pformat(status))
            json.dump(status, fp)     


    def report(self, ok):
        if not self.is_client_mode:
            LOG.debug('Reporting is not enabled in {0} mode'.format(self.client_mode))
            return
        error = str(sys.exc_info()[1]) if not ok else ''                
        self.update_server.report(
                ok=ok, package=self.package, version=self.candidate, 
                server_id=self.server_id, scalr_id=self.scalr_id, scalr_version=self.scalr_version, 
                phase=self.state, dist=self.dist, error=error)   


    @rpc.command_method
    def restart(self, force=False):
        getattr(self.daemon, 'forcerestart' if force else 'restart')()
        if not self.daemon.running:
            raise Exception('Service restart failed')


    @rpc.query_method
    def status(self, cached=False):
        keys_to_copy = [
            'server_id', 'farm_role_id', 'system_id', 'platform', 'queryenv_url', 
            'messaging_url', 'scalr_id', 'scalr_version', 
            'repository', 'repo_url', 'package', 'executed_at', 
            'state', 'error', 'dist'
        ]
        status = {} if cached else self.pkgmgr.info(self.package)
        if cached:
            keys_to_copy.extend(['candidate', 'installed'])
        for key in keys_to_copy:
            status[key] = getattr(self, key)
        if not cached:
            status['service_status'] = 'running' if self.daemon.running else 'stopped'
        else:
            status['service_status'] = 'unknown'
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

