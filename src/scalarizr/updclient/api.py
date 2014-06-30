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
import threading
import urllib2
import uuid
import time
import pkg_resources
import multiprocessing

from scalarizr import linux, queryenv, rpc, config, __version__
from scalarizr.api import operation
from scalarizr.api.binding import jsonrpc_http
from scalarizr.bus import bus
from scalarizr.linux import pkgmgr, coreutils
from scalarizr.messaging import p2p as messaging
from scalarizr.util import metadata, initdv2, sqlite_server, wait_until

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
    data['server_id'] = data.pop('serverid')
    data['messaging_url'] = data.pop('p2p_producer_endpoint')
    # - my uptime is 1086 days, 55 mins, o-ho-ho
    if data['messaging_url'] == 'http://scalr.net/messaging':
        data['messaging_url'] = 'https://my.scalr.com/messaging'
    if data['queryenv_url'] == 'http://scalr.net/query-env':
        data['queryenv_url'] = 'https://my.scalr.com/query-env'
    data['farm_role_id'] = data.pop('farm_roleid', None)  
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


def get_win_process(pid):
    wmi = win32com.client.GetObject('winmgmts:')
    for proc in wmi.ExecQuery('SELECT * FROM Win32_Process WHERE ProcessId = {0}'.format(pid)):
        return proc
    raise LookupError('Process {0!r} not found'.format(pid))


class UpdClientAPI(object):
    '''
    States:
     * noop - initial state 
     * in-progress -  update performed
     * completed - new package installed
     * rollbacked - new package installation failed, so previous was restored
     * error - update failed and unrecovered

    Transitions:
     noop -> in-progress
     in-progress -> completed -> in-progress
     in-progress -> rollbacked -> in-progress
     in-progress -> error -> in-progress
    '''

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
    downgrades_enabled = True

    server_id = farm_role_id = system_id = platform = queryenv_url = messaging_url = None
    scalr_id = scalr_version = None
    _state = prev_state = installed = candidate = executed_at = error = dist = None
    ps_script_pid = None
    ps_attempt = 0

    update_server = None
    messaging_service = None
    scalarizr = None
    queryenv = None
    pkgmgr = None
    daemon = None
    meta = None
    shutdown_ev = None

    system_matches = False

    if linux.os.windows:
        _base = r'C:\Program Files\Scalarizr'
        etc_path = os.path.join(_base, 'etc')
        share_path = os.path.join(_base, 'share')
        log_file = os.path.join(_base, r'var\log\scalarizr_update.log')
        del _base
    else:
        etc_path = '/etc/scalr'
        share_path = '/usr/share/scalr'
        log_file = '/var/log/scalarizr_update.log'

    _private_path = os.path.join(etc_path, 'private.d')
    status_file = os.path.join(_private_path, 'update.status')
    win_status_file = os.path.join(_private_path, 'update_win.status')
    crypto_file = os.path.join(_private_path, 'keys', 'default')
    db_file = os.path.join(_private_path, 'db.sqlite')
    del _private_path

    @property
    def is_solo_mode(self):
        return self.client_mode == 'solo'

    @property
    def is_client_mode(self):
        return self.client_mode == 'client'

    def state():
        # pylint: disable=E0211, E0202
        def fget(self):
            return self._state
        def fset(self, state):
            self.prev_state = self._state
            self._state = state
            LOG.info('State transition: {0} -> {1}'.format(self.prev_state, state))
        return locals()
    state = property(**state())


    def __init__(self, **kwds):
        self._update_self_dict(kwds)
        self.pkgmgr = pkgmgr.package_mgr()
        self.daemon = initdv2.Daemon('scalarizr')
        self.op_api = operation.OperationAPI()
        self.dist = '{name} {release} {codename}'.format(**linux.os)
        self.state = 'noop'
        self.meta = metadata.Metadata()
        self.shutdown_ev = threading.Event()
        self.early_bootstrapped = False


    def _update_self_dict(self, data):
        self.__dict__.update(data)
        if 'state' in data:
            self.__dict__['_state'] = data['state']


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
        if not bus.db:
            self._init_db()

        if not bus.cnf:
            bus.cnf = config.ScalarizrCnf(self.etc_path)
            bus.cnf.bootstrap()

        if not self.queryenv:
            def init_queryenv():
                try:
                    self._init_queryenv()
                    return True
                except queryenv.InvalidSignatureError:
                    if bus.cnf.state == 'bootstrapping':
                        LOG.debug('Ignore InvalidSignatureError while Scalarizr is bootstrapping, retrying...')
                        return False
                    else:
                        raise
            wait_until(init_queryenv, timeout=120, sleep=10)

        if not self.messaging_service:
            LOG.debug('Initializing messaging')
            bus.messaging_service = messaging.P2pMessageService(
                    server_id=self.server_id,
                    crypto_key_path=self.crypto_file,
                    producer_url=self.messaging_url,
                    producer_retries_progression='1,2,5,10,20,30,60')

        if self.is_client_mode and not self.update_server:
            self.update_server = jsonrpc_http.HttpServiceProxy(self.server_url, self.crypto_file, 
                    server_id=self.server_id, 
                    sign_only=True)

        if not self.scalarizr:
            self.scalarizr = jsonrpc_http.HttpServiceProxy('http://localhost:8010/', self.crypto_file) 


    def get_system_id(self):
        def win32_serial_number():
            try:
                wmi = win32com.client.GetObject('winmgmts:')
                for row in wmi.ExecQuery('SELECT SerialNumber FROM Win32_BIOS'):
                    return row.SerialNumber
                else:
                    LOG.debug('WMI returns empty UUID')
            except:
                LOG.debug('WMI query failed: %s', sys.exc_info()[1])

        def dmidecode_uuid():
            try:
                ret = linux.system('dmidecode -s system-uuid', shell=True)[0].strip()
                if not ret:
                    LOG.debug('dmidecide returns empty UUID')
                elif len(ret) != 36:
                    LOG.debug("dmidecode returns invalid UUID: %s", ret)
                else:
                    return ret
            except:
                LOG.debug('dmidecode failed: %s', sys.exc_info()[1])

        ret = win32_serial_number() if linux.os.windows else dmidecode_uuid()
        if not ret:
            ret = self.meta['instance_id']
        if not ret:
            raise NoSystemUUID('System UUID not detected')
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
                if 'downgrades_enabled' not in status_data:
                    # Field introduced in 2.7.12 
                    # Missing field here means downgrades_enabled=False, 
                    # cause it's setted by postinst migration to new update system 
                    status_data['downgrades_enabled'] = False
            system_matches = status_data['system_id'] == self.system_id
            if not system_matches:
                LOG.info('System ID in lock file and machine one are not matched: %s != %s', 
                        status_data['system_id'], self.system_id)
            else:
                LOG.debug('Serial number in lock file matches machine one')

        if system_matches:
            LOG.debug('Apply %s settings', self.status_file)
            self._update_self_dict(status_data)

            if self.ps_script_pid:
                def wait_update_script(): 
                    polling_started = False
                    polling_finished = False
                    while not self.shutdown_ev.is_set():
                        if not polling_started:
                            polling_started = True
                            LOG.info("Start polling update.ps1 (pid: %s)", self.ps_script_pid)
                        try:
                            proc = get_win_process(self.ps_script_pid)
                        except LookupError:
                            polling_finished = True
                        else:
                            if not proc.name.startswith('powershell'):
                                polling_finished = True
                            else:
                                self.shutdown_ev.wait(1)
                                continue
                        if polling_finished:
                            LOG.info('update.ps1 (pid: %s) finished', self.ps_script_pid)
                            if os.path.exists(self.win_status_file):
                                with open(self.win_status_file) as fp:
                                    LOG.debug('Apply %s settings', self.win_status_file)
                                    self._update_self_dict(json.load(fp))
                                os.unlink(self.win_status_file)   
                            if self.error:
                                LOG.info('Update error: %s', self.error)
                            if self.state.startswith('in-progress'):
                                if self.ps_attempt < 3:
                                    LOG.warn('Update was interrupted in {0!r}, scheduling it again'.format(self.state))
                                    self.state = 'noop'
                                    return True
                                else:
                                    LOG.warn(('Update was interrupted in {0!r}'
                                            ' and it was already executed {1} times, '
                                            'skip updating this time').format(self.state, self.ps_attempt))
                            return
                try:
                    system_matches = not wait_update_script()
                except:
                    LOG.warn('Caught from wait_update_script', exc_info=sys.exc_info())
                if self.shutdown_ev.is_set():
                    return
        if not system_matches:
            LOG.info('Getting cloud user-data')
            try:
                user_data = self.meta['user_data']
            except metadata.NoUserDataError:
                if 'NoData' in str(self.meta.provider_for_capability['instance_id']):  
                    retry_int = 5
                    num_attempts = 10
                    LOG.info('Found no user-data and no instance-id, '
                            'this mean that all data providers failed. I should '
                            'wait {0} seconds and retry'.format(retry_int))
                    for attempt in range(0, num_attempts):
                        time.sleep(retry_int)
                        self.meta = metadata.Metadata()
                        try:
                            user_data = self.meta['user_data']
                            break
                        except metadata.NoUserDataError:
                            if attempt == num_attempts - 1:
                                LOG.error(('Still no user-data, '
                                        'check why $ETC_DIR/.scalr-user-data not exists. '))
                                raise
                            else:
                                LOG.debug(('Still no user-data, '
                                        'retrying after {0} seconds...').format(retry_int))
                else:
                    raise
            norm_user_data(user_data)
            LOG.info('Applying user-data settings')
            self._update_self_dict(user_data)

            crypto_dir = os.path.dirname(self.crypto_file)
            if not os.path.exists(crypto_dir):
                os.makedirs(crypto_dir)
            if os.path.exists(self.crypto_file): 
                LOG.info('Testing that crypto key works (file: %s)', self.crypto_file) 
                try:
                    self._init_queryenv()
                    LOG.info('Crypto key works')
                except queryenv.InvalidSignatureError:
                    LOG.info("Crypto key doesn't work: got invalid signature error")
                    self.queryenv = None
            if not self.queryenv:
                LOG.info("Use crypto key from user-data")
                if os.path.exists(self.crypto_file):
                    os.chmod(self.crypto_file, 0600)
                with open(self.crypto_file, 'w+') as fp:
                    fp.write(user_data['szr_key'])
                os.chmod(self.crypto_file, 0400)
        self.early_bootstrapped = True

        self._init_services()
        # - my uptime is 644 days, 20 hours and 13 mins and i know nothing about 'platform' in user-data
        if not self.platform: 
            self.platform = bus.cnf.rawini.get('general', 'platform')
        # - my uptime is 1086 days, 55 mins and i know nothing about 'farm_roleid' in user-data
        if not self.farm_role_id:
            self.farm_role_id = bus.cnf.rawini.get('general', 'farm_role_id')
        if not linux.os.windows:
            self.package = 'scalarizr-' + self.platform

        self.system_matches = system_matches
        if not self.system_matches:
            if dry_run:
                self._sync()  
                self._ensure_repos(updatedb=False)
            else:
                self.update(bootstrap=True)
        else:
            #if self.state in ('completed/wait-ack', 'noop'):
            if self.state not in ('error', 'rollbacked'):
                # forcefully finish any in-progress operations
                self.state = 'completed'
            self.store()
        if not (self.shutdown_ev.is_set() or dry_run or \
                self.state == 'error' or self.daemon.running):
            self.daemon.start()
        if self.state == 'completed/wait-ack':
            obsoletes = pkg_resources.Requirement.parse('A<=2.7.5')
            if self.installed in obsoletes:
                def restart_self():
                    time.sleep(5)
                    name = 'ScalrUpdClient' if linux.os.windows else 'scalr-upd-client'
                    service = initdv2.Daemon(name)
                    service.restart()
                proc = multiprocessing.Process(target=restart_self)
                proc.start()


    def uninstall(self):
        pid = None
        if not linux.os.windows:
            # Prevent scalr-upd-client restart when updating from old versions 
            # package 'scalr-upd-client' replaced with 'scalarizr'
            pid_file = '/var/run/scalr-upd-client.pid'
            if os.path.exists(pid_file):
                with open(pid_file) as fp:
                    pid = fp.read().strip()
                with open(pid_file, 'w') as fp:
                    fp.write('0')
        try:
            self.pkgmgr.removed(self.package)
            if not linux.os.windows:
                self.pkgmgr.removed('scalarizr-base', purge=True)
                if self.pkgmgr.info('scalr-upd-client')['installed']:
                    # Only latest package don't stop scalr-upd-client in postrm script
                    self.pkgmgr.latest('scalr-upd-client')
                    self.pkgmgr.removed('scalr-upd-client', purge=True)
            if linux.os.debian_family:
                self.pkgmgr.apt_get_command('autoremove') 
        finally:
            if pid:
                with open(pid_file, 'w+') as fp:
                    fp.write(pid)    
        

    def _ensure_repos(self, updatedb=True):
        if 'release-latest' in self.repo_url or 'release-stable' in self.repo_url:
            LOG.warn("Special branches release/latest and release/stable currently doesn't work") 
            self.repo_url = devel_repo_url_for_branch('master')
        repo = pkgmgr.repository('scalr-{0}'.format(self.repository), self.repo_url)
        # Delete previous repository 
        for filename in glob.glob(os.path.dirname(repo.filename) + os.path.sep + 'scalr-*'):
            if os.path.isfile(filename):
                os.remove(filename)
        if 'buildbot.scalr-labs.com' in self.repo_url and not linux.os.windows:
            self._configure_devel_repo(repo)
        elif linux.os.debian_family:
            self._apt_pin_release('scalr')  # make downgrades possible
        # Ensure new repository
        repo.ensure()
        if updatedb:
            LOG.info('Updating packages cache')
            self.pkgmgr.updatedb() 


    def _configure_devel_repo(self, repo):
        # Pin repository
        if linux.os.redhat_family or linux.os.oracle_family:
            #pkg = 'yum-priorities' \
            #        if linux.os['release'] < (6, 0) else \
            #        'yum-plugin-priorities'
            #self.pkgmgr.installed(pkg)
            repo.config += 'priority=10\n'
        else:
            self._apt_pin_release(self.repository)

        # Scalr repo has all required dependencies (like python-* libs, etc), 
        # while Branch repository has only scalarizr package
        release_repo = pkgmgr.repository('scalr-release', devel_repo_url_for_branch('scalr'))
        release_repo.ensure()


    def _apt_pin_release(self, release):
        if os.path.isdir('/etc/apt/preferences.d'):
            prefile = '/etc/apt/preferences.d/scalr'
        else:
            prefile = '/etc/apt/preferences'
        with open(prefile, 'w+') as fp:
            fp.write((
                'Package: scalarizr-*\n'
                'Pin: release a={0}\n'
                'Pin-Priority: 1001\n'
            ).format(release))        


    def _ensure_daemon(self):
        if not self.daemon.running:
            self.daemon.start()


    def _sync(self):
        params = self.queryenv.list_farm_role_params(self.farm_role_id)
        update = params.get('params', {}).get('base', {}).get('update', {})
        self._update_self_dict(update)
        self.repo_url = value_for_repository(
            deb=update.get('deb_repo_url'),
            rpm=update.get('rpm_repo_url'),
            win=update.get('win_repo_url')
            ) or self.repo_url

        globs = self.queryenv.get_global_config()['params']
        self.scalr_id = globs['scalr.id']
        self.scalr_version = globs['scalr.version']


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

                if self.daemon.running and self.scalarizr.operation.has_in_progress():
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

        def update_windows(pkginfo):
            package_url = self.pkgmgr.index[self.package]
            if os.path.exists(self.win_status_file):
                os.unlink(self.win_status_file)

            LOG.info('Invoke powershell script "update.ps1 -URL %s"', package_url)
            proc = subprocess.Popen([
                    'powershell.exe', 
                    '-NoProfile', 
                    '-NonInteractive', 
                    '-ExecutionPolicy', 'RemoteSigned', 
                    '-File', os.path.join(os.path.dirname(__file__), 'update.ps1'),
                    '-URL', package_url
                ], 
                env=os.environ, 
                close_fds=True,
                cwd='C:\\'
            )
            self.ps_script_pid = proc.pid
            self.ps_attempt += 1
            LOG.debug('Started powershell process (pid: %s)', proc.pid)
            LOG.debug('Waiting for interruption (Timeout: %s)', self.win_update_timeout)
            self.shutdown_ev.wait(self.win_update_timeout)
            if self.shutdown_ev.is_set():
                LOG.debug('Interrupting...')
                return
            else:
                msg = ('UpdateClient expected to be terminated by update.ps1, '
                        'but never happened')
                raise UpdateError(msg)

        def update_linux(pkginfo):
            try:
                self.pkgmgr.install(
                    self.package, self.candidate, 
                    backup=True,
                    rpm_raise_scriptlet_errors=True)
                self._ensure_daemon()
            except:
                if pkginfo['backup_id']:
                    # TODO: remove stacktrace
                    LOG.warn('Install failed, rollbacking. Error: %s', sys.exc_info()[1], exc_info=sys.exc_info())
                    self.state = 'in-progress/rollback'
                    self.error = str(sys.exc_info()[1])
                    self.pkgmgr.restore_backup(self.package, pkginfo['backup_id'])
                    self._ensure_daemon()
                    self.state = 'rollbacked'
                    LOG.info('Rollbacked')
                    if reports:
                        self.report(False)
                else:
                    raise
            else:
                self.state = 'completed/wait-ack'
                self.installed = self.candidate
                self.candidate = None
                if reports:
                    self.report(True)
            return self.status(cached=True)

        def do_update(op):
            self.executed_at = time.strftime(DATE_FORMAT, time.gmtime())
            self.state = 'in-progress/prepare'
            self.error = ''
            self._sync()
            self._ensure_repos()

            pkgmgr.LOG.addHandler(op.logger.handlers[0])
            try:
                pkginfo = self.pkgmgr.info(self.package)
                if not pkginfo['candidate']:
                    self.state = 'completed'
                    LOG.info('No new version available ({0})'.format(self.package))
                    return 
                if self.pkgmgr.version_cmp(pkginfo['candidate'], pkginfo['installed']) == -1 \
                        and not self.downgrades_enabled:
                    self.state = 'completed'
                    LOG.info('New version {0!r} less then installed {1!r}, but downgrades disabled'.format(
                                pkginfo['candidate'], pkginfo['installed']))
                    return
                self._update_self_dict(pkginfo)

                if bootstrap and not linux.os.windows:
                    self.state = 'in-progress/uninstall'
                    self.uninstall()
                    self.installed = None

                check_allowed()
                try:
                    self.state = 'in-progress/install'
                    self.store()
                    LOG.info('Installing {0}={1}'.format(
                            self.package, pkginfo['candidate']))
                    if linux.os.windows:
                        update_windows(pkginfo)
                    else:
                        return update_linux(pkginfo)

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
                if not self.shutdown_ev.is_set():
                    self.store()
                pkgmgr.LOG.removeHandler(op.logger.handlers[0])

        return self.op_api.run('scalarizr.update', do_update, async=async, 
                    exclusive=True, notifies=notifies)


    def shutdown(self):
        if self.early_bootstrapped:
            self.store()
        self.shutdown_ev.set()


    def store(self, status=None):
        status = status or self.status(cached=True)
        coreutils.mkdir(os.path.dirname(self.status_file), 0700)
        with open(self.status_file, 'w+') as fp:
            LOG.debug('Saving status: %s', pprint.pformat(status))
            json.dump(status, fp)     


    def report(self, ok):
        if not self.is_client_mode:
            LOG.debug('Reporting is not enabled in {0} mode'.format(self.client_mode))
            return
        error = str(sys.exc_info()[1]) if not ok else ''                
        self.update_server.report(
                ok=ok, package=self.package, version=self.candidate or self.installed, 
                server_id=self.server_id, scalr_id=self.scalr_id, scalr_version=self.scalr_version, 
                phase=self.state, dist=self.dist, error=error)   


    @rpc.command_method
    def restart(self, force=False):
        getattr(self.daemon, 'forcerestart' if force else 'restart')()
        if not self.daemon.running:
            raise Exception('Service restart failed')


    @rpc.query_method
    def status(self, cached=False):
        status = {}
        keys_to_copy = [
            'server_id', 'farm_role_id', 'system_id', 'platform', 'queryenv_url', 
            'messaging_url', 'scalr_id', 'scalr_version', 
            'repository', 'repo_url', 'package', 'downgrades_enabled', 'executed_at', 
            'ps_script_pid', 'ps_attempt',
            'state', 'prev_state', 'error', 'dist'
        ]

        pkginfo_keys = ['candidate', 'installed']
        if cached:
            keys_to_copy.extend(pkginfo_keys)
        else:
            self._sync()
            self._ensure_repos(False)
            self.pkgmgr.updatedb(apt_repository='scalr-{0}'.format(self.repository))
            pkginfo = self.pkgmgr.info(self.package)
            status.update((key, pkginfo[key]) for key in pkginfo_keys)

        for key in keys_to_copy:
            status[key] = getattr(self, key)

        # we should exclude status from realtime data, 
        # cause postinst for < 2.7.7 calls --make-status-file that fails to call scalarizr status
        #
        # \_ /bin/bash /etc/rc3.d/S84scalarizr_update start
        #     \_ /usr/bin/python2.6 -c ?from upd.client.package_mgr import YumPackageMgr?mgr = YumPackageMgr()?try:??mgr.u
        #         \_ /usr/bin/python /usr/bin/yum -d0 -y --disableplugin=priorities install scalarizr-base-2.7.28-1.el6 sc
        #             \_ /bin/sh /var/tmp/rpm-tmp.OXe7Fi 2
        #                 \_ /usr/bin/python2.6 -m scalarizr.updclient.app --make-status-file --downgrades-disabled
        #                     \_ /usr/bin/python2.6 /usr/bin/scalarizr status
        #                         \_ /usr/bin/python2.6 /usr/bin/scalr-upd-client status
        #                             \_ /usr/bin/python /usr/bin/yum -d0 -y clean expire-cache --exclude *.i386 --exclude
        if not cached:
            status['service_status'] = 'running' if self.daemon.running else 'stopped'
        else:
            status['service_status'] = 'unknown'
        status['service_version'] = __version__
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

