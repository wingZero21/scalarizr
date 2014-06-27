'''
Created on Dec 24, 2009

@author: marat
'''

import sys
import time
import json
import random
import ConfigParser
import subprocess
import threading
import os
import shutil
import stat
import signal
import logging
import Queue
import binascii

from scalarizr import config as szrconfig
from scalarizr import linux
from scalarizr.bus import bus
from scalarizr.handlers import Handler, HandlerError
from scalarizr.handlers.chef import ChefSolo, ChefClient, extract_json_attributes
from scalarizr.messaging import Queues, Messages
from scalarizr.node import __node__
from scalarizr.util import parse_size, format_size, read_shebang, split_strip, wait_until
from scalarizr.config import ScalarizrState


def get_handlers():
    return [ScriptExecutor()]


def get_truncated_log(logfile, maxsize=None):
    maxsize = maxsize or logs_truncate_over
    f = open(logfile, "r")
    try:
        ret = unicode(f.read(int(maxsize)), 'utf-8', errors='ignore')
        if (os.path.getsize(logfile) > maxsize):
            ret += u"... Truncated. See the full log in " + logfile.encode('utf-8')
        return ret.encode('utf-8')
    finally:
        f.close()


LOG = logging.getLogger(__name__)


skip_events = set()


logs_truncate_over = 20000


if linux.os.windows_family:
    exec_dir_prefix = os.getenv('TEMP') + r'\scalr-scripting'
    logs_dir = os.getenv('PROGRAMFILES') + r'\Scalarizr\var\log\scripting'
else:
    exec_dir_prefix = '/usr/local/bin/scalr-scripting.'
    logs_dir = '/var/log/scalarizr/scripting'


class ScriptExecutor(Handler):

    name = 'script_executor'
    _data = None

    def __init__(self):
        self.queue = Queue.Queue()
        self.in_progress = []
        self.global_variables = None
        bus.on(
                init=self.on_init,
                start=self.on_start,
                shutdown=self.on_shutdown
        )

        # Operations
        self._op_exec_scripts = 'Execute scripts'
        self._step_exec_tpl = "Execute '%s' in %s mode"

        # Services
        self._cnf = bus.cnf
        self._queryenv = bus.queryenv_service
        self._platform = bus.platform

    def on_init(self):
        global exec_dir_prefix, logs_dir, logs_truncate_over

        bus.on(
            host_init_response=self.on_host_init_response,
            before_host_up=self.on_before_host_up
        )

        # Configuration
        cnf = bus.cnf
        ini = cnf.rawini

        # read exec_dir_prefix
        '''
        TODO: completely remove ini options 
        try:
            exec_dir_prefix = ini.get(self.name, 'exec_dir_prefix')
        except ConfigParser.Error:
            pass
        if linux.os['family'] == 'Windows':
            exec_dir_prefix = os.path.expandvars(exec_dir_prefix)
        '''
        if not os.path.isabs(exec_dir_prefix):
            os.path.join(bus.base_path, exec_dir_prefix)

        # read logs_dir
        '''
        try:
            logs_dir = ini.get(self.name, 'logs_dir')
        except ConfigParser.Error:
            pass
        if linux.os['family'] == 'Windows':
            logs_dir = os.path.expandvars(logs_dir)  
        '''  
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)

        # logs_truncate_over
        try:
            logs_truncate_over = parse_size(ini.get(self.name, 'logs_truncate_over'))
        except ConfigParser.Error:
            pass

        self.log_rotate_runnable = LogRotateRunnable()
        self.log_rotate_thread = threading.Thread(name='ScriptingLogRotate',
                                                                target=self.log_rotate_runnable)
        self.log_rotate_thread.setDaemon(True)

    def on_start(self):
        # Start log rotation
        self.log_rotate_thread.start()

        #if linux.os.windows_family:
        #    system2(['C:\\Windows\\sysnative\\WindowsPowerShell\\v1.0\\powershell.exe', 
        #           '-Command', 'Set-ExecutionPolicy RemoteSigned -Scope LocalMachine -Force'])

        # Restore in-progress scripts
        LOG.debug('STATE[script_executor.in_progress]: %s', szrconfig.STATE['script_executor.in_progress'])
        scripts = []
        for kwds in szrconfig.STATE['script_executor.in_progress'] or []:
            script_class = ChefSoloScript if "chef" in kwds else Script
            scripts.append(script_class(**kwds))
        LOG.debug('Restoring %d in-progress scripts', len(scripts))

        for sc in scripts:
            self._execute_one_script(sc)

        if __node__['state'] == 'running':
            params = self._queryenv.list_farm_role_params(__node__['farm_role_id']).get('params', {})
            keep_scripting_logs_time = int(params.get('base', {}).get('keep_scripting_logs_time', 86400))
            self.log_rotate_runnable.keep_scripting_logs_time = keep_scripting_logs_time

    def on_shutdown(self):
        # save state
        LOG.debug('Saving Work In Progress (%d items)', len(self.in_progress))
        szrconfig.STATE['script_executor.in_progress'] = [sc.state() for sc in self.in_progress]

    def on_host_init_response(self, hir_message):
        self._data = hir_message.body.get('base', {})
        self._data = self._data or {}
        if 'keep_scripting_logs_time' in self._data:
            self.log_rotate_runnable.keep_scripting_logs_time = int(self._data.get('keep_scripting_logs_time', 86400))

    def on_before_host_up(self, hostup):
        if not 'base' in hostup.body:
            hostup.base = {}
        hostup.base['keep_scripting_logs_time'] = self.log_rotate_runnable.keep_scripting_logs_time


    def _execute_one_script(self, script):
        if script.asynchronous:
            threading.Thread(target=self._execute_one_script0,
                                            args=(script, )).start()
        else:
            self._execute_one_script0(script)

    def _execute_one_script0(self, script):
        exc_info = None
        try:
            self.in_progress.append(script)
            if not script.start_time:
                script.start()
            script.wait()

        except:
            exc_info = sys.exc_info()
            if script.asynchronous:
                msg = 'Asynchronous script {0!r} error: {1}'.format(
                        script.name, str(exc_info[1]))
                LOG.warn(msg, exc_info=exc_info)
            raise
        finally:
            script_result = script.get_result()
            if exc_info:
                script_result['stderr'] = binascii.b2a_base64(exc_info[1][1])
                script_result['return_code'] = 1
            self.send_message(Messages.EXEC_SCRIPT_RESULT, script_result, queue=Queues.LOG)
            self.in_progress.remove(script)

    def execute_scripts(self, scripts, event_name, scripts_qty):
        """
        Scripts will be executing in order given by script generator.
        Construction-time script errors (such as missing interpreter) will stop
        all execution.
        Runtime error that were caused by synchronous scripts will stop all
        execution. Asynchronous will not.
        """
        if not scripts:
            return
        # read logs_dir_prefix
        ini = bus.cnf.rawini
        try:
            logs_dir = ini.get(self.name, 'logs_dir')
            if not os.path.exists(logs_dir):
                os.makedirs(logs_dir)
        except ConfigParser.Error:
            pass

        if event_name:
            msg = "Executing %d %s script(s)" % (scripts_qty, event_name)
        else:
            msg = 'Executing %d script(s)' % (scripts_qty, )
        self._logger.info(msg)

        for script in scripts:
            msg = "Execute '%s' in %s mode" % (script.name, 'async' if script.asynchronous else 'sync')
            self._execute_one_script(script)

    def accept(self, message, queue, **kwds):
        return not message.name in skip_events

    def __call__(self, message):
        event_name = message.event_name if message.name == Messages.EXEC_SCRIPT else message.name
        role_name = message.body.get('role_name', 'unknown_role')
        LOG.debug("Scalr notified me that '%s' fired", event_name)

        if self._cnf.state == ScalarizrState.IMPORTING:
            LOG.debug('Scripting is OFF when state: %s', ScalarizrState.IMPORTING)
            return

        scripts = []
        scripts_qty = 0

        if 'scripts' in message.body:
            if not message.body['scripts']:
                self._logger.debug('Empty scripts list. Breaking')
                return

            environ = os.environ.copy()

            global_variables = message.body.get('global_variables') or []
            global_variables = dict((kv['name'], kv['value'].encode('utf-8') if kv['value'] else '') for kv in global_variables)
            if linux.os.windows_family:
                global_variables = dict((k.encode('ascii'), v.encode('ascii')) for k, v in global_variables.items())
            environ.update(global_variables)

            LOG.debug('Fetching scripts from incoming message')

            def _create_script(message_script_params):
                kwds = message_script_params.copy()

                if 'chef' in kwds:
                    if 'asynchronous' in kwds:
                        assert not int(kwds['asynchronous']), 'Chef script could only be executed in synchronous mode'
                    script_class = ChefSoloScript if 'cookbook_url' in kwds['chef'] else ChefClientScript
                else:
                    script_class = Script

                if 'timeout' in kwds:
                    kwds['exec_timeout'] = kwds.pop('timeout')
                if 'asynchronous' in kwds:
                    kwds['asynchronous'] = int(kwds['asynchronous'])
                kwds['role_name'] = role_name
                kwds['event_server_id'] = message.body.get('server_id')
                kwds['event_id'] = message.body.get('event_id')
                kwds['event_name'] = event_name
                kwds['environ'] = environ
                try:
                    return script_class(**kwds)
                except (BaseException, Exception), e:
                    message_body = {
                            'stdout': '',
                            'stderr': e.message,
                            'return_code': 1,
                            'time_elapsed': 0,
                            'event_name': event_name,
                            'event_id': kwds.get('event_id'),
                            'execution_id': kwds.get('execution_id'),
                            'run_as': kwds.get('run_as')
                        }
                    if script_class is ChefSoloScript:
                        message_body.update({'cookbook_url': kwds.get('cookbook_url')})
                    else:
                        message_body.update({'script_name': kwds.get('name'), 'script_path': kwds.get('path')})
                    self.send_message(Messages.EXEC_SCRIPT_RESULT, message_body, queue=Queues.LOG)
                    raise

            scripts_qty = len(message.body['scripts'])
            scripts = (_create_script(item) for item in message.body['scripts'])
        else:
            LOG.debug("No scripts embed into message '%s'", message.name)
            return

        LOG.debug('Fetched %d scripts', scripts_qty)
        self.execute_scripts(scripts, event_name, scripts_qty)


class Script(object):
    name = None
    body = None
    run_as = None
    path = None
    asynchronous = None
    event_name = None
    role_name = None
    exec_timeout = 0
    event_server_id = None
    event_id = None

    id = None
    pid = None
    return_code = None
    interpreter = None
    start_time = None
    exec_path = None
    environ = None

    logger = None
    proc = None
    stdout_path = None
    stderr_path = None
    execution_id = None

    def __init__(self, **kwds):
        '''
        Variant A:
        Script(name='AppPreStart', body='#!/usr/bin/python ...', asynchronous=True)

        Variant B:
        Script(id=43432234343, name='AppPreStart', pid=12145,
                        interpreter='/usr/bin/python', start_time=4342424324, asynchronous=True)
        '''
        for key, value in kwds.items():
            setattr(self, key, value)
        self.elapsed_time = 0
        self.return_code = 1
        assert self.name, '`name` required'
        assert self.exec_timeout, '`exec_timeout` required'

        if self.body or self.path:
            # time.time() can produce the same microseconds fraction in different async script execution threads, 
            # and therefore produce the same id. solution is to seed random millisecods number
            random.seed()
            self.id = '%d.%d' % (time.time(), random.randint(0, 100))
        else:
            assert self.id, '`id` required'
            assert self.pid, '`pid` required'
            assert self.start_time, '`start_time` required'
            if self.interpreter:
                self.interpreter = split_strip(self.interpreter)[0]
                
        self.logger = logging.getLogger('%s.%s' % (__name__, self.id))
        self.exec_path = self.path or os.path.join(exec_dir_prefix + self.id, self.name)

        if self.exec_timeout:
            self.exec_timeout = int(self.exec_timeout)

        if self.execution_id:
            args = (self.name, self.event_name, self.execution_id)
            self.stdout_path = os.path.join(logs_dir, '%s.%s.%s-out.log' % args)
            self.stderr_path = os.path.join(logs_dir, '%s.%s.%s-err.log' % args)
        else:
            args = (self.name, self.event_name, self.role_name, self.id)
            self.stdout_path = os.path.join(logs_dir, '%s.%s.%s.%s-out.log' % args)
            self.stderr_path = os.path.join(logs_dir, '%s.%s.%s.%s-err.log' % args)

    def check_runability(self):
        if self.body or self.path:
            self.interpreter = read_shebang(path=self.path, script=self.body)
            if linux.os['family'] == 'Windows' and self.body:
                # Erase first line with #!
                self.body = '\n'.join(self.body.splitlines()[1:])

        if self.interpreter == 'powershell' \
                and os.path.splitext(self.exec_path)[1] not in ('ps1', 'psm1'):
            self.exec_path += '.ps1'
        elif self.interpreter == 'cmd' \
                and os.path.splitext(self.exec_path)[1] not in ('cmd', 'bat'):
            self.exec_path += '.bat'

        if self.path and not os.access(self.path, os.X_OK):
            msg = 'Path {0!r} is not executable'.format(self.path)
            raise HandlerError(msg)
        if linux.os['family'] == 'Windows' and self.run_as:
            raise HandlerError("Windows can't execute scripts remotely " \
                               "under user other than Administrator. " \
                               "Script '%s', given user: '%s'" % (self.name, self.run_as))
        if not self.interpreter:
            raise HandlerError("Can't execute script '%s' cause it hasn't shebang.\n"
                "First line of the script should have the form of a shebang "
                "interpreter directive is as follows:\n"
                "#!interpreter [optional-arg]" % (self.name, ))
        if not os.path.exists(self.interpreter) and linux.os['family'] != 'Windows':
            raise HandlerError("Can't execute script '%s' cause "
                "interpreter '%s' not found" % (self.name, self.interpreter))

    def start(self):
        if not os.path.exists(self.stdout_path):
            open(self.stdout_path, 'w+').close()
        if not os.path.exists(self.stderr_path):
            open(self.stderr_path, 'w+').close()
        self.check_runability()
        if not self.path:
            # Write script to disk, prepare execution
            exec_dir = os.path.dirname(self.exec_path)
            if not os.path.exists(exec_dir):
                os.makedirs(exec_dir)

            with open(self.exec_path, 'w') as fp:
                fp.write(self.body.encode('utf-8'))
            if not linux.os.windows_family:
                os.chmod(self.exec_path,
                         stat.S_IREAD |
                         stat.S_IRGRP |
                         stat.S_IROTH |
                         stat.S_IEXEC |
                         stat.S_IXGRP |
                         stat.S_IXOTH)

        stdout = open(self.stdout_path, 'w+')
        stderr = open(self.stderr_path, 'w+')
        if self.interpreter == 'powershell':
            command = ['powershell.exe', 
                        '-NoProfile', '-NonInteractive', 
                        '-ExecutionPolicy', 'RemoteSigned', 
                         '-File', self.exec_path]
        elif self.interpreter == 'cmd':
            command = ['cmd.exe', '/C', self.exec_path]
        else:
            command = []
            if self.run_as and self.run_as != 'root':
                command = ['sudo', '-u', self.run_as]
            command += [self.exec_path]

        # Start process
        self.logger.debug('Executing %s'
                        '\n  %s'
                        '\n  1>%s'
                        '\n  2>%s'
                        '\n  timeout: %s seconds',
                        self.interpreter, self.exec_path, self.stdout_path,
                        self.stderr_path, self.exec_timeout)
        self.proc = subprocess.Popen(command, 
                        stdout=stdout, stderr=stderr, 
                        close_fds=linux.os['family'] != 'Windows',
                        env=self.environ)
        self.pid = self.proc.pid
        self.start_time = time.time()

    def wait(self):
        try:
            # Communicate with process
            self.logger.debug('Communicating with %s (pid: %s)', self.interpreter, self.pid)
            while time.time() - self.start_time < self.exec_timeout:
                if self._proc_poll() is None:
                    time.sleep(0.5)
                else:
                    # Process terminated
                    self.logger.debug('Process terminated')
                    self.return_code = self._proc_complete()
                    break
            else:
                # Process timeouted
                self.logger.debug('Timeouted: %s seconds. Killing process %s (pid: %s)',
                                                        self.exec_timeout, self.interpreter, self.pid)
                self.return_code = self._proc_kill()

            if not os.path.exists(self.stdout_path):
                open(self.stdout_path, 'w+').close()
            if not os.path.exists(self.stderr_path):
                open(self.stderr_path, 'w+').close()

            self.elapsed_time = time.time() - self.start_time
            self.logger.debug('Finished %s'
                            '\n  %s'
                            '\n  1: %s'
                            '\n  2: %s'
                            '\n  return code: %s'
                            '\n  elapsed time: %s',
                            self.interpreter, self.exec_path,
                            format_size(os.path.getsize(self.stdout_path)),
                            format_size(os.path.getsize(self.stderr_path)),
                            self.return_code,
                            self.elapsed_time)

        except:
            if threading.currentThread().name != 'MainThread':
                self.logger.exception('Exception in script execution routine')
            else:
                raise

        finally:
            if not self.path:
                f = os.path.dirname(self.exec_path)
                if os.path.exists(f):
                    shutil.rmtree(f)

    def get_result(self):
        stdout = binascii.b2a_base64(get_truncated_log(self.stdout_path))
        stderr = binascii.b2a_base64(get_truncated_log(self.stderr_path))
        ret = dict(
            stdout=stdout,
            stderr=stderr,
            execution_id=self.execution_id,
            time_elapsed=self.elapsed_time,
            script_name=self.name,
            script_path=self.exec_path,
            event_name=self.event_name or '',
            return_code=self.return_code,
            event_server_id=self.event_server_id,
            event_id=self.event_id,
            run_as=self.run_as
        )
        return ret

    def state(self):
        return {'id': self.id,
                'pid': self.pid,
                'name': self.name,
                'interpreter': self.interpreter,
                'start_time': self.start_time,
                'asynchronous': self.asynchronous,
                'event_name': self.event_name,
                'role_name': self.role_name,
                'exec_timeout': self.exec_timeout,
                'run_as': self.run_as}

    def _proc_poll(self):
        if self.proc:
            return self.proc.poll()
        else:
            statfile = '/proc/%s/stat' % self.pid
            exefile = '/proc/%s/exe' % self.pid
            if os.path.exists(exefile) and os.readlink(exefile) == self.interpreter:
                stat = open(statfile).read().strip().split(' ')
                if stat[2] not in ('Z', 'D'):
                    return None

            return 0

    def _proc_kill(self):
        self.logger.debug('Timeouted: %s seconds. Killing process %s (pid: %s)',
                                                self.exec_timeout, self.interpreter, self.pid)
        if self.proc and self._proc_poll() is None:
            os.kill(self.pid, signal.SIGTERM)
            if not wait_until(lambda: self._proc_poll() is not None,
                            timeout=2, sleep=.5, raise_exc=False):
                os.kill(self.pid, signal.SIGKILL)
                return -9
            return self.proc.returncode

    def _proc_complete(self):
        if self.proc:
            self._proc_finalize()
            return self.proc.returncode
        else:
            return 0

    def _proc_finalize(self):
        if self.proc.stdout:
            try:
                self.proc.stdout.flush()
                os.fsync(self.proc.stdout.fileno())
            except:
                pass
        if self.proc.stderr:
            try:
                self.proc.stderr.flush()
                os.fsync(self.proc.stderr.fileno())
            except:
                pass


class BaseChefScript(Script):

    def start(self):
        self.chef.prepare()
        super(BaseChefScript, self).start()

    def state(self):
        return {'id': self.id,
                'name': self.name,
                'pid': self.pid,
                'chef': self.chef_params,
                'start_time': self.start_time,
                'asynchronous': False,
                'event_name': self.event_name,
                'role_name': self.role_name,
                'exec_timeout': self.exec_timeout,
                'run_as': self.run_as}


    def _get_shebang(self):
        shebang = "#!%s" % ("cmd" if linux.os.windows_family else "/bin/bash")
        return shebang + "\n" + " ".join(self.chef.get_cmd())


    def wait(self):
        try:
            super(BaseChefScript, self).wait()
        finally:
            self.chef.cleanup()


class ChefClientScript(BaseChefScript):

    def __init__(self, **kwds):
        self.chef_params = kwds.pop('chef')
        self.with_json_attributes = extract_json_attributes(self.chef_params)

        self.chef = ChefClient(self.chef_params['server_url'],
                                      self.with_json_attributes,
                                      self.chef_params.get('node_name'),
                                      self.chef_params.get('validator_name'),
                                      self.chef_params.get('validator_key'),
                                      self.chef_params.get('environment'),
                                      kwds.get("environ"))

        self.body = self._get_shebang()
        super(ChefClientScript, self).__init__(**kwds)


    def state(self):
        state = super(ChefClientScript, self).state()
        return state


class ChefSoloScript(BaseChefScript):

    json_attributes = None
    relative_path = None
    ssh_private_key = None
    run_list = None
    role = None
    chef = None
    pid = None


    def __init__(self, **kwds):
        self.name = kwds.get("name") or "chef-solo-script.%s" % time.time()
        self.chef_params = kwds.pop("chef")
        self.with_json_attributes = extract_json_attributes(self.chef_params)

        self.chef = ChefSolo(self.chef_params.get("cookbook_url"),
                                   self.chef_params.get("cookbook_url_type"),
                                   self.with_json_attributes,
                                   self.chef_params.get("relative_path"),
                                   kwds.get("environ"),
                                   self.chef_params.get("ssh_private_key"),
                                   run_as=kwds.get("run_as"),
                                   temp_dir=kwds.get("temp_dir"))

        self.chef_temp_dir = self.chef.temp_dir
        self.body = self._get_shebang()
        super(ChefSoloScript, self).__init__(**kwds)

    def state(self):
        state = super(ChefSoloScript, self).state()
        state['temp_dir'] = self.chef_temp_dir
        return state


class LogRotateRunnable(object):
    keep_scripting_logs_time = 86400  # 1 day

    def __call__(self):
        while True:
            LOG.debug('Starting log_rotate routine')
            now = time.time()
            for name in os.listdir(logs_dir):
                filename = os.path.join(logs_dir, name)
                if os.stat(filename).st_ctime + self.keep_scripting_logs_time < now:
                    LOG.debug('Delete %s', filename)
                    os.remove(filename)
            time.sleep(3600)
