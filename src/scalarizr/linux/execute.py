__author__ = 'Nick'

import re
import os
import sys
import time
import locale
import logging
import threading
import subprocess

if sys.version_info[0:2] >= (2, 7):
    from collections import OrderedDict
else:
    from scalarizr.externals.collections import OrderedDict

from scalarizr import linux
from scalarizr.linux import pkgmgr
from scalarizr.util import software

class parameter_handler(object):
    def __init__(self, regexp):
        self.regexp = re.compile(regexp)

    def __call__(self, fn):
        fn.regexp = self.regexp
        return fn


class ProcessTimeout(Exception):
    pass


class ProcessError(Exception):
    pass


class BaseExec(object):
    _handlers = dict()
    executable = None
    _checked = False
    package = None


    def __init__(self, lazy_check=True, wait=True, raise_exc=True, timeout=None,
                        acceptable_codes=None, logger=None, to_log=True, **kwds):

        self.lazy_check = lazy_check
        self.wait_for_process = wait
        self.raise_exc=raise_exc
        self.timeout = timeout
        self.acceptable_codes = acceptable_codes or (0,)
        self.logger = logger or logging.getLogger(__name__)
        self.to_log = to_log

        kwds['close_fds'] = True
        self.subprocess_kwds = kwds

        if not lazy_check:
            self.check()
        self._collect_handlers()


    def check(self):
        if not self.executable.startswith('/'):
            exec_paths = software.whereis(self.executable)
            exec_path = exec_paths[0] if exec_paths else None
        else:
            exec_path = self.executable

        if not exec_path or not os.access(exec_path, os.X_OK):
            if self.package:
                pkgmgr.installed(self.package)

            else:
                msg = 'Executable %s is not found, you should either ' \
                      'specify `package` attribute or install the software ' \
                      'manually' % self.executable
                raise linux.LinuxError(msg)


    def _get_stdin(self):
        return self.subprocess_kwds.get('stdin')
    def _set_stdin(self, stdin):
        self.subprocess_kwds['stdin'] = stdin
    stdin = property(_get_stdin, _set_stdin)

    def _get_stdout(self):
        return self.subprocess_kwds.get('stdout')
    def _set_stdout(self, stdout):
        self.subprocess_kwds['stdout'] = stdout
    stdout = property(_get_stdout, _set_stdout)

    def _get_stderr(self):
        return self.subprocess_kwds.get('stderr')
    def _set_stderr(self, stderr):
        self.subprocess_kwds['stderr'] = stderr
    stderr = property(_get_stderr, _set_stderr)


    def _collect_handlers(self):
        # Collects self methods wrapped in handler decorator
        for attr in dir(self):
            attr = getattr(self, attr)
            if callable(attr) and hasattr(attr, 'regexp'):
                self._handlers[attr.regexp] = attr


    def _default_handler(self, key, value, cmd_args):
        # Default parameter handler, very straightforward by default
        if len(key) == 1:
            cmd_args.append('-%s' % key)
        else:
            cmd_args.append('--%s' % key.replace('_', '-'))

        if value is True:
            return
        else:
            cmd_args.append(str(value))


    def _before_all_handlers(self, *params, **keys):
        # By default, return same params and key-value pairs iterator
        return params, keys.iteritems()


    def _after_all_handlers(self, cmd_args):
        # By default, do nothing to final cmd args
        return cmd_args


    def prepare_args(self, *params, **keys):
        cmd_args = []
        # 1st step, before handlers
        params, key_value_pairs = self._before_all_handlers(*params, **keys)

        # 2nd step, trying to find appropriate handler, otherwise using default
        for key, value in key_value_pairs:
            for validator_re, handler in self._handlers.iteritems():
                re_result = validator_re.match(key)
                if re_result:
                    handler(re_result, key, value, cmd_args)
                    break
            else:
                self._default_handler(key, value, cmd_args)

        # 3rd step, after all
        cmd_args.extend(params)
        cmd_args = self._after_all_handlers(cmd_args)
        return cmd_args


    def start(self, *params, **keys):
        try:
            if not self._checked:
                self.check()
            if len(keys) == 1 and 'kwargs' in keys:
                keys = keys['kwargs']
            # Set locale
            if not 'env' in self.subprocess_kwds:
                self.subprocess_kwds['env'] = os.environ
                # Set en_US locale or C
            if not self.subprocess_kwds['env'].get('LANG'):
                default_locale = locale.getdefaultlocale()
                if default_locale == ('en_US', 'UTF-8'):
                    self.subprocess_kwds['env']['LANG'] = 'en_US'
                else:
                    self.subprocess_kwds['env']['LANG'] = 'C'

            cmd_args = self.prepare_args(*params, **keys)

            if not self.subprocess_kwds.get('shell') and not self.executable.startswith('/'):
                # TODO: Raise error if not found
                self.executable = software.whereis(self.executable)[0]

            final_args = (self.executable,) + tuple(cmd_args)
            self._check_streams()
            read_stdout = self.stdout == subprocess.PIPE
            read_stderr = self.stderr == subprocess.PIPE

            self.logger.debug('Executing command: {%s} kwds: %s', ' '.join(final_args), self.subprocess_kwds)
            self.popen = subprocess.Popen(final_args, **self.subprocess_kwds)
            if self.wait_for_process:
                rcode = self.wait(self.popen, self.timeout)
                ret = dict(return_code=rcode)
                if read_stdout:
                    ret['stdout'] = self.popen.stdout.read()
                    self.logger.debug('Stdout: %s' % ret['stdout'])
                if read_stderr:
                    ret['stderr'] = self.popen.stderr.read()
                    self.logger.debug('Stderr: %s' % ret['stderr'])
                if rcode not in self.acceptable_codes and self.raise_exc:
                    raise ProcessError('Process %s finished with code %s' % (self.executable, rcode))
                return ret
            else:
                return self.popen
        finally:
            for stream in ('stderr, stdout, stdin'):
                self.subprocess_kwds.pop(stream, None)


    def wait(self, popen, timeout=None):
        wait_start = time.time()
        while True:
            rcode = popen.poll()
            if rcode is not None:
                return rcode
            else:
                if timeout:
                    now = time.time()
                    if now - wait_start > timeout:
                        raise ProcessTimeout('Process %s reached timeout %s sec' % (self.executable, timeout))


    def _check_streams(self):
        if not 'stdin' in self.subprocess_kwds:
            self.subprocess_kwds['stdin'] = subprocess.PIPE
        if not 'stdout' in self.subprocess_kwds:
            self.subprocess_kwds['stdout'] = subprocess.PIPE
        if not 'stderr' in self.subprocess_kwds:
            self.subprocess_kwds['stderr'] = subprocess.PIPE


#### Realisations ######


class dd_exec(BaseExec):
    executable = '/bin/dd'

    # dd uses -- before parameter only for --version and --help,
    # which we can ignore
    def _default_handler(self, key, value, cmd_args):
        cmd_args.append('%s=%s' % (key, value))

    @parameter_handler('^conv$')
    def conv(self, re_result, key, value, cmd_args):
        if isinstance(value, list) or isinstance(value, tuple):
            value = ','.join(map(str, value))
        cmd_args.append('%s=%s' % (key, value))


class iptables_exec(BaseExec):
    executable = '/sbin/iptables'
    package = 'iptables'

    @parameter_handler('^(not_)?(.+)')
    def params_with_not(self, re_result, key, value, cmd_args):
        if re_result.group(1):
            cmd_args.append('!')
        self._default_handler(re_result.group(2), value, cmd_args)


    def _before_all_handlers(self, *params, **keys):
        # in iptables, protocol and match should preceed other flags
        ordered_keys = OrderedDict()
        for key in ("t", "protocol", "p", "not_protocol", "not_p", "match"):
            if key in keys:
                ordered_keys[key] = keys.pop(key)
        ordered_keys.update(keys)
        return params, ordered_keys.iteritems()


class grep_exec(BaseExec):
    executable = 'grep'


class pigz(BaseExec):
    executable='pigz'
    package='pigz'


class tee(BaseExec):
    executable='tee'


