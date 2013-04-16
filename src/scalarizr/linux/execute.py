__author__ = 'Nick'

import re
import os
import sys
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


class BaseExec(object):
    _handlers = dict()
    executable = None
    _checked = False
    package = None

    def __init__(self, lazy_check=True, **kwds):
        kwds['close_fds'] = True
        self.subprocess_kwds = kwds
        self.lazy_check = lazy_check
        if not lazy_check:
            self.check()
        self._collect_handlers()


    def check(self):
        if not self.executable.startswith('/'):
            exec_path = software.whereis(self.executable)
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


    def _prepare_args(self, *params, **keys):
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
            cmd_args = self._prepare_args(*params, **keys)
            if not self.subprocess_kwds.get('shell') and not self.executable.startswith('/'):
                self.executable = software.whereis(self.executable)
            final_args = (self.executable,) + tuple(cmd_args)
            self._check_streams()
            return subprocess.Popen(final_args, **self.subprocess_kwds)
        finally:
            for stream in ('stderr, stdout, stdin'):
                self.subprocess_kwds.pop(stream, None)


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