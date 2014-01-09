#!/usr/bin/python
import sys
import os
import imp
import time
import resource
import inspect
from textwrap import dedent

from scalarizr.adm import command as command_module
from scalarizr.app import init_script
from scalarizr.adm.commands.queryenv import Queryenv as QueryenvCmd


__dir__ = os.path.dirname(__file__)


def find_modules(directory):
    """Method returns iterator over modules in given directory"""
    directory = os.path.abspath(directory)
    result = []

    dir_content = os.listdir(directory)
    module_names = [m.replace('.py', '') for m in dir_content]
    module_names = list(set(module_names))
    this_module = os.path.basename(__file__).replace('.py', '')

    for name in module_names:
        if name in ('__init__', this_module):
            continue
        try:
            module_info = imp.find_module(name, [directory])
        except ImportError:
            continue
        module = imp.load_module(name, *module_info)
        if module:
            yield module


class Szradm(command_module.Command):
    """
    Szradm is scalarizr administration tool.

    Usage:
      szradm [options] [<command>] [<args>...]

    Options:
      -v, --version                Show version.
      -h, --help                   show this help message and exit
      -q, --queryenv               QueryEnv CLI
      --api-version=API_VERSION    QueryEnv API version
      -m, --msgsnd                 Message sender CLI
      -n <name>, --name=<name>         
      -f <msgfile>, --msgfile=<msgfile>
      -e <endpoint>, --endpoint=<endpoint>
      -o <queue>, --queue=<queue>
      --fire-event=<event_name>    Fire custom event in Scalr. Parameters are passed in a
                                   key=value form
    """

    version = (0, 2)

    def help(self):
        """
        Redefining this method because we don't need to print subcommands list
        for szradm in here.
        """
        return dedent(self.__doc__)

    def __init__(self, commands_dir=None):
        super(Szradm, self).__init__()
        self.subcommands = self.find_commands(commands_dir)

    def __call__(self, 
                 command=None,
                 version=False,
                 help=False,
                 queryenv=False,
                 msgsnd=False,
                 qa_report=False,
                 repair=False,
                 name=None,
                 msgfile=None,
                 queue=None,
                 api_version=None,
                 fire_event=None,
                 endpoint=None,
                 args=[]):

        if version:
            print 'Szradm version: %s.%s' % self.version
            return

        if help:
            self.subcommands = list(self.subcommands)
            print self.help() + self._command_help()
            return

        try:
            # old-style command execution for backward compatibility
            if queryenv:
                run_args = ['fetch']
                kwds = {}
                for pair in args:
                    k, v = pair.split('=')
                    kwds[k] = v
                kwds['command'] = command
                
                return self.run_subcommand('queryenv', run_args, kwds, options_first=True)

            if msgsnd:
                kwds = {'name': name,
                        'msgfile': msgfile,
                        'endpoint': endpoint,
                        'queue': queue}
                return self.run_subcommand('msgsnd', [], kwds)

            if fire_event:
                return self.run_subcommand('fire-event', [], {'name': fire_event})  # TODO:

            if not command:
                return self(help=True)

            if command in QueryenvCmd.supported_methods():
                return self.run_subcommand('queryenv', [command] + args)

            # Standard command execution style
            return self.run_subcommand(command, args, options_first=True)

        except (command_module.UnknownCommand, command_module.InvalidCall), e:
            call_str = 'szradm %s %s' % (command, ' '.join(args))
            message = '\n'.join((call_str, e.message, e.usage))
            raise e.__class__(message)

        except command_module.RuntimeError, e:
            # except-section for user-defined exceptions, semantic errors, etc.
            call_str = 'szradm %s %s' % (command, ' '.join(args))
            message = '\n'.join((call_str, e.message))
            raise Exception(message)

    def find_commands(self, directory=None):
        """
        Method returns iterator over Command subclasses that found in modules of given directory
        """
        result = []
        if not directory:
            directory = __dir__
        modules = find_modules(directory)
        for module in modules:
            # 'commands' is name of attr that defines list of provided top-level szradm commands.
            # Subcommands should not be included
            if hasattr(module, 'commands'):
                for cmd in module.commands:
                    yield cmd


def main(argv):
    init_script()
    szradm = Szradm(os.path.join(__dir__, 'commands'))
    # If szradm called with no arguments - print help() and all/most used possible commands
    szradm_kwds = command_module.parse_command_line(argv[1:], szradm.help())
    # TODO: return exit codes which are dependent on exception thrown
    return szradm(**szradm_kwds)


if __name__ == '__main__':
    sys.exit(main(sys.argv))
