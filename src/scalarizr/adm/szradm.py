#!/usr/bin/python
import sys
import os
import imp
import time
import resource

from scalarizr.adm import command as command_module


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
      szradm --version
      szradm <command> [<args>...]

    Options:
      -v, --version     Show version.
    """

    def __init__(self, commands_dir=None):
        super(Szradm, self).__init__()
        self.subcommands = self.find_commands(commands_dir)

    def __call__(self, command=None, version=False, help=False, args=[]):
        try:
            return self.run_subcommand(command, args)

        except (command_module.UnknownCommand, command_module.InvalidCall), e:
            call_str = 'szradm ' + command + ' ' + ' '.join(args)
            message = '\n'.join((call_str, e.message, e.usage))
            raise e.__class__(message)

        except command_module.RuntimeError, e:
            # except-section for user-defined exceptions, semantic errors, etc.
            call_str = 'szradm ' + command + ' ' + ' '.join(args)
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
    szradm = Szradm(os.path.join(__dir__, 'commands'))
    # If szradm called with no arguments - print help() and all/most used possible commands
    szradm_kwds = command_module.parse_command_line(argv[1:], szradm.help())
    # TODO: return exit codes which are dependent on exception thrown
    return szradm(**szradm_kwds)


if __name__ == '__main__':
    main(sys.argv)
