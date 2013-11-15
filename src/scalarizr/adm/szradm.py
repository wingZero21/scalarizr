#!/usr/bin/python
import sys
import os
import re
import imp
import inspect
import time
import resource

from docopt import docopt


__dir__ = os.path.dirname(__file__)


def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


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


class Command(object):
    """Class that represents scalarizr command"""

    # list or generator of Command subclasses, that will be used as subcommands
    subcommands = []

    def __init__(self):
        super(Command, self).__init__()
        for attr_name in dir(self):
            attr = getattr(self, attr_name)
            if inspect.isclass(attr) and 

    def __call__(self):
        raise NotImplementedError('You need to define __call__ method')

    def help(self):
        return self.__doc__

    def run_subcommand(self, subcommand, args):
        """
        Searches subcommand in self.subcommands and launches it with given args.
        If no subcommand found - raises exception.
        """
        for sub_cmd_class in self.subcommands:
            name = Command.class_to_command_name(sub_cmd_class)
            if name == subcommand:
                sub_cmd = sub_cmd_class()
                kwds = sub_cmd.parse_args(args)
                return sub_cmd(**kwds)
        raise BaseException('Unknown subcommand: %s' % subcommand)

    def parse_args(self, args):
        """
        Parses list of command-line args using self.__doc__ and translates
        them to keyword dictionary which can be used to call self.
        """
        arguments = docopt(self.__doc__, argv=args, options_first=True)
        kwds = docopt_args_to_kwds(arguments)
        return kwds

    def docopt_args_to_kwds(self, arguments):
        result = {}
        for k, v in arguments.items():
            new_k = k.lstrip('-').replace('-', '_')
            new_k = new_k.replace('<', '').replace('>', '')
            result[new_k] = v
        return result

    @classmethod
    def command(cls, function):
        """Decorator that makes new Command class inheritor from function"""
        if inspect.ismethod(function):
            call = function
        else:
            call = lambda self, *args, **kwds: function(*args, **kwds)
        attrs = {'__call__': call, '__doc__': function.__doc__}
        command_class = type(function.__name__, (cls,), attrs)
        return command_class

    @classmethod
    def class_to_command_name(cls, c):
        """
        Returns command name from given class or class name. 
        (type or str) -> str
        """
        class_name = c
        if type(c) == type:
            class_name = c.__name__
        return camel_to_underscore(class_name).replace('_', '-')


class Szradm(Command):
    """
    Szradm is scalarizr administration tool.

    Usage:
      szradm --version
      szradm --help
      szradm <command> [<args>...]

    Options:
      -v, --version     Show version.
    """

    def __init__(self):
        super(Szradm, self).__init__()
        self.subcommands = self.find_commands()

    def __call__(self, command=None, version=False, help=False, args=[]):
        try:
            return self.run_subcommand(command, args)
        except BaseException:
            print "Unknown command."
            print self.__doc__

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
    arguments = docopt(Szradm.__doc__, argv=argv[1:], options_first=True)
    szradm_kwds = docopt_args_to_kwds(arguments)

    szradm = Szradm()
    return szradm(**szradm_kwds)


if __name__ == '__main__':
    start_time = time.time()
    main(sys.argv)
    print time.time() - start_time, "seconds"
