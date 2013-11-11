#!/usr/bin/python
import sys
import os
import re
import imp
import inspect

from docopt import docopt


__dir__ = os.path.dirname(__file__)


def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def docopt_args_to_kwds(arguments):
    result = {}
    for k, v in arguments.items():
        new_k = k.lstrip('-').replace('-', '_')
        new_k = new_k.replace('<', '').replace('>', '')
        result[new_k] = v
    return result


class Command(object):
    """Class that represents scalarizr command"""

    # list of Command subclasses, that will be used as subcommands
    subcommands = []

    def __init__(self):
        super(Command, self).__init__()

    def __call__(self):
        raise NotImplementedError('You need to specify __call__ method')

    def help(self):
        return self.__doc__

    def run_subcommand(self, subcommand=None, *args):
        for sub_cmd_class in self.subcommands:
            name = camel_to_underscore(sub_cmd_class.__name__).replace('_', '-')
            if name == subcommand:
                sub_cmd = sub_cmd_class()
                kwds = sub_cmd.parse_args(args)
                return sub_cmd(**kwds)
        raise BaseException('Unknown subcommand: %s' % subcommand)

    def parse_args(self, args):
        arguments = docopt(self.__doc__, argv=args, options_first=True)
        kwds = docopt_args_to_kwds(arguments)
        return kwds

    @classmethod
    def command(cls, function):
        """Decorator that makes new Command class inheritor from function"""
        call = lambda self, *args, **kwds: function(*args, **kwds)
        attrs = {'__call__': call, '__doc__': function.__doc__}
        command_class = type(function.__name__, (cls,), attrs)
        return command_class


class SubCommand(Command):
    """
    This class is used to mark subcommand classes so they won't appear in
    find_commands() result.
    """
    pass


def find_modules(directory):
    """Method finds modules in given directory and subdirectories"""
    directory = os.path.abspath(directory)
    result = []

    module_paths = [os.path.join(directory, m) for m in os.listdir(directory)]
    for path in module_paths:
        is_module = path.endswith('.py') \
            and path is not __file__ \
            and path is not os.path.join(__dir__, '__init__.py')
        if is_module:
            module_name = os.path.basename(path).replace('.py', '')
            module = imp.load_source(module_name, path)
            result.append(module)
        elif os.path.isdir(path):
            result += find_modules(path)
    return result


def find_commands():
    """Method finds commands in modules of this package"""
    result = {} #{'szradm': Szradm} TODO: make Szradm class
    modules = find_modules(__dir__)
    is_command = lambda x: inspect.isclass(x) and x.__base__.__name__ == 'Command' #and issubclass(x, Command) #and \?????????????????
         # not issubclass(x, SubCommand)
    for module in modules:
        for el in inspect.getmembers(module):
            if is_command(el[1]):
                command_name = camel_to_underscore(el[0]).replace('_', '-')
                command_class = el[1]
                result[command_name] = command_class
    return result


# TODO: unfold args
def szradm(command=None, version=False, help=False, args=[]):
    """
    Szradm is scalarizr administration tool.

    Usage:
      szradm --version
      szradm --help
      szradm <command> [<args>...]

    Options:
      -v, --version     Show version.
    """
    commands_table = find_commands()
    # print 'commands table:', commands_table
    if command not in commands_table:
        print "Unknown command."
        print szradm.__doc__
        return
    command_class = commands_table[command]
    cmd = command_class()
    parsed_kwds = cmd.parse_args(args)
    # TODO: set some fields/params
    # print parsed_kwds, cmd.__class__
    exit(cmd(**parsed_kwds))


def main(argv):
    arguments = docopt(szradm.__doc__, argv=argv[1:], options_first=True)
    # arguments = {k: v for k, v in arguments.items() if v}
    # TODO: do some work with szradm related arguments. Like --version, etc.
    szradm_kwds = docopt_args_to_kwds(arguments)
    szradm(**szradm_kwds)


if __name__ == '__main__':
    main(sys.argv)
