#!/usr/bin/python
import sys
import os
import re
import imp
import inspect

from docopt import docopt


__dir__ = os.path.dirname(__file__)

class Command(object):
    """Class that represents scalarizr command"""

    def __init__(self):
        super(Command, self).__init__()

    def __call__(self):
        raise NotImplementedError('You need to specify __call__ method')

    def help(self):
        return self.__doc__

    @classmethod
    def command(cls, function):
        """Decorator that makes new Command class inheritor from function"""
        pass


def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def underscore_to_hyphen(name):
    return name.replace('_', '-')


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
    result = []
    modules = find_modules(__dir__)
    for module in modules:
        is_command = lambda x: inspect.isclass(x) and issubclass(x, Command)
        commands = [el[1] for el in inspect.getmembers(module) if is_command(el[1])]
        result.extend(commands)
    return result


def main(**argv):
    arguments = docopt(__doc__, argv=argv, options_first=True)


if __name__ == '__main__':
    main(sys.argv)
