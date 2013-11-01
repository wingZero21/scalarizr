#!/usr/bin/python
import sys
import os
import imp

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


# list containing methods and functions that are defined as commands
command_list = []


def find_modules(directory):
    """Method finds modules in given directory and subdirectories"""
    result = []
    module_paths = [os.path.join(directory, m) for m in os.listdir(directory)]
    for path in module_paths:
        if path.endswith('.py'):
            module_name = os.path.basename(path).replace('.py', '')
            module = imp.load_source(module_name, path)
            result.append(module)
        elif os.path.isdir(path):
            result += find_modules(path)
    return result


def find_commands():
    """Method finds commands in modules of this package"""
    module_paths = [os.path.join(__dir__, m) for m in os.listdir(__dir__)]
    for path in module_paths:
        if path.endswith('.py'):
            # import module, find commands
            pass
        elif os.path.isdir(path):
            # import modules from package
            pass


def main(**argv):
    arguments = docopt(__doc__, argv=argv, options_first=True)


if __name__ == '__main__':
    main(sys.argv)
