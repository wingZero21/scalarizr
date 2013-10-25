import sys
import os

from docopt import docopt


class Command(object):
    """Class that represents scalarizr command"""

    def __init__(self):
        super(Command, self).__init__()

    def __call__(self):
        raise NotImplementedError('You need to specify __call__ method')

    @classmethod
    def command(cls):
        """Decorator that makes new Command class inheritor from functon"""
        pass


# list containing methods and functions that are defined as commands
command_list = []


def find_commands():
    """Method finds commands in modules of this package"""
    pass



def main(**argv):
    arguments = docopt(__doc__, argv=argv, options_first=True)


if __name__ == '__main__':
    main(sys.argv)
