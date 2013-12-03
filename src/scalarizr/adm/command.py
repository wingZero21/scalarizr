import re
import inspect

from docopt import docopt
from docopt import DocoptExit
from docopt import parse_section as get_section


def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


class UnknownCommand(SystemExit):
    
    def __init__(self, message, command_name=None, usage=None):
        super(UnknownCommand, self).__init__(message)
        self.command_name = command_name
        self.usage = usage


class InvalidCall(SystemExit):
    
    def __init__(self, message, command_name=None, usage=None):
        super(InvalidCall, self).__init__(message)
        self.command_name = command_name
        self.usage = usage


class Command(object):
    """Class that represents scalarizr command"""

    # list or generator of Command subclasses, that will be used as subcommands
    subcommands = []
    aliases = []

    def __init__(self):
        super(Command, self).__init__()

    def __call__(self):
        raise NotImplementedError('You need to define __call__ method')

    def help(self):
        doc = self.__doc__
        if not '--help' in self.__doc__:
            match = re.search(r'usage:\s+', x, re.IGNORECASE | re.DOTALL)
            # Insert --help usage line after match.end() with tabs      
        return self.__doc__

    def run_subcommand(self, subcommand, args):
        """
        Searches subcommand in self.subcommands and launches it with given args.
        If no subcommand found - raises exception.
        """
        for sub_cmd_definition in self.subcommands:
            name = get_command_name(sub_cmd_definition)
            aliases = sub_cmd_definition.aliases if hasattr(sub_cmd_definition, 'aliases') else []

            if subcommand == name or subcommand in aliases:
                # assuming that by default subcommand is simple function
                sub_cmd = sub_cmd_definition
                is_class = inspect.isclass(sub_cmd_definition)
                sub_cmd_doc = sub_cmd.help() if is_class else sub_cmd.__doc__
                try:
                    kwds = parse_command_line(args, sub_cmd_doc)
                except DocoptExit:
                    usage = ''.join(get_section('usage', sub_cmd_doc))
                    raise InvalidCall('%s: invalid call' % get_command_name(sub_cmd), subcommand, usage)

                if inspect.isclass(sub_cmd_definition):
                    sub_cmd = sub_cmd_definition()
                elif 'self' in inspect.getargspec(sub_cmd_definition).args:
                    kwds['self'] = self

                return sub_cmd(**kwds)

        usage = ''.join(get_section('usage', self.help()))
        raise UnknownCommand('%s: unknown subcommand %s' % (self._name, subcommand),
                             subcommand,
                             usage)


def parse_command_line(argv, doc):
    """
    Parses list of command-line argv using doc and translates
    them to keyword dictionary which can be used to call Command instance.
    """
    arguments = docopt(doc, argv=argv, options_first=True)
    kwds = _docopt_out_to_kwds(arguments)
    return kwds


def _docopt_out_to_kwds(arguments):
    """
    Renaming arguments from command-line-like to python-like.
    """
    result = {}
    for k, v in arguments.items():
        new_k = k.lstrip('-').replace('-', '_')
        new_k = new_k.replace('<', '').replace('>', '')
        result[new_k] = v
    return result


def get_command_name(cls_or_func):
    """
    Returns command name from given class or class name. 
    (cls or func or str) -> str
    """
    name = cls_or_func
    if inspect.isclass(cls_or_func) or inspect.isfunction(cls_or_func):
        name = cls_or_func.__name__
    return camel_to_underscore(name).replace('_', '-')

