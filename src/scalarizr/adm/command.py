import re
import inspect

from docopt import docopt
from docopt import DocoptExit
from docopt import parse_section as get_section


def camel_to_underscore(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def extended_doc(doc, usage_case):
    """Returns doc extended with new usage case"""
    match = re.search(r'usage:\s+', doc, re.IGNORECASE | re.DOTALL)
    help_usage_index = match.end()
    tab_str = match.group().split('\n')[1]
    return doc[:help_usage_index] + usage_case + tab_str + doc[help_usage_index:]


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


class RuntimeError(SystemExit):
    pass


class Command(object):
    """Class that represents scalarizr command"""

    # list or generator of Command subclasses, that will be used as subcommands
    subcommands = []
    aliases = []

    def __init__(self):
        super(Command, self).__init__()

    def __call__(self):
        raise NotImplementedError('You need to define __call__ method')

    def list_subcommands(self):
        """Returns list of possible subcommands"""
        return [get_command_name(cmd) for cmd in self.subcommands]

    def help(self):
        doc = self.__doc__
        if not '--help' in doc:
            help_usage_string = get_command_name(self) + ' --help\n'
            doc = extended_doc(self.__doc__, help_usage_string)

        subcommands_help = '\nSubcommands:\n' + '\n  '.join(self.list_subcommands())
        doc = doc + subcommands_help

        return doc

    def _find_subcommand(self, subcommand):
        """
        Searches subcommand in self.subcommands.
        If no subcommand found - raises exception.
        """
        for sub_cmd in self.subcommands:
            name = get_command_name(sub_cmd)
            if subcommand == name or subcommand in sub_cmd.aliases:
                return sub_cmd

        usage = ''.join(get_section('usage', self.help()))
        raise UnknownCommand('%s: unknown subcommand %s' % (get_command_name(self), subcommand),
                             subcommand,
                             usage)

    def run_subcommand(self, subcommand, args):
        """
        Launches subcommands with given args.
        """
        sub_cmd_definition = self._find_subcommand(subcommand)
        is_class = inspect.isclass(sub_cmd_definition)

        if is_class:
            sub_cmd = sub_cmd_definition()
            sub_cmd_doc = sub_cmd.help()
        else:
            sub_cmd = sub_cmd_definition
            sub_cmd_doc = sub_cmd.__doc__

        try:
            kwds = parse_command_line(args, sub_cmd_doc)
            if 'self' in inspect.getargspec(sub_cmd_definition).args:
                kwds['self'] = self
        except DocoptExit:
            usage = ''.join(get_section('usage', sub_cmd_doc))
            raise InvalidCall('%s: invalid call' % subcommand, subcommand, usage)

        # TODO: maybe do this some other way
        if 'help' in kwds:
            print sub_cmd_doc
        else:
            return sub_cmd(**kwds)


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


def get_command_name(obj):
    """
    Returns command name from given class or class name. 
    (cls or func or str) -> str
    """
    name = obj
    if inspect.isclass(obj) or inspect.isfunction(obj):
        name = obj.__name__
    elif isinstance(obj, object):
        name = obj.__class__.__name__
    return camel_to_underscore(name).replace('_', '-')

