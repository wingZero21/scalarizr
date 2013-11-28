from scalarizr.adm.command import Command
from scalarizr.util import system2


class SubCmd(Command):
    """
    sub-cmd subcommand test. Prins its arg.

    Usage:
      sub-cmd [--version] <arg>

    Options:
      -v, --version     Show version.
    """

    def __call__(self, arg=None, version=False):
        print 'Uhh... helloe'
        if version:
            print 'sub-cmd version is 0.01'
        if arg:
            print 'arg is: %s' % arg
        else:
            raise BaseException('arg is needed')


class ClsCmd(Command):
    """
    cls-cmd is test command

    Usage:
      cls-cmd [--version] [<command>] [<args>...]

    Options:
      -v, --version     Show version.
    """

    subcommands = [SubCmd]
    
    def __call__(self, command=None, version=False, args=[]):
        if version:
            print 'cls-cmd version is 0.02'
        if not command:
            print 'Use sub-cmd to see something'
        else:
            return self.run_subcommand(command, args)


def status():
    """
    Status is the command for getting system basic info.

    Usage:
      status
    """
    out = system2(['uname -a'], shell=True)[0]
    print out


commands = [status, ClsCmd]
