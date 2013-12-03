import inspect

from scalarizr.adm.command import Command
from scalarizr.util import system2


class Queryenv(Command):
    """
    queryenv command is used to launch queryenv methods.

    Usage:
      queryenv --help
      queryenv list-roles [--behaviour=<bhvr>] [--role-name=<rolename>] [--with-initializing]
      queryenv list-virtualhosts [--name=<name>] [--https]
      queryenv get-latest-version
      queryenv list-ebs-mountpoints
      queryenv get-https-certificate
      queryenv list-role-params
      queryenv list-scripts [--event=<event>] [--asynchronous] [--name=<name>]
      queryenv <method> [<args>...]

    General options:
      -h, --help               Show version.
    
    Options for list-roles:
      -b, --behaviour=<bhvr>      Role behaviour.
      -r, --role-name=<rolename>  Role name.
      -i, --with-initializing     Show initializing servers

    Options for list-virtualhosts:
      -n, --name               Show virtual host by name
      -s, --https              Show virtual hosts by https
    """

    def __init__(self):
        super(Queryenv, self).__init__()

    @classmethod
    def queryenv(cls):
        if not hasattr(cls, '_queryenv'):
            cls._queryenv = new_queryenv()
        return cls._queryenv

    def _run_queryenv_method(self, method, kwds, kwds_mapping=None):
        """
        Executes queryenv method with given `kwds`. `kwds` can contain excessive
        key-value pairs, this method filters it and passes only acceptable
        kwds by target method. `kwds_mapping` defines how `kwds` keys will be
        renamed when passed to queryenv method.
        """
        if not method:
            return
        if not kwds_mapping:
            kwds_mapping = {}

        method = method.replace('-', '_')
        m = getattr(self.queryenv, method)
        argnames = inspect.getargspec(m).args
        filtered_kwds = {}
        for k, v in kwds.items():
            arg_name = kwds_mapping.get(k, k)
            if arg_name in argnames:
                filtered_kwds[arg_name] = v

        return m(**filtered_kwds)


    def __call__(self, method=None, help=False, args=None, **kwds):
        if help:
            self.help()
            return
        if method == 'list-roles':
            return self._run_queryenv_method(
                method,
                kwds,
                {'with_initializing': 'with_init'})
        elif method == 'list-virtualhosts':
            return self._run_queryenv_method(method, kwds)
        else:
            return self._run_queryenv_method(method, kwds)


commands = [Queryenv]
