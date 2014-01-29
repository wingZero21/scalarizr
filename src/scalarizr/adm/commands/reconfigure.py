import sys
import inspect

from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.adm.command import Command
from scalarizr.adm.command import CommandError
from scalarizr.api.nginx import NginxAPI


behavior_apis = {
    'nginx': NginxAPI,
}


class Reconfigure(Command):

    def __call__(self, behavior=None):

        if behavior not in behavior_apis:
            raise CommandError('Unknown behavior.')

        api = behavior_apis[behavior]()
        reconfigure_args = inspect.getargspec(api.reconfigure).args
        reconfigure_args.remove('self')
        
        queryenv = bus.queryenv_service
        role_params = queryenv.list_farm_role_params(__node__['farm_role_id'])['params']
        behavior_params = role_params.get(behavior, {})
        behavior_params = dict((k, v) for k, v in behavior_params.items() if k in reconfigure_args)

        try:
            api.reconfigure(**behavior_params)
        except (BaseException, Exception), e:
            print 'Reconfigure failed.\n%s' % e
            return int(CommandError())
        return 0


commands = [Reconfigure]
