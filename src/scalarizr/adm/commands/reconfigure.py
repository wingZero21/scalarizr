import sys

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
        api.reconfigure()




commands = [Reconfigure]