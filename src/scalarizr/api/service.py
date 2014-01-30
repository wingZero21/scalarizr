'''
Created on Aug 16, 2012

@author: Dmytro Korsakov
'''

from __future__ import with_statement

from scalarizr import rpc
from scalarizr.api import operation
from scalarizr.bus import bus
from scalarizr.node import __node__
from scalarizr.api.nginx import NginxAPI
from scalarizr.api.apache import ApacheAPI


services = {}


behavior_apis = {
    'www': NginxAPI,
    'app': ApacheAPI,
}


class ServiceAPI(object):

    def __init__(self):
        self._op_api = operation.OperationAPI()
        self.queryenv = bus.queryenv_service

    @rpc.query_method
    def get_preset(self, behavior):
        if behavior not in services:
            raise AssertionError('Behaviour %s is not registred in ServiceAPI')

        provider = services[behavior]
        manifest = provider.get_manifest(behavior)
        if manifest:
            return provider.get_preset(manifest)
        else:
            raise BaseException('Cannot retrieve preset: Manifest not found.')

    @rpc.command_method
    def set_preset(self, behavior, values):
        if behavior not in services:
            raise AssertionError('Behaviour %s is not registred in ServiceAPI')

        provider = services[behavior]
        manifest = provider.get_manifest(behavior)
        if manifest:
            provider.set_preset(values, manifest)
        else:
            raise AssertionError('Cannot apply preset: Manifest not found.')

    def do_reconfigure(self, behavior_params=None):
        """
        behavior_params is dict where behavior names are keys and they
        reconfigure params are values (they are itself dicts)
        """
        if not behavior_params:
            behavior_params = queryenv.list_farm_role_params(__node__['farm_role_id'])['params']

        behaviors = behavior_params.keys()
        for behavior in behaviors:
            api = behavior_apis[behavior]
            if api.hasattr('init_service'):
                api.init_service()
            api.do_reconfigure(**behavior_params.get(behavior, {}))

    @rpc.query_method
    def reconfigure(self, behavior_params=None, async=True):
        """ If behavior_params is None - reconfiguring all behaviors """
        self._op_api.run('api.service.reconfigure',
                         func=do_backup,
                         func_kwds={'behavior_params': behavior_params},
                         async=async,
                         exclusive=True)
