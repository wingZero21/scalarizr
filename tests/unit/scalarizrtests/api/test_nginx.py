'''
Created on Nov 2, 2012

@author: uty
'''
import os

from scalarizr.api import nginx

import mock

class TestNginxAPI(object):

    def test_parse_roles(self):
        api = nginx.NginxAPI()
        roles = [123, '456', {'id': '098', 'down': True}]
        parsed_roles = api._parse_roles(roles)
        assert parsed_roles

    def test_parse_servers(self, servers):
        pass

    def test_make_backend_conf(self, name, destinations):
        pass

    def test_group_destinations(self, destinations):
        pass
