'''
Created on Nov 2, 2012

@author: uty
'''
import os
import mock
import StringIO

from scalarizr.api import nginx

class TestNginxAPI(object):

    def test_parse_roles(self):
        api = nginx.NginxAPI()
        roles = [123, '456', {'id': '098', 'down': True}]
        parsed_roles = api._parse_roles(roles)
        assert parsed_roles == [
            {'id': '123', 'servers': []},
            {'id': '456', 'servers': []},
            {'id': '098', 'servers': [], 'down': True}], "%s" % parsed_roles

    def test_parse_servers(self):
        api = nginx.NginxAPI()
        servers = ['123.123.132.321', {'host': '10.10.10.10', 'backup': True}]
        parsed_servers = api._parse_servers(servers)
        assert parsed_servers == [{'servers': ['123.123.132.321']},
                                  {'host': '10.10.10.10', 'servers': ['10.10.10.10'], 'backup': True}], \
            "%s" % parsed_servers

    def test_make_backend_conf(self):
        api = nginx.NginxAPI()
        destinations = [{'servers': ['123.231.0.1', '122.232.0.9'], 'max_fails': 5},
                        {'servers': ['0.0.0.1', '1.1.1.9'], 'down': True}]
        desired_config = '''upstream ppp {
\tiphash;
\tserver\t123.231.0.1 max_fails=5;
\tserver\t122.232.0.9 max_fails=5;
\tserver\t0.0.0.1 down;
\tserver\t1.1.1.9 down;
}
'''
        conf = api._make_backend_conf('ppp', destinations)
        str_fp = StringIO.StringIO()
        conf.write_fp(str_fp, close=False)
        assert desired_config == str_fp.getvalue(), '%s' % str_fp.getvalue()

    def test_group_destinations(self):
        api = nginx.NginxAPI()
        destinations = [
            {'id': '123', 'location': '/'},
            {'id': '234'},
            {'id': '1232', 'location': '/something'},
            {'id': '0158', 'location': '/something'},
            {'id': '756', 'location': '/alocation'},
            {'id': '951', 'location': '/alocation'}
        ]
        grouped_destinations = api._group_destinations(destinations)
        assert grouped_destinations == [[{'id': '1232', 'location': '/something'},
                                         {'id': '0158', 'location': '/something'}],
                                        [{'id': '756', 'location': '/alocation'},
                                         {'id': '951', 'location': '/alocation'}],
                                        [{'id': '123', 'location': '/'},
                                         {'id': '234', 'location': '/'}]], \
            '%s' % grouped_destinations
