# -*- coding: utf-8 -*-
import BaseHTTPServer
import urllib2
import time
from threading import Thread

from lettuce import step
from lettuce import world
from lettuce import before
from scalarizr.api import nginx


class Server(BaseHTTPServer.HTTPServer):
    """
    Server creates simple HTTP listener on given port on localhost
    """

    def __init__(self, port, get_response=None):
        "process with running server should be creaded"
        if not get_response:
            get_response = "<html><head><title>Something</title></head></html>"

        class _Handler(BaseHTTPServer.BaseHTTPRequestHandler):

            def do_HEAD(self):
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()

            def do_GET(self):
                """Respond to a GET request."""
                self.send_response(200)
                self.send_header("Content-type", "text/html")
                self.end_headers()
                self.wfile.write(get_response)

        super_cls = BaseHTTPServer.HTTPServer
        super_cls.__init__(self, ('localhost', port), _Handler)

        def serve_job():
            super_cls.serve_forever(self)

        self.serve_thread = Thread(target=serve_job)
        self.serve_thread.daemon = True

    def serve_forever(self):
        self.serve_thread.start()


def clear_nginx_includes():
    with open('/etc/nginx/app-servers.include', 'w') as fp:
        fp.write('')
    with open('/etc/nginx/https.include', 'w') as fp:
        fp.write('')


def read_nginx_includes():
    result = None
    with open('/etc/nginx/app-servers.include', 'r') as fp:
        result = fp.read() + '\n\n'
    with open('/etc/nginx/https.include', 'r') as fp:
        result += fp.read() + '\n\n'
    return result


@before.each_feature
def create_api(feature=None):
    world.api = nginx.NginxAPI()


@step(u'Given I have a server')
def given_i_have_a_server(step):
    world.expected_response = 'KUKU'
    world.server = Server(8000, world.expected_response)
    world.server.serve_forever()
    world.servers = [{'host': 'localhost', 'port': '8000'}]
    world.roles = None


@step(u'When I add proxy')
def when_i_add_proxy(step):
    world.api.add_proxy('uty.com',
                        roles=world.roles,
                        servers=world.servers,
                        port=8008)


@step(u'Then I expect proxying to server')
def then_i_expect_proxying_to_server(step):
    conn = urllib2.urlopen('http://localhost:8008')
    response = conn.read()
    world.server.shutdown()
    world.server.server_close()
    world.server = None

    clear_nginx_includes()
    world.servers = None

    assert response == world.expected_response

###############################################################################


@step(u'Given I have a role')
def given_i_have_a_role(step):
    server1_port = 8000
    server2_port = 8001
    # Mocking up get role servers to return our Server adresses
    world.api._get_role_servers = lambda x: ['localhost:%s' % server1_port,
                                             'localhost:%s' % server2_port]

    world.expected_response1 = 'Test1'
    world.expected_response2 = 'Test2'
    world.server1 = Server(server1_port, world.expected_response1)
    world.server1.serve_forever()
    world.server2 = Server(server2_port, world.expected_response2)
    world.server2.serve_forever()

    world.roles = [123]
    world.servers = None


@step(u'Then I expect proxying to role')
def then_i_expect_proxying_to_role(step):
    conn = urllib2.urlopen('http://localhost:8008')
    response1 = conn.read()
    conn = urllib2.urlopen('http://localhost:8008')
    response2 = conn.read()

    world.server1.shutdown()
    world.server1.server_close()
    world.server2.shutdown()
    world.server2.server_close()

    clear_nginx_includes()
    world.roles = None
    create_api()

    assert response1 == world.expected_response1, response1
    assert response2 == world.expected_response2, response2

###############################################################################


@step(u'Given I have a proxy to a role')
def given_i_have_a_proxy_to_a_role(step):
    given_i_have_a_role(step)
    when_i_add_proxy(step)


@step(u'When I launch new server of this role')
def when_i_launch_new_server_of_this_role(step):
    


@step(u'Then server appears in backend')
def then_server_appears_in_backend(step):
    assert False, 'This step must be implemented'


###############################################################################


@step(u'When I terminate one server of this role')
def when_i_terminate_one_server_of_this_role(step):
    assert False, 'This step must be implemented'


@step(u'Then server removed from backend')
def then_server_removed_from_backend(step):
    assert False, 'This step must be implemented'


@step(u'And I have SSL keypair')
def and_i_have_ssl_keypair(step):
    assert False, 'This step must be implemented'


@step(u'Then I expect proxying https -> http')
def then_i_expect_proxying_https_http(step):
    assert False, 'This step must be implemented'


@step(u'And I have HTTP disabled')
def and_i_have_http_disabled(step):
    assert False, 'This step must be implemented'


@step(u'And I expect redirect https -> http')
def and_i_expect_redirect_https_http(step):
    assert False, 'This step must be implemented'


@step(u'Given I have a proxy to two roles: master and backup')
def given_i_have_a_proxy_to_two_roles_master_and_backup(step):
    assert False, 'This step must be implemented'


@step(u'When I terminate master servers')
def when_i_terminate_master_servers(step):
    assert False, 'This step must be implemented'


@step(u'Then I expect proxying to backup servers')
def then_i_expect_proxying_to_backup_servers(step):
    assert False, 'This step must be implemented'


@step(u'Given I have a proxy to two servers')
def given_i_have_a_proxy_to_two_servers(step):
    assert False, 'This step must be implemented'


@step(u'When I update proxy marking one server as down')
def when_i_update_proxy_marking_one_server_as_down(step):
    assert False, 'This step must be implemented'


@step(u'Then I expect proxying to remaining server')
def then_i_expect_proxying_to_remaining_server(step):
    assert False, 'This step must be implemented'


@step(u'Given I have a regular server S')
def given_i_have_a_regular_server_s(step):
    assert False, 'This step must be implemented'


@step(u'And I have a down server SD')
def and_i_have_a_down_server_sd(step):
    assert False, 'This step must be implemented'


@step(u'And I have I backup server SB')
def and_i_have_i_backup_server_sb(step):
    assert False, 'This step must be implemented'


@step(u'And I have a regular role R')
def and_i_have_a_regular_role_r(step):
    assert False, 'This step must be implemented'


@step(u'And I have a backup role RB')
def and_i_have_a_backup_role_rb(step):
    assert False, 'This step must be implemented'


@step(u'And I have a down role RD')
def and_i_have_a_down_role_rd(step):
    assert False, 'This step must be implemented'


@step(u'Then I expect S and R servers are regular in backend')
def then_i_expect_s_and_r_servers_are_regular_in_backend(step):
    assert False, 'This step must be implemented'


@step(u'And I expect SD and RD servers are down in backend')
def and_i_expect_sd_and_rd_servers_are_down_in_backend(step):
    assert False, 'This step must be implemented'


@step(u'And I expect SB and RB servers are backup in backend')
def and_i_expect_sb_and_rb_servers_are_backup_in_backend(step):
    assert False, 'This step must be implemented'
