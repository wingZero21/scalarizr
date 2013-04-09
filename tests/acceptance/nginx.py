# -*- coding: utf-8 -*-
from lettuce import step
from lettuce import world

@step(u'Given I have a server')
def given_i_have_a_server(step):
    assert False, 'This step must be implemented'

@step(u'When I add proxy')
def when_i_add_proxy(step):
    assert False, 'This step must be implemented'

@step(u'Then I expect proxying')
def then_i_expect_proxying(step):
    assert False, 'This step must be implemented'

###############################################################################

@step(u'Given I have a role')
def given_i_have_a_role(step):
    assert False, 'This step must be implemented'

###############################################################################

@step(u'Given I have a proxy to a role')
def given_i_have_a_proxy_to_a_role(step):
    assert False, 'This step must be implemented'

@step(u'When I launch new server of this role')
def when_i_launch_new_server_of_this_role(step):
    assert False, 'This step must be implemented'

@step(u'Then server appears in backend')
def then_server_appears_in_backend(step):
    assert False, 'This step must be implemented'

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