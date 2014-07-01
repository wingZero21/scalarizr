__author__ = 'spike'

import os
import time
from habibi import Habibi, events

from behave import given, when, then, step

validator_key_path = os.environ.get('VALIDATOR_KEY_PATH', '/etc/chef/validator.key')
try:
    with open(validator_key_path) as f:
        VALIDATOR_KEY = f.read()
except:
    raise Exception('Chef validator key is not available')


class ChefScriptSpy(object):

    @events.listener({'event': 'outgoing_message', 'message.name': 'HostInitResponse'})
    def add_scripts(self, event):
        event.message.body['scripts'] = [{
            "timeout": 120,
            "asynchronous": 0,
            "chef": {
               "cookbook_url": "https://github.com/Scalr/fixtures.git",
               "cookbook_url_type": "git",
               "relative_path": "cookbooks",
               "run_list": '["recipe[test_file_create]"]'
            }
        }]


class ChefClientSpy(object):

    @events.listener({'event': 'outgoing_message', 'message.name': 'HostInitResponse'})
    def add_bootstrap(self, event):
        event.message.body['chef'] = {
                "server_url": "https://api.opscode.com/organizations/webta",
                "role": 'dummy_role',
                "validator_key": VALIDATOR_KEY,
                "validator_name": "webta-validator",
                "environment": "_default",
                "node_name": "acceptancetest.%s" % time.time()
        }

    @events.listener({'event': 'outgoing_message', 'message.name': 'HostUp'})
    def add_scripts(self, event):
        event.message.body['scripts'] = [
            {
                "timeout": 120,
                "asynchronous": 0,
                "chef": {
                    "server_url": "https://api.opscode.com/organizations/webta",
                    "run_list": '["recipe[create_file]"]',
                    "json_attributes": '{"create_file": {"path": "/tmp/xoxoxo"}}'
                }
            }
        ]




@given('I have configured chef-client for the role')
def bootstrap(context):
    context.spy = ChefClientSpy()
    context.farm.spy(context.spy)

@given('I have configured role in farm')
def add_role_to_farm(context):
    context.farm = Habibi()
    context.role = context.farm.add_role("rolename", ["chef"])

@given("I add chef-solo scripts to HostInit event")
def add_chef_scripts(context):
    context.spy = ChefScriptSpy()
    context.farm.spy(context.spy)

@when("I start server")
def start_server(context):
    context.farm.start()
    context.server = context.role.run_server()

@then('I see that chef scripts were successfully executed')
def i_see_results(context):
    def check_exec_result(event):
        assert int(event.message.body['return_code']) == 0

    context.farm.event_mgr.wait({'event': 'incoming_message', 'message.name': 'ExecScriptResult'}, timeout=300,
                                        fn=check_exec_result)

    file_path = os.path.join(context.server.rootfs_path, "tmp/xoxoxo")
    assert os.path.exists(file_path), "%s doesnt exist" % file_path




