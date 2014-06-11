__author__ = 'spike'

import os
from habibi import Habibi, events

from behave import given, when, then, step



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



@given('I have configured role in farm')
def add_role_to_farm(context):
    context.farm = Habibi()
    context.role = context.farm.add_role("rolename", [])

@given("I add chef scripts to HostInit event")
def add_chef_scripts(context):
    context.spy = ChefScriptSpy()
    context.farm.spy(context.spy)

@when("I start server")
def start_server(context):
    context.farm.start()
    context.server = context.role.run_server()

@then('I see that chef scripts were successfully executed')
def i_see_results(context):
    context.farm.event_mgr.wait({'event': 'incoming_message', 'message.name': 'HostUp'}, timeout=1200)
    file_path = os.path.join(context.server.rootfs_path, "tmp/test_file")
    assert os.path.exists(file_path), "%s doesnt exist" % file_path




