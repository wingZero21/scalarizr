__author__ = 'spike'
from habibi import Habibi, events

from behave import given, when, then, step


class ChefScriptSpy(object):

    @events.listener({'event': 'outgoing_message', 'message.name': 'HostInitResponse'})
    def add_scripts(self, event):
        event.message.body['scripts'] = [{
            "timeout": 120,
            "chef": {

            }
        }]



@given('I have configured role in farm')
def add_role_to_farm(context):
    context.farm = Habibi()
    context.role = context.farm.add_role("rolename", ["base"])
