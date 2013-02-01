__author__ = 'vladimir'

import os

from lettuce import step, world, before, after

from scalarizr.api import system


FEATURE = 'API'
SCRIPT_PATH = '/usr/local/scalarizr/hooks/auth-shutdown'


@before.each_feature
def setup(feature):
	if feature.name == FEATURE:
		world.prev = None
		if os.path.exists(SCRIPT_PATH):
			with open(SCRIPT_PATH) as f:
				world.previous = f.read()
		if not os.path.exists(os.path.dirname(SCRIPT_PATH)):
			os.makedirs(os.path.dirname(SCRIPT_PATH))


@after.each_feature
def teardown(feature):
	if feature.name == FEATURE:
		if world.prev:
			with open(SCRIPT_PATH, 'w') as f:
				f.write(world.previous)
		else:
			os.remove(SCRIPT_PATH)


@before.each_scenario
def setup_(scenario):
	world.result = None


@step("I have a script that returns (\d)")
def i_have_script_that_returns(step, code):
	script = "#!/bin/bash\n"\
			 "exit %s\n"

	with open(SCRIPT_PATH, 'w') as f:
		f.write(script % code)

	os.chmod(SCRIPT_PATH, 0744)  #? 755?


@step("I call it")
def i_call_it(step):
	world.result = system.SysInfoAPI().call_auth_shutdown_hook()


@step("I expect (\d) returned")
def i_expect_returned(step, code):
	assert world.result == int(code), "%s != %s" % (world.result, code)

