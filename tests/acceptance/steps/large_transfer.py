__author__ = 'vladimir'


import shutil
import tempfile
import os
import subprocess

from lettuce import step, world, before, after

from scalarizr.storage2.cloudfs import LargeTransfer


@before.each_scenario
def setup(scenario):
	world.basedir = tempfile.mkdtemp()
	world.result = None


@after.each_scenario
def teardown(scenario):
	shutil.rmtree(world.basedir)



@step("I have a (\d+) megabytes file (\w+)")
def i_have_a_file(step, megabytes, filename):
	subprocess.call([
		"dd",
		"if=/dev/urandom",
		"of=%s" % os.path.join(world.basedir, filename),
		"bs=1M",
		"count=%s" % megabytes
	])
	world.source = os.path.join(world.basedir, filename)


@step("I (upload|download) it to ([^\s]+) with gzipping")
def i_load_it_with_gzipping(step, direction, url):
	world.result = LargeTransfer(world.source, url, direction)


