"""
Run on percona-lvm instance.

Requires /root/create_msg.xml and /root/cancel_msg.xml.
"""
__author__ = 'vladimir'

import subprocess
import time

from lettuce import before, after, world, step
import yaml


FEATURE = "Cancel xtrabackup"


def send_message(f):
	n = "DbMsr_CreateDataBundle"
	if "cancel" in n:
		n = "DbMsr_CancelDataBundle"
	subprocess.call([
		"szradm",
		"-m",
		"-e",
		"http://localhost:8013",
		"-o",
		"control",
		"-n",
		n,
		"-f",
		f,
	], close_fds=True)


def list_messages():
	proc = subprocess.Popen([
		"szradm"
		"list-messages",
		"-n",
		"DbMsr_CreateDataBundleResult"
	], stdout=subprocess.PIPE, close_fds=True)
	out = proc.communicate()[0]

	ids = map(lambda x: x.split()[1], out.splitlines()[3:-1])
	return set(ids)


@before.each_scenario
def setup(scenario):
	world.existing = list_messages()


@after.each_scenario
def teardown(scenario):
	world.existing = None


@step("I have used the storage for (\d+) MB")
def i_have_used_the_storage_for(step, mb):
	subprocess.call([
		"dd",
		"if=/dev/urandom",
		"of=%s" % "/mnt/dbstorage/for_test",
		"bs=1M",
		"count=%s" % mb,
	], stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT, close_fds=True)


@step("I have sent CreateDataBundle message")
def i_have_sent_create_data_bundle_message(step):
	send_message("/root/create_msg.xml")


@step("I wait for (\d+) seconds")
def i_wait_for_seconds(step, seconds):
	time.sleep(int(seconds))


@step("I send CancelDataBundle message")
def i_send_cancel_data_bundle_message(step):
	send_message("/root/cancel_msg.xml")


@step("I expect it canceled")
def i_expect_it_canceled(step):
	# we expect to have only one outgoing CreateDataBundleResult message
	# that contains "Canceled"
	new = list_messages() - world.existing
	assert len(new) == 1, "Got multiple messages while running the test"
	msg_id = new.pop()

	proc = subprocess.Popen([
		"szradm",
		"message-details",
		msg_id
	], stdout = subprocess.PIPE, close_fds=True)
	out = proc.communicate()[0]
	msg = yaml.load(out)

	assert msg["body"]["status"] == "error", msg["body"]["status"]
	assert msg["body"]["last_error"] == "Canceled", msg["body"]["last_error"]

