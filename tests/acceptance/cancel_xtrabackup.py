"""
Run on percona-lvm instance.

Uses $AWS_ACCESS_KEY_ID, $AWS_SECRET_ACCESS_KEY, $AWS_ACCOUNT_ID
"""
__author__ = 'vladimir'

import subprocess
import time
import os

from lettuce import before, after, world, step
import yaml


FEATURE = "Cancel xtrabackup"


def send_message(action):
	if action == "create":
		name = "DbMsr_CreateDataBundle"
	elif action == "cancel":
		name = "DbMsr_CancelDataBundle"
	else:
		return

	msg = '<?xml version="1.0"?>\n' \
		  '<message id="d2d2251c-8f65-4175-a131-fc5b99b609e6" name="%s"><meta>' \
		  '<scalr_version>4.1.0</scalr_version></meta><body><scripts/><percona>' \
		  '<backup><type>xtrabackup</type><compressor>gzip</compressor>' \
		  '<backup_type>full</backup_type><cloudfs_target>s3://scalr-ab6d8171' \
		  '-3414-us-east-1/data-bundles/12416/percona/</cloudfs_target></backup>' \
		  '</percona><storage_type>lvm</storage_type><platform_access_data>' \
		  '<account_id>%s</account_id><key_id>%s</key_id><key>%s</key>' \
		  '</platform_access_data></body></message>'
	msg = msg % (name, os.environ["AWS_ACCOUNT_ID"],
				 os.environ["AWS_ACCESS_KEY_ID"],
				 os.environ["AWS_SECRET_ACCESS_KEY"])

	path = "/tmp/msg.xml"
	with open(path, 'w') as f:
		f.write(msg)

	subprocess.call([
		"szradm",
		"-m",
		"-e",
		"http://localhost:8013",
		"-o",
		"control",
		"-n",
		name,
		"-f",
		path,
	], close_fds=True)

	os.remove(path)


def list_messages():
	proc = subprocess.Popen([
		"szradm",
		"list-messages",
		"-n",
		"DbMsr_CreateDataBundleResult",
	], stdout=subprocess.PIPE, close_fds=True)
	out = proc.communicate()[0]

	if not out:
		return set()

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
	send_message("create")


@step("I wait for (\d+) seconds")
def i_wait_for_seconds(step, seconds):
	time.sleep(int(seconds))


@step("I send CancelDataBundle message")
def i_send_cancel_data_bundle_message(step):
	send_message("cancel")


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

