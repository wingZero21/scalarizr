__author__ = 'vladimir'


import shutil
import tempfile
import os
import subprocess
import json
import logging

from lettuce import step, world, before, after
from boto import connect_s3

from scalarizr.storage2.cloudfs import s3, LargeTransfer, LOG


LOG.setLevel(logging.DEBUG)
LOG.addHandler(logging.FileHandler("transfer_test.log", 'w'))


# make s3 connection work
with open(os.path.dirname(__file__) +"/../fixtures/aws_keys.json") as fd:
	data = json.loads(fd.read())
ACCESS_KEY = data["ACCESS_KEY"]
SECRET_KEY = data["SECRET_KEY"]
del data
s3.S3FileSystem._get_connection = lambda x: connect_s3(ACCESS_KEY, SECRET_KEY)


class S3(object):

	def __init__(self):
		self.driver = s3.S3FileSystem()

	def exists(self, remote_path):
		parent = os.path.dirname(remote_path.rstrip('/'))
		ls = self.driver.ls(parent)
		return remote_path in ls

	def __getattr__(self, name):
		return getattr(self.driver, name)


STORAGES = {
	"s3": {
		"url": "s3://scalr.test_bucket/vova_test",
		"driver": S3,
	}
}


@before.each_scenario
def setup(scenario):
	world.basedir = tempfile.mkdtemp()
	world.source = None
	world.storage = None
	world.destination = None
	world.driver = None
	world.result = None
	world.result_chunks = []


@after.each_scenario
def teardown(scenario):
	shutil.rmtree(world.basedir)


def make_file(name, size):
	subprocess.call([
		"dd",
		"if=/dev/urandom",
		"of=%s" % name,
		"bs=1M",
		"count=%s" % size
	])


@step("I have a (\d+) megabytes file (\w+)")
def i_have_a_file(step, megabytes, filename):
	world.source = os.path.join(world.basedir, filename)
	make_file(world.source, megabytes)


@step("I (upload|download) it to ([^\s]+) with gzipping")
def i_load_it_with_gzipping(step, direction, storage):
	world.destination = STORAGES[storage]["url"]
	world.driver = STORAGES[storage]["driver"]()
	world.result = LargeTransfer(world.source, world.destination, direction,
		gzip_it=True).run()


@step("I expect manifest as a result")
def i_expect_manifest_as_a_result(step):
	local_path = world.driver.get(world.result, world.basedir)

	with open(local_path) as fd:
		data = json.loads(fd.read())

	for file in data["files"]:
		world.result_chunks.extend(file["chunks"])


@step("all chunks are uploaded")
def all_chunks_are_uploaded(step):
	for chunk, md5sum in world.result_chunks:
		assert world.driver.exists(os.path.join(os.path.dirname(world.result),
			chunk))


@step("I have a dir (\w+) with (\d+) megabytes file (\w+), with (\d+) megabytes file (\w+)")
def i_have_dir_with_files(step, dirname, f1_size, f1_name, f2_size, f2_name):
	world.source = os.path.join(world.basedir, dirname)
	os.mkdir(world.source)

	make_file(os.path.join(world.source, f1_name), f1_size)
	make_file(os.path.join(world.source, f2_name), f2_size)




