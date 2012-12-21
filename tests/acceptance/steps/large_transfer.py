__author__ = 'vladimir'


import shutil
import tempfile
import os
import subprocess
import json
import logging
import hashlib
from copy import copy
from io import BytesIO

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


@before.all
def global_setup():
	subprocess.Popen(["strace", "-T", "-t", "-f", "-q", "-o", "strace_latest",
					  "-p", str(os.getpid())], close_fds=True)


@before.each_scenario
def setup(scenario):
	world.basedir = tempfile.mkdtemp()
	world.sources = []
	world.destination = None
	world.driver = None
	world.result_chunks = []


@after.each_scenario
def teardown(scenario):
	shutil.rmtree(world.basedir)


def md5(name):
	if os.path.isfile(name):
		out = subprocess.Popen(["md5sum", name], stdout=subprocess.PIPE,
			close_fds=True).communicate()
		return out[0].split()[0]
	elif os.path.isdir(name):
		dir_md5 = []
		for location, dirs, files in os.walk(name):
			files_md5 = map(lambda x: (x, md5(os.path.join(location, x))), files)
			rel_loc = location.replace(name, ".", 1)

			dir_md5.append((rel_loc, dirs, files_md5))
		return hashlib.md5(str(dir_md5)).hexdigest()


def make_file(name, size):
	subprocess.call([
		"dd",
		"if=/dev/urandom",
		"of=%s" % name,
		"bs=1M",
		"count=%s" % size
	], stdout=open('/dev/null', 'w'), stderr=subprocess.STDOUT, close_fds=True)

	return md5(name)


def make_stream(name, size):
	out = subprocess.Popen([
		"dd",
		"if=/dev/urandom",
		"bs=1M",
		"count=%s" % size
	], stdout=subprocess.PIPE, stderr=open('/dev/null', 'w'),
		close_fds=True).communicate()

	md5sum = hashlib.md5(out[0])
	stream = BytesIO(out[0])
	stream.name = name

	return stream, md5sum


@step("Initialize upload variables")
def initialize_upload_variables(step):
	world.manifest_url = None
	world.items = {}


@step("I have a (\d+) megabytes file (\w+)")
def i_have_a_file(step, megabytes, filename):
	filename = os.path.join(world.basedir, filename)
	world.sources.append(filename)

	f_md5 = make_file(filename, megabytes)
	world.items[os.path.basename(filename)] = f_md5


@step("I upload it to ([^\s]+) with gzipping")
def i_upload_it_with_gzipping(step, storage):
	world.destination = STORAGES[storage]["url"]
	world.driver = STORAGES[storage]["driver"]()
	world.manifest_url = LargeTransfer(world.sources[0], world.destination, "upload",
		gzip_it=True).run()


@step("I upload multiple sources to ([^\s]+) with gzipping")
def i_upload_multiple_sources_with_gzipping(step, storage):
	world.destination = STORAGES[storage]["url"]
	world.driver = STORAGES[storage]["driver"]()

	def src_gen(sources=copy(world.sources)):
		for src in sources:
			yield src

	world.manifest_url = LargeTransfer(src_gen(), world.destination, "upload",
		gzip_it=True).run()


@step("I expect manifest as a result")
def i_expect_manifest_as_a_result(step):
	local_path = world.driver.get(world.manifest_url, world.basedir)

	with open(local_path) as fd:
		data = json.loads(fd.read())

	for file in data["files"]:
		world.result_chunks.extend(file["chunks"])


@step("all chunks are uploaded")
def all_chunks_are_uploaded(step):
	for chunk, md5sum in world.result_chunks:
		assert world.driver.exists(os.path.join(os.path.dirname(world.manifest_url),
			chunk))


@step("I have a dir (\w+/?) with (\d+) megabytes file (\w+), with (\d+) megabytes file (\w+)")
def i_have_dir_with_files(step, dirname, f1_size, f1_name, f2_size, f2_name):
	dirname = os.path.join(world.basedir, dirname)
	world.sources.append(dirname)

	os.mkdir(dirname)
	f1_md5 = make_file(os.path.join(dirname, f1_name), f1_size)
	f2_md5 = make_file(os.path.join(dirname, f2_name), f2_size)

	if dirname.endswith('/'):
		world.items[f1_name] = f1_md5
		world.items[f2_name] = f2_md5
	else:
		world.items[os.path.basename(dirname)] = md5(dirname)


@step("I have a list with (\d+) megabytes stream (\w+), with (\d+) megabytes stream (\w+)")
def i_have_list_of_streams(step, s1_size, s1_name, s2_size, s2_name):
	for name, size in [(s1_name, s1_size), (s2_name, s2_size)]:
		abs_path = os.path.join(world.basedir, name)
		stream_md5 = make_file(abs_path, size)
		stream = open(abs_path, 'rb')

		world.sources.append(stream)
		world.items[os.path.basename(stream.name)] = stream_md5


@step("I have info from the previous upload")
def i_have_info_from_previous_upload(step):
	assert world.manifest_url
	assert world.items


@step("I download with the manifest")
def i_download_with_the_manifest(step):
	LargeTransfer(world.manifest_url, world.basedir, "download").run()


@step("I expect original items downloaded")
def i_expect_original_items_downloaded(step):
	for file, md5sum in world.items.iteritems():
		file_loc = os.path.join(world.basedir, file)
		assert os.path.exists(file_loc), file_loc
		assert md5sum == md5(file_loc), file_loc

