__author__ = 'vladimir'


import shutil
import tempfile
import os
import subprocess
import json
import logging
import hashlib
import base64
import mock
from copy import copy
from ConfigParser import ConfigParser
from random import choice

from lettuce import step, world, before, after
from boto import connect_s3
import cloudfiles

from scalarizr.storage2.cloudfs import LargeTransfer, LOG
from scalarizr.storage2.cloudfs import s3, gcs, cf
from scalarizr.platform.gce import STORAGE_FULL_SCOPE, GoogleServiceManager


# Essential environment variables:
# $AWS_ACCESS_KEY_ID, $AWS_SECRET_ACCESS_KEY for s3
# $RS_USERNAME, $RS_API_KEY for cf
# default storage is s3; override with $LT_TEST_STORAGE


#
# Patches
#


# prevent ini parser from lowercasing params
ConfigParser.optionxform = lambda self, x: x

# make connections work
# s3
s3.S3FileSystem._get_connection = lambda self: connect_s3()
# gcs
def get_pk(f="gcs_pk.p12"):  # TODO:
	with open(f, "rb") as fd:
		pk = fd.read()
	return base64.b64encode(pk)

ACCESS_DATA = {
	"service_account_name": '876103924605@developer.gserviceaccount.com',
	"key": get_pk(),
}

gcs.bus = mock.MagicMock()
gcs.bus.platform.get_access_data = lambda k: ACCESS_DATA[k]

gsm = GoogleServiceManager(gcs.bus.platform,
	"storage", "v1beta1", STORAGE_FULL_SCOPE)

gcs.bus.platform.get_numeric_project_id.return_value = '876103924605'
gcs.bus.platform.new_storage_client = lambda: gsm.get_service()
# cf
cf.CFFileSystem._get_connection = lambda self: cloudfiles.Connection(
		os.environ["RS_USERNAME"], os.environ["RS_API_KEY"])


class S3(s3.S3FileSystem):

	def exists(self, remote_path):
		parent = os.path.dirname(remote_path.rstrip('/'))
		ls = self.ls(parent)
		return remote_path in ls


class GCS(gcs.GCSFileSystem):

	def exists(self, remote_path):
		parent = os.path.dirname(remote_path.rstrip('/'))
		ls = self.ls(parent)
		return remote_path in ls


class CF(cf.CFFileSystem):

	def exists(self, remote_path):
		parent = os.path.dirname(remote_path.rstrip('/'))
		ls = self.ls(parent)
		return remote_path in ls


#
# Logging
#

LOG.setLevel(logging.DEBUG)
LOG.addHandler(logging.FileHandler("transfer_test.log", 'w'))


"""
@before.all
def global_setup():
	subprocess.Popen(["strace", "-T", "-t", "-f", "-q", "-o", "strace_latest",
					  "-p", str(os.getpid())], close_fds=True)
"""

#
#
#


STORAGES = {
	"s3": {
		"url": "s3://scalr.test_bucket/vova_test",
		"driver": S3,
	},
	"gcs": {
		"url": "gcs://vova-test",
		"driver": GCS,
	},
	"cf": {
		"url": "cf://vova-test",
		"driver": CF,
	},
}

STORAGE = "s3"
if "LT_TEST_STORAGE" in os.environ:
	STORAGE = os.environ["LT_TEST_STORAGE"]
assert STORAGE in STORAGES, "%s not in %s" % (STORAGE, STORAGES.keys())


def convert_manifest(json_manifest):
	assert len(json_manifest["files"]) == 1
	assert json_manifest["files"][0]["compressor"] == "gzip"

	parser = ConfigParser()
	parser.add_section("snapshot")
	parser.add_section("chunks")

	parser.set("snapshot", "description", json_manifest["description"])
	parser.set("snapshot", "created_at", json_manifest["created_at"])
	parser.set("snapshot", "pack_method", json_manifest["files"][0]["compressor"])

	for chunk, md5sum in reversed(json_manifest["files"][0]["chunks"]):
		parser.set("chunks", chunk, md5sum)

	LOG.debug("CONVERT: %s" % parser.items("chunks"))
	return parser


def release_local_data():
	"""
	Delete everything from the basedir and return contents of the manifest.
	"""

	# relies on "I expect manifest as a result" step and
	# LargeTransfer downloading manifest as "manifest.json"
	with open(os.path.join(world.basedir, "manifest.json")) as fd:
		manifest = json.loads(fd.read())

	subprocess.call(["rm -r %s/*" % world.basedir], shell=True)

	return manifest


@before.each_scenario
def setup(scenario):
	world.basedir = tempfile.mkdtemp()
	world.sources = []
	world.destination = None
	world.driver = None
	world.result_chunks = []
	world.dl_result = {
		'completed': [],
		'failed': [],
		'multipart_result': None,
	}
	world.deleted_chunk = None


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


@step("I upload it to Storage with gzipping")
def i_upload_it_with_gzipping(step):
	world.destination = STORAGES[STORAGE]["url"]
	world.driver = STORAGES[STORAGE]["driver"]()
	world.manifest_url = LargeTransfer(world.sources[0], world.destination).run().cloudfs_path



@step("I upload multiple sources to Storage with gzipping")
def i_upload_multiple_sources_with_gzipping(step):
	world.destination = STORAGES[STORAGE]["url"]
	world.driver = STORAGES[STORAGE]["driver"]()

	def src_gen(sources=copy(world.sources)):
		for src in sources:
			yield src

	world.manifest_url = LargeTransfer(src_gen(), world.destination).run().cloudfs_path


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
	world.dl_result = LargeTransfer(world.manifest_url, world.basedir).run()


@step("I expect original items downloaded")
def i_expect_original_items_downloaded(step):
	for file, md5sum in world.items.iteritems():
		file_loc = os.path.join(world.basedir, file)
		assert os.path.exists(file_loc), file_loc
		assert md5sum == md5(file_loc), file_loc


@step("I clear the tempdir and replace the manifest with it's old representation")
def i_replace_the_manifest_with_old_repr(step):
	manifest = release_local_data()

	manifest_ini_path = os.path.join(world.basedir, "manifest.ini")
	with open(manifest_ini_path, 'w') as fd:
		convert_manifest(manifest).write(fd)

	world.driver.delete(world.manifest_url)

	world.manifest_url = world.driver.put(manifest_ini_path,
		os.path.join(os.path.dirname(world.manifest_url), ''))
	LOG.debug("NEW %s" % world.manifest_url)


@step("I delete one of the chunks")
def i_delete_one_of_the_chunks(step):
	manifest = release_local_data()
	remote_dir, manifest_name = os.path.split(world.manifest_url)

	chunk, md5sum = choice(choice(manifest["files"])["chunks"])

	chunk_url = os.path.join(remote_dir, chunk)
	world.deleted_chunk = chunk_url
	LOG.debug("Lettuce deleting %s" % chunk_url)

	world.driver.delete(chunk_url)


@step("I expect failed list returned")
def i_expect_failed_list_returned(step):
	# Unfortunately, this doesn't test if transfer gets killed right away
	assert world.deleted_chunk in world.dl_result["failed"][0]["src"], world.deleted_chunk
	assert len(world.dl_result["failed"]) == 1


@step("I have a (\d+) megabytes stream (\w+)")
def i_have_a_stream(step, megabytes, name):
	abs_path = os.path.join(world.basedir, name)
	stream_md5 = make_file(abs_path, megabytes)
	stream = open(abs_path, 'rb')

	world.sources.append(stream)
	world.items[os.path.basename(stream.name)] = stream_md5


@step("I upload it to Storage with intentional interrupt")
def i_upload_it_with_intentional_interrupt(step):
	world.destination = STORAGES[STORAGE]["url"]
	world.driver = STORAGES[STORAGE]["driver"]()

	lt = LargeTransfer(world.sources[0], world.destination, chunk_size=20, num_workers=2)
	lt.on(transfer_complete=lambda *args: lt.kill())
	lt.run()

	world.manifest_url = os.path.join(world.destination, lt.transfer_id)


@step("I expect cloud path cleaned")
def i_expect_path_clean(step):
	assert not world.driver.ls(world.manifest_url)



