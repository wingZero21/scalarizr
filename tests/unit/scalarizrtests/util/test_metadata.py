
import os
import types
from scalarizr.util import metadata

from nose.tools import eq_, ok_, raises


FIXTURE_PATH = os.path.abspath(os.path.dirname(__file__) + '../../../fixtures/util')

def test_parse_user_data():
	user_data = open(FIXTURE_PATH + '/user-data').read()
	user_data = metadata.parse_user_data(user_data)
	eq_(type(user_data), types.DictType)
	eq_(user_data['platform'], 'ec2')

def test_parse_user_data_error():
	user_data = open(FIXTURE_PATH + '/user-data.error').read()
	user_data = metadata.parse_user_data(user_data)
	ok_(not user_data)

class TestUrlMeta(object):
	def test_get_item(self):
		pass


class TestFileMeta(object):
	def test_user_data(self):
		meta = metadata.FileMeta(FIXTURE_PATH + '/user-data')
		user_data = meta.user_data()
		eq_(user_data['server_index'], '1')


class TestCloudStackMeta(object):
	def test_dhcp_server_identifier(self):
		leases_pattern = FIXTURE_PATH + '/dhc*.eth0.leases'
		router_host = metadata.CloudStackMeta.dhcp_server_identifier(leases_pattern)
		eq_(router_host, '10.2.1.45')
