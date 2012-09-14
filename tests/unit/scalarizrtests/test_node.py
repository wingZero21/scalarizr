import os

import mock

from scalarizr import node
from nose.tools import raises


class TestCompound(object):
	def test_plain_key(self):
		store = mock.MagicMock(spec=node.Store)
		store.__len__.return_value = 1
		store.__getitem__.return_value = 'aaa'

		master = node.Compound({'plain_key': store})

		assert master['plain_key'] == 'aaa'
		store.__getitem__.assert_called_with('plain_key')

	
	def test_re_key(self):
		store = {
			'root_password': 'qqq',
			'stat_password': 'ppp'
		}		

		master = node.Compound({'*_password': store})

		assert master['root_password'] == 'qqq'
		assert master['stat_password'] == 'ppp'
		try:
			master['undefined_password']
			assert 0, 'Expected KeyError'
		except KeyError:
			pass


	def test_enum_key(self):
		values = {
			'server_id': '14593',
			'platform': 'ec2'
		}
		def getitem(key):
			return values[key]
		store = mock.MagicMock(spec=node.Store)
		store.__len__.return_value = len(values)
		store.__getitem__.side_effect = getitem
		
		master = node.Compound({'server_id,platform': store})

		assert master['server_id'] == '14593'
		assert master['platform'] == 'ec2'


	def test_set_undefined_key(self):
		master = node.Compound()

		master['key1'] = 'ooo'
		assert master['key1'] == 'ooo'
		
		
class TestJson(object):
	def setup(self):
		filename = os.path.dirname(__file__) + '/../fixtures/node.json'
		self.store = node.Json(filename, mock.Mock())

	
	def test_get(self):
		val = self.store['any_key']
		
		assert val
		self.store.fn.assert_called_with(
				type='eph', 
				id='eph-vol-592f4b8c', 
				size='80%')

	
	@mock.patch('__builtin__.open')
	@mock.patch('json.dump')
	def test_set_dict(self, json, open):
		data = {'type': 'lvm', 'vg': 'mysql'}

		self.store['any_key'] = data
		
		json.assert_called_with(mock.ANY, data)
		open.assert_called_with(self.store.filename, mock.ANY)
			
	
	@mock.patch('__builtin__.open')
	@mock.patch('json.dump')
	def test_set_object(self, json, open):
		class _Data(object):
			def __init__(self, data):
				self.data = data
			def __json__(self):
				return self.data
		
		data = {'type': 'lvm', 'vg': 'mysql'}
		self.store['any_key'] = _Data(data)
		
		json.assert_called_with(mock.ANY, data)
		open.assert_called_with(self.store.filename, mock.ANY)


class TestIni(object):
	def setup(self):
		filename = os.path.dirname(__file__) + '/../fixtures/node.ini'
		self.store = node.Ini(filename, 'mysql')

	
	def test_get(self):
		assert self.store['root_password'] == 'Q9OgJxYf19ygFHpRprLF'
	
	
	@raises(KeyError)
	def test_get_nosection(self):
		self.store.section = 'undefined'
		self.store['log_file']
	
	@raises(KeyError)
	def test_get_nooption(self):
		self.store['undefined_option']
	
	@mock.patch('__builtin__.open')
	def test_set(self, open):
		with mock.patch.object(self.store, '_reload') as reload:
			self.store.ini = mock.Mock()
			self.store['new_option'] = 1
			self.store.ini.set.assert_called_with(self.store.section, 'new_option', '1')
			assert self.store.ini.write.call_count == 1
