'''
Created on Jun 19, 2012

@author: marat
'''

from scalarizr.api import storage as storage_api
from scalarizr import storage as storage_lib
from scalarizr import handlers
import threading

import mock
from nose.tools import assert_equal, assert_true

class ThreadMock(mock.Mock):
	def start(self):
		ThreadMock.call_args[1]['target']()

class TestStorageAPI(object):
	
	@classmethod
	def setup_class(cls):
		storage_lib.Storage = mock.Mock()
		handlers.operation = mock.MagicMock()
		threading.Thread = ThreadMock
	
	def setup(self):
		self.api = storage_api.StorageAPI()
		self.sample_vol = storage_lib.Volume('/dev/xvde1', '/mnt/services', 'ext4', 'base')

	
	def test_create_async(self):
		
		storage_lib.Storage.create.return_value = self.sample_vol
		ret = self.api.create(storage_config={'type': 'ebs', 'size': 1}, async=True)
		assert_true(ret)
		
		
		