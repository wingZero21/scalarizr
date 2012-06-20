'''
Created on Jun 19, 2012

@author: marat
'''

from scalarizr.api import storage as storage_api
from scalarizr.api.storage import threading
from scalarizr import storage as storage_lib
from scalarizr import handlers

import mock
from nose.tools import assert_equal, assert_true


class TestStorageAPI(object):
	
	@classmethod
	def setup_class(cls):
		threading.Thread = mock.MagicMock()

		def thread_side_effect(*args, **kwargs):
			target = kwargs.get('target')
			ret = mock.MagicMock()
			ret.start.side_effect = lambda: target()
			return ret

		threading.Thread.side_effect = thread_side_effect

	
	def setup(self):
		storage_lib.Storage = mock.MagicMock()
		handlers.operation = mock.MagicMock()
		self.api = storage_api.StorageAPI()
		self.sample_vol = storage_lib.Volume('/dev/xvde1', '/mnt/services', 'ext4', 'base')

	
	def test_create_async(self):
		storage_lib.Storage.create.return_value = self.sample_vol
		self.api.create(volume_config={'type': 'ebs', 'size': 1}, async=True)
		handlers.operation.assert_called_once_with(name='Create volume')



		
		