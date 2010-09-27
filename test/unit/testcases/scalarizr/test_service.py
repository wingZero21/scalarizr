'''
Created on Sep 13, 2010

@author: shaitanich
'''
import unittest
from scalarizr.service import CnfPreset, CnfPresetStore, _CnfManifest, CnfController
from scalarizr.util import init_tests
import os

class Test(unittest.TestCase):


	def setUp(self):
		pass


	def tearDown(self):
		pass


	def _test_save_load(self):
		mock_name = 'mock1'
		mock_settings = {'test1':'1', 'test2':'2'}
		mock_service = 'mock'
		mock_preset_type = CnfPresetStore.PresetType.DEFAULT
		mock_preset = CnfPreset(name=mock_name, settings=mock_settings)
		store = CnfPresetStore()
		store.save(mock_service, mock_preset, mock_preset_type)
		preset = store.load(service_name=mock_service, preset_type=mock_preset_type)
		self.assertEqual(mock_name, preset.name)
		self.assertEqual(mock_settings, preset.settings)
		
	def test__CnfManifest(self):
		behaviour = 'www'
		C = CnfController()
		www = C._get_manifest(behaviour)
		options = _CnfManifest(www)
		for option in options:
			print option
		


if __name__ == "__main__":
	init_tests()
	unittest.main()