'''
Created on Sep 13, 2010

@author: shaitanich
'''
import unittest
from scalarizr.service import PresetType, CnfPreset, _CnfManifest, CnfController, CnfPresetStore, _OptionSpec
from scalarizr.bus import bus
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
		mock_preset_type = PresetType.DEFAULT
		mock_preset = CnfPreset(name=mock_name, settings=mock_settings)
		store = CnfPresetStore()
		store.save(mock_service, mock_preset, mock_preset_type)
		preset = store.load(service_name=mock_service, preset_type=mock_preset_type)
		self.assertEqual(mock_name, preset.name)
		self.assertEqual(mock_settings, preset.settings)
	
	def test__manifest(self):
		self.behaviour = 'www'
		C = CnfController(self.behaviour, '/etc/apache2/apache2.conf', 'apache', definitions={'1':'on','0':'off'})
		www = C._manifest

		for option in www:
			self.assertEqual(option.__class__, _OptionSpec)

if __name__ == "__main__":
	bus.scalr_url = 'http://scalr-dev.local.webta.net'
	init_tests()
	unittest.main()