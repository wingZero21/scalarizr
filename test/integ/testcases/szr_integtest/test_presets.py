'''
Created on Oct 25, 2010

@author: Dmytro Korsakov
'''
import unittest
import logging
import time
from szr_integtest import get_selenium, config, MutableLogFile
from szr_integtest_libs import exec_command
from szr_integtest_libs.scalrctl import login
from scalarizr.util import wait_until

class PresetConfigurator:
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._logger.info('Preparing Selenium')
		self.sel = get_selenium()
		self._logger.info('Logging in on Scalr')
		login(self.sel)
		self.main_page = self.sel.browserURL
		self._logger.info('Now we on Scalr main page %s' % self.main_page)
		self.presets_page = '/service_config_presets.php'
		
	def _change_opened_preset(self, preset_name, behaviour='mysql', settings=None, apply=True):
		btn_save = '//button[text()="Save"]'
		wait_until(lambda: self.sel.is_element_present(btn_save) == True, sleep=1.5)
		if settings: 
			for var, value in settings.items():
				name = 'var[%s]' % var
				if self.sel.is_element_present(name):
					self._logger.info('%s set to %s in Preset %s' % (var, value, preset_name))
					self.sel.type(name, value)
				else:
					self._logger.warning('%s does not exist in preset`s Manifest')
		if apply:
			self.sel.click(btn_save)
			self._logger.info("%s preset '%s' saved." % (behaviour, preset_name))
	
	def new_preset(self, preset_name, behaviour='mysql', settings=None, apply=True):
		self._logger.info("Going to apache_vhost_add.php")
		self.sel.open('/service_config_preset_add.php')	
		self._logger.info("Trying to create %s preset '%s'" % (behaviour, preset_name))
		self.sel.type('role_behavior', behaviour)
		self.sel.type('name', preset_name)
		self.sel.click('//button[text()="Continue"]')
		self._logger.info("Configuring new preset")
		self._change_opened_preset(preset_name, behaviour, settings, apply)
			
	def edit_preset(self, preset_name, behaviour='mysql', settings=None, apply=True, force=True):
		preset_id = self.get_preset_id(preset_name)
		self._logger.info('preset_id: %s' % preset_id) 
		if preset_id:
			edit_page = '%sservice_config_preset_add.php?preset_id=%s' % (self.main_page, preset_id)
			self.sel.open(edit_page)
			self._change_opened_preset(preset_name, behaviour, settings, apply)	
		elif force:
			self.new_preset(preset_name, behaviour, settings, apply)
		
	
	def delete_preset(self, preset_name, behaviour):
		self.sel.open(self.presets_page)
		try:
			wait_until(lambda:
					self.sel.is_element_present('//em[text()="%s"]' % preset_name) == True,
					sleep=1, 
					time_until=3)
		except BaseException:
			return
		
	
	def get_preset_id(self, preset_name):
		#TODO: use behaviour, Luke. Sometimes even names can be equal.
		if not self.sel.browserURL.endswith(self.presets_page):
			self.sel.open(self.presets_page)
		try:
			wait_until(lambda: 
					self.sel.is_element_present('//em[text()="%s"]' % preset_name) == True, 
					sleep=1, 
					time_until=3)
		except BaseException:
			return None
		return str(self.sel.get_text('//em[text()="%s"]/../../dt[1]/em' % preset_name))

class Test(unittest.TestCase):


	def setUp(self):
		pass


	def tearDown(self):
		pass


	def test_new_preset(self):
		pc = PresetConfigurator()
		#pc.edit_preset('test-preset', behaviour='app', settings = {'KeepAliveTimeout':'6'}, apply=False)
		#print pc.get_preset_id('test-preset')
		pc.delete_preset('test-preset', behaviour='app')


if __name__ == "__main__":
	unittest.main()		
	#from szr_integtest_libs.scalrctl import EC2_ROLE_DEFAULT_SETTINGS
	#role_opts = EC2_ROLE_DEFAULT_SETTINGS.copy()
	#role_opts['aws.servicesconfig.fieldset.app'] = 'test1'