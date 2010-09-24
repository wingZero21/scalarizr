'''
Created on Sep 22, 2010

@author: marat
'''
import unittest
from szr_integtest import get_selenium, config

class TestLogin(unittest.TestCase):

	def test_login(self):
		sel = get_selenium()
		sel.open('/')
		sel.click('//div[@class="login-trigger"]/a')
		sel.type('login', config.get('general', 'scalr_net_login'))
		sel.type('pass', config.get('general', 'scalr_net_password'))
		sel.click('//form/button')
		sel.wait_for_page_to_load(30000)
		self.assertTrue(sel.get_location().find('/client_dashboard.php') > -1)

if __name__ == "__main__":
	unittest.main()