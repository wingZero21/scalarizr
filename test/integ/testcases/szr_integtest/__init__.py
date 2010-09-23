import os
from ConfigParser import ConfigParser
from selenium import selenium

BASE_PATH = os.path.join(os.path.dirname(__file__), '..' + os.path.sep + '..' + os.path.sep + '..')
RESOURCE_PATH = os.path.join(BASE_PATH, 'resources')


config = ConfigParser()
config.read(os.path.join(RESOURCE_PATH, 'integ_test.ini'))
_user_ini = os.path.expanduser('~/.scalr-dev/integ_test.ini')
if os.path.exists(_user_ini):
	config.read(_user_ini)
	
_sel = selenium(
	config.get('general', 'selenium_rc_host'), 
	config.get('general', 'selenium_rc_port'), 
	'*firefox', 
	config.get('general', 'scalr_net_url')
)
_sel_started = False

def get_selenium():
	if not _sel_started:
		_sel.start()
		globals()['_sel_started'] = True
	return _sel
