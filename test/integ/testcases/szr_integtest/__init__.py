import os
from ConfigParser import ConfigParser
from selenium import selenium
from threading import Thread, Timer
import time
import os
import signal
import sys

BASE_PATH = os.path.join(os.path.dirname(__file__), '..' + os.path.sep + '..' + os.path.sep + '..')
RESOURCE_PATH = os.path.join(BASE_PATH, 'resources')
OPT_SESSION_ID = 'session_id'

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
_opened      = False

def check_windows():
	t = Timer(2, lambda: sys.exit())
	t.start()
	try:
		_sel.get_all_window_titles()
	except:
		return
	globals()['_opened'] = True	

def get_selenium():
	try:
		session_id = config.get('general', OPT_SESSION_ID)
		_sel.sessionId = unicode(session_id)
		try:
			globals()['_opened'] = False
			t = Thread(target=check_windows)
			t.start()
			start_time = time.time()
			while (time.time() - start_time < 3):
				time.sleep(0.1)
				if globals()['_opened'] == True:
					break
			else:
				raise BaseException('timeout')
			
			_sel.delete_all_visible_cookies()
			globals()['_sel_started'] = True
		except e:
			_sel.stop()
			globals()['_sel_started'] = False
	except:
		pass
	
	if not _sel_started:
		_sel.start()
		globals()['_sel_started'] = True
		
	config.set('general', OPT_SESSION_ID, _sel.sessionId)
	fp = open(os.path.join(RESOURCE_PATH, 'integ_test.ini'), 'w')
	try:
		config.write(fp)
	finally:
		fp.close()
		
	return _sel
