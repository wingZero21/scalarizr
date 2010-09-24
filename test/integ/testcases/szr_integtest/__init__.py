from ConfigParser import ConfigParser
from selenium import selenium
from multiprocessing import Process
import time
import os
import signal
import sys

BASE_PATH = os.path.join(os.path.dirname(__file__), '..' + os.path.sep + '..' + os.path.sep)
RESOURCE_PATH = os.path.join(BASE_PATH, 'resources')
OPT_SESSION_ID = 'session_id'

config = ConfigParser()
config.read(os.path.join(RESOURCE_PATH, 'integ_test.ini'))
_user_ini = os.path.expanduser('~/.scalr-dev/integ_test.ini')
if os.path.exists(_user_ini):
	config.read(_user_ini)
	

_sel_started = False

_sel = selenium(
		config.get('general', 'selenium_rc_host'), 
		config.get('general', 'selenium_rc_port'), 
		'*firefox', 
		config.get('general', 'scalr_net_url')
		)

		
def check_windows(_sel):
	try:
		_sel.get_all_window_titles()
	except:
		sys.exit(-1)	

def get_selenium():
	try:
		session_id = config.get('general', OPT_SESSION_ID)
		try:
			_sel.sessionId = unicode(session_id)
			t = Process(target=check_windows, args=(_sel,))
			t.start()
			start_time = time.time()
			while (time.time() - start_time < 3):
				time.sleep(0.1)
				if not t.is_alive():
					if t.exitcode == 0:
						break
					else:
						raise BaseException()
			else:
				os.kill(t.pid, signal.SIGKILL)
				raise BaseException('timeout')
			_sel.delete_all_visible_cookies()
			globals()['_sel_started'] = True
		except:
			_sel.stop()
			globals()['_sel_started'] = False
	except:
		globals()['_sel_started'] = False
			
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
