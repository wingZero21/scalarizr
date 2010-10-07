from ConfigParser import ConfigParser
from selenium import selenium
from multiprocessing import Process
from scalarizr.libs.metaconf import NoPathError, Configuration
import time
import os
import signal
import sys
import paramiko
import logging

BASE_PATH = os.path.join(os.path.dirname(__file__), '..' + os.path.sep + '..')
RESOURCE_PATH = os.path.join(BASE_PATH, 'resources')
OPT_SESSION_ID = 'session_id'

logging.basicConfig(
		format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", 
		stream=sys.stdout, 
		level=logging.DEBUG)

config = Configuration('ini')
user_config = Configuration('ini')

config.read(os.path.join(RESOURCE_PATH, 'integ_test.ini'))

_user_ini_path = os.path.expanduser('~/.scalr-dev/integ_test.ini')

if os.path.exists(_user_ini_path):
	config.read(_user_ini_path)
	user_config.read(_user_ini_path)
else:
	basepath = os.path.dirname(_user_ini_path)
	if not os.path.isdir(basepath):
		os.makedirs(basepath)
	user_config.add('./selenium')


_sel_started = False

try:
	_sel = selenium(
			config.get('./selenium/host'), 
			config.get('./selenium/port'), 
			'*firefox',
			config.get('./scalr/url')
			)
	
except NoPathError:
	raise Exception("Configuration file doesn't contain all essential options")

		
def check_windows(_sel):
	try:
		_sel.get_all_window_titles()
	except:
		sys.exit(-1)

def get_selenium():
	try:
		session_id = config.get('./selenium/' + OPT_SESSION_ID)
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
		try:
			_sel.start()
		except (Exception, BaseException), e:
			raise Exception("Can't connect to selenium RC or start a session: %s" % e)
		globals()['_sel_started'] = True
		
		config.set('./selenium/' + OPT_SESSION_ID, _sel.sessionId, force = True)
		user_config.set('./selenium/' + OPT_SESSION_ID, _sel.sessionId, force = True)
		user_config.write(open(_user_ini_path, 'w'))
	return _sel


class Ec2TestAmis:
	UBUNTU_1004_EBS = 'ami-714ba518'
	UBUNTU_1004_IS  = 'ami-2d4aa444'
	UBUNTU_804_EBS  = 'ami-cb8d61a2'
	UBUNTU_804_IS   = 'ami-59b35f30'
	CENTOS_5_EBS    = ''
	CENTOS_5_IS     = ''
	
	UNUBTU_1010_EBS = ''
	UBUNTU_1010_IS  = ''
	
	