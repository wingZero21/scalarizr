from ConfigParser import ConfigParser
from optparse import OptionParser
from selenium import selenium
from multiprocessing import Process
from scalarizr.libs.metaconf import NoPathError, Configuration
import time
import os
import signal
import sys
import logging

BASE_PATH = os.path.join(os.path.dirname(__file__), '..' + os.path.sep + '..')
RESOURCE_PATH = os.path.join(BASE_PATH, 'resources')
OPT_SESSION_ID = 'session_id'

logging.basicConfig(
		format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", 
		stream=sys.stdout, 
		level=logging.INFO)

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
			#_sel.delete_all_visible_cookies()
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
		user_config.write(_user_ini_path)
	_sel._logged_in = False
	return _sel


'''			
class Ec2TestAmis:
	UBUNTU_1004_EBS = 'ami-714ba518'
	UBUNTU_1004_IS  = 'ami-2d4aa444'
	UBUNTU_804_EBS  = 'ami-cb8d61a2'
	UBUNTU_804_IS   = 'ami-59b35f30'
	CENTOS_5_EBS    = ''
	CENTOS_5_IS     = ''
	
	UNUBTU_1010_EBS = ''
	UBUNTU_1010_IS  = ''


class ResourceManagerFactory:
	managers={}
	
	@staticmethod
	def get_resource_manager():
		self = ResourceManagerFactory
		try:
			platform	= os.environ['platform']
			dist		= os.environ['dist']
		except:
			raise Exception("Can't get platform name from OS environment variables.")
		
		if not platform in self.managers:
			raise Exception('Unknown platform: %s' % platform)
		
		config_path = os.path.join(RESOURCE_PATH, platform + '.json')
		
		if not os.path.exists(config_path):
			raise Exception('Config file for platform "%s" does not exist.' % platform)
		
		raw_config = read_file(config_path)
		
		try:
			config = json.loads(raw_config)
		except:
			raise Exception('Config file for platform "%s" does not contain valid json configuration.')
		
		return self.managers['platform'](config)
	
	@staticmethod		
	def register_manager(pl_name, manager):
		self = ResourceManagerFactory
		self.managers[pl_name] = manager

class ResourceManager:
	platform = None
	
	def __init__(self, config):
		self._config = config
	def get_role_name(self, behaviour):
		pass	
	def get_image_id(self):
		pass
	def start_instance(self):
		pass
	def terminate_instance(self, inst_id):
		pass
	def get_ssh_manager(self, inst_id):
		pass
	
	
class Ec2ResourceManager(ResourceManager):
	platform = 'ec2'
	
ResourceManagerFactory.register_manager('ec2', Ec2ResourceManager)
'''

'''
	
class StartupTest(unittest.TestCase):
	target = None
	
	def test(self):
		server = self.target() if callable(self.target) else self.target
		reader = server.log.head()
		
		reader.expect("Message 'HostInit' delivered")
		server.scalr.exec_cronjob('ScalarizrMessaging')
		reader.expect("Message 'HostUp' delivered")

class BaseRoleSuite(unittest.TestSuite):
	def __init__(self):
		dp = DataProvider(behaviour='base') 

		t = StartupTest()
		t.target = dp.server
		self.addTest(t)
		
		# Reboot
		# Exec scripts
		# ...
		# ...
'''