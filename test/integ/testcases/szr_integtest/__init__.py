from ConfigParser import ConfigParser
from optparse import OptionParser
from selenium import selenium
from multiprocessing import Process
from scalarizr.libs.metaconf import NoPathError, Configuration
import time
import os
import signal
import sys
import paramiko
import logging
import re
import json
from threading import Thread, Lock, Event
from Queue import Queue, Empty
from szr_integtest_libs import exec_command

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
		user_config.write(_user_ini_path)
	return _sel


class LogReader:
	
	_exception = None
	_error     = ''
	
	def __init__(self, queue):
		self.err_re = re.compile('^\d+-\d+-\d+\s+\d+:\d+:\d+,\d+\s+-\s+ERROR\s+-\s+.+$', re.M)
		self.traceback_re = re.compile('Traceback.+')
		self.queue = queue 

	def expect(self, regexp, timeframe):
		self._error = ''
		self.ret = None
		break_tail = Event()

		t = Thread(target =self.reader_thread, args=(regexp, break_tail))
		t.start()
		
		start_time = time.time()
		
		while time.time() - start_time < timeframe:
			time.sleep(0.1)
			if break_tail.is_set():
				if self._error:
					raise Exception('Error detected: %s' % self._error)
				if self.ret:
					return self.ret
				else:
					raise Exception('Something bad happened')
		else:
			break_tail.set()
			raise Exception('Timeout after %s.' % timeframe)				

	def reader_thread(self, regexp, break_tail):
		search_re = re.compile(regexp) if type(regexp) == str else regexp
		while not break_tail.is_set():
			
			try:
				log_line = self.queue.get(False)
			except Empty:
				continue
			
			
			error = re.search(self.err_re, log_line)
			if error:
				# Detect if traceback presents
				try:
					traceback = self.queue.get(False)					
				
					if re.search(self.traceback_re, traceback):
						while True:
							newline = self.queue.get(False)
							if not newline.startswith(' '):
									break
							traceback += '\n' + newline
						self._error = error.group(0) + traceback
					else:
						self._error = error.group(0)
				except Empty:
					self._error = error.group(0)
				break_tail.set()
				break
			
			
			matched = re.search(search_re, log_line)
			if matched:
				self.ret = matched
				break_tail.set()
				break
			

class MutableLogFile:
	_logger = None
	_queues = None
	_channel = None
	_log_file = None
	_whole_log = None
	_lock = None	
	_reader = None
	_reader_started = False
	
	def __init__(self, channel, log_file='/var/log/scalarizr.log'):
		if channel.closed:
			raise Exception('Channel is closed')
		self._channel = channel
		self._log_file = log_file
		self._queues = []
		self._lock = Lock()
		self._whole_log = []
		self._reader = Thread(target=self._read, name='Log file %s reader' % self._log_file)
		self._reader.setDaemon(True)
		self._logger = logging.getLogger(__name__ + '.MutableLogFile')
	
	def _start_reader(self):
		self._reader.start()
		self._reader_started = True	
	
	def _read(self):
		# Clean channel
		self._logger.debug('Clean channel')
		while self._channel.recv_ready():
			self._channel.recv(1)
			
		# Wait when log file will be available
		self._logger.debug('Wait when log file will be available')
		while True:
			if exec_command(self._channel, 'ls -la %s 2>/dev/null' % self._log_file):
				break
			
		# Open log file
		self._logger.debug('Open log file')
		cmd = 'tail -f -n +0 %s\n' % self._log_file
		self._channel.send(cmd)
		time.sleep(0.3)
		self._channel.recv(len(cmd))
		
		# Read log file
		self._logger.debug('Entering read log file loop')
		line = ''
		while True:
			# Receive line
			#self._logger.debug('Receiving line')
			while self._channel.recv_ready():
				char = self._channel.recv(1)
				line += char
				if char == '\n':
					self._logger.debug('Received: %s', line)
					self._lock.acquire()			
					self._whole_log.append(line[:-1])
					self._lock.release()
					for queue in self._queues:
						queue.put(line[:-1])
					line = ''

					break

	
	def head(self, skip_lines=0):
		if not self._reader_started:
			self._start_reader()

		queue = Queue()
		if len(self._whole_log) < skip_lines:
			raise Exception("Cannot skip %s lines after log's head: there is no such amount of lines in log" % skip_lines)
		
		for line in self._whole_log[skip_lines:]:
			queue.put(line)
					
		self._queues.append(queue)

		return LogReader(queue)
	
	def tail(self, lines=0):
		if not self._reader_started:
			self._start_reader()
		
		if len(self._whole_log) < lines:
			raise Exception("Cannot get %s lines before log's tail: there is no such amount of lines in log" % lines)
		
		queue = Queue()	
		for line in self._whole_log[-lines:]:
			queue.put(line)
			
		self._queues.append(queue)
		return LogReader(queue)
	
	def detach(self, queue):
		if queue in self._queues:
			self._queues.remove(queue)


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