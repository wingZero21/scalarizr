'''
Created on Oct 16, 2010

@author: marat
'''

from szr_integtest import get_selenium, config
from szr_integtest_libs import tail_log_channel, expect, SshManager, scalrctl,\
	LogReader, exec_command
from szr_integtest_libs.scalrctl import FarmUI, ScalrCtl
import logging
import re, os
import unittest
import time
from optparse import OptionParser
import szr_integtest
import pexpect
import threading


roles = {
	'test-base-euca-2' : ('x86_64', 'eucalyptus', 'centos')
}

server_id = None
server_ip = None
ssh = None
root_ssh_channel = None



'''
class TestLaunch(unittest.TestCase):
	role_name = None
	role_opts = None
	terminate = False
	
	_logger = None
	
	def __init__(self, methodName='runTest', **kwargs):
		unittest.TestCase.__init__(self, methodName)
		for k, v in kwargs:
			setattr(self, k, v)
	
	def setUp(self):
		self._logger = logging.getLogger(__name__)		
		self.scalr_ctl = ScalrCtl()
		self.farm = scalrctl.FarmUI(get_selenium())
	
	def test_it(self):
		global server_id, server_ip, ssh, root_ssh_channel
		
		
		# Launch farm
		self._logger.info('Launching farm')
		self.farm.use(self.farm_id)
		self.farm.remove_all_roles()
		self.farm.add_role(self.role_name, 1, 2, self.role_opts or {})
		self.farm.save()
		self.farm.launch()
		self._logger.info("Farm launched")
		
		# Scale farm
		out = self.scalr_ctl.exec_cronjob('Scaling')
		server_id = self.scalrctl.parse_scaled_up_server_id(out)
		self._logger.info("New server id: %s" % server_id)
		
		# Wait when cloud launches server
		server_ip = self.farm.get_public_ip(server_id)
		self._logger.info("New server's ip: %s" % server_ip)
		
		# Wait for ssh connection
		ssh = SshManager(self.ip, self.farm_key)
		ssh.connect()
		self._logger.info('Connected to instance')
		
		root_ssh_channel = ssh.get_root_ssh_channel()		
		
		# Temporary solution
#		self._logger.info("Deploying dev branch")
#		deployer = ScalarizrDeploy(self.ssh)
#		deployer.apply_changes_from_tarball()
#		del(deployer)		
#		self.ssh.close_all_channels()
#		

#
#		exec_command(channel, '/etc/init.d/scalarizr stop')
#		exec_command(channel, 'rm -f /etc/scalr/private.d/.state')
#		exec_command(channel, '/etc/init.d/scalarizr start')
#		time.sleep(2)
		
		
		
		
		tail_log_channel(channel)

	
		self.expect_sequence(channel, sequence)
			
		self._logger.info('>>> Role has been successfully initialized.')		
		pass
	
	def tearDown(self):
		if self.terminate:
			# TODO: terminate farm
			pass

'''

log_file = None

class MutableLogFile:
	_logger = None
	_fifos = None
	_channel = None
	_log_file = None
	_read_log_file = None
	_read_log_file_fd = None
	_lock = None	
	_reader = None
	_reader_started = False
	
	def __init__(self, channel, log_file='/var/log/scalarizr.log'):
		if channel.closed:
			raise Exception('Channel is closed')
		self._channel = channel
		self._log_file = log_file
		self._fifos = {}
		self._read_log_file = os.tmpnam()
		self._lock = threading.Lock()
		self._reader = threading.Thread(target=self._read, name='Log file %s reader' % self._log_file)
		self._reader.setDaemon(True)
		self._logger = logging.getLogger(__name__ + '.MutableLogFile')
	
	def _start_reader(self):
		self._read_log_file_fd = open(self._read_log_file, 'w')
		self._reader.start()		
	
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
		print self._channel.recv(len(cmd))
		
		# Read log file
		self._logger.debug('Entering read log file loop')
		while True:
			# Receive line
			self._logger.debug('Receiving line')
			line = ''
			while self._channel.recv_ready():
				char = self._channel.recv(1)
				line += char
				if char == '\n':
					self._logger.debug('Received: %s', line)					
					break
			
			
			'''
			# Notify readers
			try:
				self._lock.acquire()
				self._read_log_file_fd.write(line)
				for fd in self._fifos:
					fd.write(line)
			finally:
				self._lock.release()
			'''	
			# TODO: Parse errors
					
		'''		
		
		
		while True:
			line = ''
			while self.channel.recv_ready():
				self.out += self.channel.recv(1)
				if self.out[-1] == '\n':
					break
			
			error = re.search(self.err_re, self.out)
			if error:
				# Detect if traceback presents
				traceback = ''
				while channel.recv_ready():
					traceback += channel.recv(1)
					if traceback[-1] == '\n':
						break
				
				if re.search(self.traceback_re, traceback):
					newline = ''
					while channel.recv_ready():
						newline += channel.recv(1)
						if newline[-1] == '\n':
							newline
							traceback += newline
							if not newline.startswith(' '):
								break
							newline = ''

					self._error = error.group(0) + traceback
				else:
					self._error = error.group(0)
				break_tail.set()
				break
				
			if re.search(search_re, self.out):
				self.ret = re.search(search_re, self.out)
				break_tail.set()
				break
		'''	
	
	def head(self, lines=0):
		if not self._reader_started:
			self._start_reader()
			
		# Create log reader fifo
		fifo = os.tmpnam()
		os.mkfifo(fifo)
		self._logger.debug('Create reader %s', fifo)

		try:
			self._lock.acquire()			
			
			# Copy read log to fifo			
			self._read_log_file_fd.flush()			
			fdr = open(self._read_log_file, 'r')
			fdw = open(fifo, 'w')
			line = 0
			while line < lines:
				fdr.readline()
				line += 1
			fdw.write(fdr.read())
			self._fifos[fifo] = fdw
		finally:
			self._lock.release()
			
		return fifo
	
	def tail(self, lines=0):
		if not self._reader_started:
			self._start_reader()
		
		# Create log reader fifo
		fifo = os.tempnam()
		os.mkfifo(fifo)

		try:
			self._lock.acquire()
			fdw = open(fifo, 'w')
			self._fifos[fifo] = fdw
		finally:
			self._lock.release()
		
		return fifo
	
	def detach(self, fifo):
		if fifo in self._fifos:
			try:
				self._lock.acquire()
				fdw = self._fifos[fifo]
				fdw.close()
				del self._fifos[fifo]
			finally:
				self._lock.release()

	
def attach_log(head=False, tail=False, lines=0):
	fifo = log_file.head(lines) if head else log_file.tail(lines)
	return pexpect.spawn('cat', [fifo])

def detach_log(exp):
	exp.close(force=True)
	log_file.detach(exp.args[0])
	

class TestHostInit(unittest.TestCase):
	
	timeout_start_main = 120
	timeout_start_snmp = 60
	timeout_host_init = 60
	
	_log = None
	
	def setUp(self):
		ssh = SshManager('ec2-174-129-177-52.compute-1.amazonaws.com', config.get('test-farm/farm_key'))
		ssh.connect()	
		log_file = MutableLogFile(ssh.get_root_ssh_channel())
		fifo = log_file.head()
		self._log = pexpect.spawn('cat', [fifo])
		#self._log = attach_log(head=True)
	
	def tearDown(self):
		detach_log(self._log)
	
	def test(self):
		self._log.expect(r'\[pid: \d+\] Starting scalarizr', self.timeout_start_main)
		self._log.expect(r'Build message consumer server', 15)
		self._log.expect(r'\[pid: \d+\] Starting scalarizr', self.timeout_start_snmp)
		self._log.expect("Message 'HostInit' delivered", self.timeout_host_init)


'''
class TestHostUp(unittest.TestCase):
	_log_channel = None
	_scalr_ctl = None
	
	def setUp(self):
		self._scalr_ctl = ScalrCtl()
		self._log_channel = tail_log_channel(ssh.get_root_ssh_channel())
	
	def test(self):
		self._scalr_ctl.exec_cronjob('ScalarizrMessaging')

		pass
'''

class TestReboot(unittest.TestCase):
	pass

class TestHostDown(unittest.TestCase):
	pass

class TestExecuteScript(unittest.TestCase):
	pass

class TestIpListBuilder(unittest.TestCase):
	pass

def suite():
	global log_file
	
	suite = unittest.TestSuite()
	suite.addTest(TestHostInit())
	return suite

if __name__ == '__main__':
	szr_integtest.main()
	

	ssh = SshManager('ec2-174-129-177-52.compute-1.amazonaws.com', config.get('test-farm/farm_key'))
	ssh.connect()	
	log_file = MutableLogFile(ssh.get_root_ssh_channel())
	
		
	'''
	alltests = unittest.TestSuite((
		TestLaunch(role_name='test-base-euca-2'),
		TestHostInit(),
		TestHostUp(),
		TestIpListBuilder(),
		TestExecuteScript(),
		TestReboot(),		
		#TestHostDown()
	))
	'''
	unittest.main()
