'''
Created on Jan 5, 2011

@author: spike
'''
from szr_integtest_libs.scalrctl		import	ScalrCtl, EC2_ROLE_DEFAULT_SETTINGS, EC2_MYSQL_ROLE_DEFAULT_SETTINGS
from szr_integtest_libs.datapvd.mysql	import	MysqlDataProvider
from szr_integtest_libs.ssh_tool import execute
from szr_integtest_libs.datapvd import DataProvider

from szr_integtest import RESOURCE_PATH

from scalarizr.util import ping_socket
from scalarizr.libs.metaconf import Configuration
from scalarizr.handlers.mysql import ROOT_USER, REPL_USER, STAT_USER, \
									OPT_REPL_PASSWORD, OPT_STAT_PASSWORD, OPT_ROOT_PASSWORD, CNF_SECTION

import unittest
import logging
import re
import time
import _mysql
import os
import copy

from StringIO import StringIO
from _mysql_exceptions import OperationalError

def get_mysql_passwords(ssh):
	private_cnf = StringIO(execute(ssh, 'cat /etc/scalr/private.d/mysql.ini'))
	cnf = Configuration('ini')
	cnf.readfp(private_cnf)
	try:
		root_pass = cnf.get('./%s/%s' % (CNF_SECTION, OPT_ROOT_PASSWORD))
		repl_pass = cnf.get('./%s/%s' % (CNF_SECTION, OPT_REPL_PASSWORD))
		stat_pass = cnf.get('./%s/%s' % (CNF_SECTION, OPT_STAT_PASSWORD))
	except:
		raise Exception("Mysql config doesn't contain essentiall passwords")
	return (root_pass, repl_pass, stat_pass)

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

opts = EC2_MYSQL_ROLE_DEFAULT_SETTINGS
opts.update(EC2_ROLE_DEFAULT_SETTINGS)
dp = MysqlDataProvider(farm_settings=opts)
scalrctl = ScalrCtl(dp.farm_id)












class StartupMasterHostUpFailed(unittest.TestCase):
	def test_master_hostup_failed(self):
		logger.info('>>>>>>>>>>>> Starting test "test_master_hostup_failed"')
		local_opts = copy.copy(opts)
		local_opts.update({'system.timeouts.launch' : '60'})
		dp.role_opts = local_opts
		#dp.edit_role(local_opts)
		master = dp.master()
		reader = master.log.head()
		reader.expect("Message 'HostInit' delivered", 60)
		ssh = master.ssh()
		execute(ssh, '/etc/init.d/scalarizr stop', 15)
		searcher = re.compile("Server \\\\'%s\\\\' did not send.+Terminating instance" % master.scalr_id)
		while True:
			poll = scalrctl.exec_cronjob('Poller')
			res = re.search(searcher, poll)
			if res:
				break
			time.sleep(5)
		#new_master = dp.slave()
		"""
		new_server_re = re.compile('\[FarmID:\s+%s\].*?%s\s+scaling\s+\up.*?ServerID\s+=\s+(?P<server_id>[\w-]+)' \
								 % (dp.farm_id, dp.role_name), re.M)
		out = scalrctl.exec_cronjob('Scaling')
		scalrctl.exec_cronjob('ScalarizrMessaging')
		
		res = re.search(new_server_re, out)
		if not res:
			raise Exception("Farm hasn't been scaled up")
		
		server_id = res.group('server_id')
		logger.info("New master's scalr id: %s" % server_id)
		dp2 = DataProvider(scalr_srv_id = server_id)
		new_master = dp.server()
		"""
		new_master = dp.slave(0)
		logger.info('Start Messaging')
		scalrctl.exec_cronjob('ScalarizrMessaging')
		logger.info('Messaging Done')
		reader = new_master.log.head()
		logger.info('Head log retrieved')
		reader.expect("Message 'HostInit' delivered", 60)
		logger.info('Host Init delivered')
		
		ssh = new_master.ssh()
		execute(ssh, '/etc/init.d/scalarizr stop', 15)
		execute(ssh, 'sed -i "s/^behaviour.*$/behaviour = /g\" /etc/scalr/public.d/config.ini', 10)
		execute(ssh, '/etc/init.d/scalarizr start', 15)
		time.sleep(4)		
		scalrctl.exec_cronjob('ScalarizrMessaging')
		msg_id = reader.expect("Received message 'HostInitResponse' \(message_id: (?P<msg_id>[\w-]+)\)", 30).group('msg_id')
		#reader.expect("Received ingoing message 'HostInitResponse' in queue control", 30)
		#msg_id = reader.expect("message_id: (?P<msg_id>[\w-]+)", 20).group('msg_id')
		message = new_master.get_message(msg_id)
		res = re.search('<replication_master>(?P<repl_master>\d)', message)
		if not res:
			raise Exception("HostInitResponse doesn't contain replication master option.")
		repl_master = res.group('repl_master')
		self.assertEqual('1', repl_master)		
		dp.terminate_farm()
		scalrctl.exec_cronjob('ScalarizrMessaging')
		logger.info('>>>>>>>>>>>> Test "test_master_hostup_failed" finished')
		
class StartupMaster(unittest.TestCase):

	def test_startup_master(self):
		logger.info('>>>>>>>>>>>> Starting test "test_startup_master"')
		local_opts = copy.copy(opts)
		local_opts.update({'system.timeouts.launch' : '2400'})
		dp.farm_settings = local_opts
		#dp.edit_role(local_opts)
		master = dp.master()
		dp.wait_for_hostup(master)
		# Check if mysql running
		ping_socket(master.public_ip, 3306, exc_str='Mysql is not running on master.')

		logger.info('Mysql is running on master instance')
				
		ssh = master.ssh()
		
		root_pass, repl_pass, stat_pass = get_mysql_passwords(ssh)
		
		logger.info('Mysql passwords successfully retrieved')
		
		master_status = execute(ssh, 'mysql -u%s -p%s -e "show master status"' % (ROOT_USER, root_pass))
		# Check if replication started
		self.assertFalse('Empty set' in master_status)
		self.assertFalse('Access denied for user' in master_status)
		logger.info('Replication master is running.')
		
		hostup = master.get_message(message_name='HostUp')
		self.assertTrue(re.search('<volume_config>.+</volume_config>', hostup))
		self.assertTrue(re.search('<snapshot_config>.+</snapshot_config>', hostup))
		self.assertTrue(re.search('<log_pos>\d+</log_pos>', hostup))
		self.assertTrue(re.search('<log_file>.+</log_file>', hostup))
		try:
			self.assertEqual(re.search('<root_password>(?P<root_pass>.+)</root_password>', hostup).group('root_pass'), root_pass)
			self.assertEqual(re.search('<repl_password>(?P<repl_pass>.+)</repl_password>', hostup).group('repl_pass'), repl_pass)
			self.assertEqual(re.search('<stat_password>(?P<stat_pass>.+)</stat_password>', hostup).group('stat_pass'), stat_pass)
		except AttributeError:
			raise Exception("Some of password were not found in HostUp message.")
		logger.info('HostUp contains all essential data')
		
		_mysql.connect(master.public_ip, STAT_USER, stat_pass)
		_mysql.connect(master.public_ip, REPL_USER, repl_pass)
		self.assertRaises(OperationalError, _mysql.connect, master.public_ip, ROOT_USER, root_pass)
		
		logger.info('Mysql users and permissions are set properly.')
		logger.info('>>>>>>>>>>>> Test "test_startup_master" finished')
		
class StartupSlave(unittest.TestCase):

	def test_startup_slave(self):
		logger.info('>>>>>>>>>>>> Starting test "test_startup_slave"')
		local_opts = copy.copy(opts)
		local_opts.update({'scaling.min_instances' : '2'})
		dp.farm_settings = local_opts
		#dp.edit_role(local_opts)
		slave = dp.slave(0)
		dp.wait_for_hostup(slave)
		ping_socket(slave.public_ip, 3306, exc_str='Mysql is not running on slave.')
		logger.info('Mysql is running on slave')

		# Getting mysql credentials
		master = dp.master()
		master_ssh = master.ssh()
		root_pass = get_mysql_passwords(master_ssh)[0]
		
		# Check for slave status
		ssh = slave.ssh()
		slave_status = execute(ssh, 'mysql -u%s -p%s -e "show slave status"' % (ROOT_USER, root_pass))
		self.assertFalse('Empty set' in slave_status)
		self.assertFalse('Access denied for user' in slave_status)
		logger.info('Slave is running.')
		
		master_private_ip = dp.farmui.get_private_ip(master.scalr_id, 60)
		self.assertTrue(master_private_ip in slave_status)
		
		logger.info("Master host has been set properly.")
		
		hostup = slave.get_message(message_name='HostUp')
		self.assertTrue(re.search("<volume_config>.+</volume_config>", hostup))
		logger.info('>>>>>>>>>>>> Test "test_startup_slave" finished')
		
class SlaveToMaster(unittest.TestCase):
	def test_slave_to_master(self):
		logger.info('>>>>>>>>>>>> Starting test "test_slave_to_master"')
		local_opts = copy.copy(opts)
		local_opts.update({'scaling.min_instances' : '2'})
		dp.farm_settings = local_opts
		master = dp.master()
		dp.wait_for_hostup(master)
		slave = dp.slave()
		dp.wait_for_hostup(slave)
		slave_reader = slave.log.head()
		logger.info('Terminating master')
		master_reader = master.log.tail()
		master.terminate()
		master_reader.expect("Message 'HostDown' delivered", 120)
		logger.info('Master successfully terminated.')
			
		scalrctl.exec_cronjob('ScalarizrMessaging')
		
		slave_reader.expect('PromoteToMaster', 60)
		logger.info('Promote to master message received by scalarizr')
		slave_reader.expect("Message 'Mysql_PromoteToMasterResult' delivered", 120)
		logger.info('Promote to master result delivered')
		ssh = slave.ssh()
		root_pass = get_mysql_passwords(ssh)[0]
		master_status_on_slave = execute(ssh, 'mysql -u%s -p%s -e "show master status"' % (ROOT_USER, root_pass))
		self.assertFalse('Empty set' in master_status_on_slave)
		self.assertFalse('Access denied for user' in master_status_on_slave)
		promo_to_master_res = slave.get_message(message_name = "Mysql_PromoteToMasterResult")
		self.assertTrue('<status>ok</status>' in promo_to_master_res)
		self.assertTrue(re.search("<volume_config>.+</volume_config>", promo_to_master_res))
		dp.sync()
		slave = dp.slave()
		dp.wait_for_hostup(slave)
		logger.info('>>>>>>>>>>>> Test "test_slave_to_master" finished')
		
		
class CreateBackup(unittest.TestCase):
	def test_create_backup(self):
		logger.info('>>>>>>>>>>>> Starting test "test_create_backup"')
		local_opts = copy.copy(opts)
		local_opts.update({'mysql.ebs_volume_size' : '4', 
						   'scaling.min_instances' : '2'})
		
		dp.farm_settings = local_opts

		slave = dp.slave()
		dp.wait_for_hostup(slave)
		master = dp.master()
		master_ssh = master.ssh()
		root_pass = get_mysql_passwords(master_ssh)[0]
		execute(master_ssh, 'mysql -u%s -p%s -e "create database test1; create database test2"'
																	 % (ROOT_USER, root_pass))
		shitgen_path = os.path.join(RESOURCE_PATH, 'shitgen.php')
		sftp = master.sftp()
		sftp.put(shitgen_path, '/tmp/shitgen.php')
		soft = 'php5-mysql php5-cli' if 'debian' == master.dist else 'php php-mysql'
		master.install_software(soft)
		execute(master_ssh, 'php /tmp/shitgen.php -u %s -p %s -d test1 -s 1Gb' % (ROOT_USER, root_pass))
		execute(master_ssh, 'php /tmp/shitgen.php -u %s -p %s -d test2 -s 10Mb' % (ROOT_USER, root_pass) )		
		slave_reader = slave.log.tail()
		dp.farmui.create_mysql_backup()
		slave_reader.expect('Mysql_CreateBackup')
		logger.info('Create backup message received by scalarizr.')
		slave_reader.expect("Dumping all databases", 60)
		slave_reader.expect("Message 'Mysql_CreateBackupResult' delivered", 60*20)
		cbr = slave.get_message(message_name="Mysql_CreateBackupResult")
		self.assertTrue('<status>ok</status>' in cbr)
		chunks = re.findall('<item>(.+)</item>', cbr)
		self.assertTrue(len(chunks) >= 2)
		logger.info('>>>>>>>>>>>> Test "test_create_backup" finished')
			
class CreateDataBundle(unittest.TestCase):
	def test_create_databundle(self):
		logger.info('>>>>>>>>>>>> Starting test "test_create_databundle"')
		dp.farm_settings = opts		
		master = dp.master()
		dp.wait_for_hostup(master)
		reader = master.log.tail()
		logger.info('Sending rebundle message to master.')
		dp.farmui.create_databundle()
		reader.expect('Mysql_CreateDataBundle', 60)
		logger.info('Message "CreateDataBundle" received.')
		reader.expect('Mysql_CreateDataBundleResult', 180)
		logger.info('Message "CreateDataBundleResult" sended.')
		cdbr = master.get_message(message_name="Mysql_CreateDataBundleResult")
		self.assertTrue('<status>ok</status>' in cdbr)
		self.assertTrue(re.search('<log_pos>\d+</log_pos>', cdbr))
		self.assertTrue(re.search('<log_file>[\w.]+</log_file>', cdbr))
		self.assertTrue(re.search('<snapshot_config>.+</snapshot_config>', cdbr))
		logger.info('>>>>>>>>>>>> Test "test_create_databundle" finished')
		
class MysqlSuite(unittest.TestSuite):
	def __init__(self, tests=()):
		self._tests = []
		self.addTest(StartupMasterHostUpFailed('test_master_hostup_failed'))
		"""
		self.addTest(StartupMaster('test_startup_master'))
		self.addTest(StartupSlave('test_startup_slave'))
		self.addTest(SlaveToMaster('test_slave_to_master'))
		self.addTest(CreateBackup('test_create_backup'))
		self.addTest(CreateDataBundle('test_create_databundle'))
		"""
		
if __name__ == "__main__":
	unittest.main()