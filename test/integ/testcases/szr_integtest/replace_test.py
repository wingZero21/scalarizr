'''
Created on Jan 24, 2011

@author: spike
'''
import unittest
import _mysql

from szr_integtest_libs.datapvd.mysql	import	MysqlDataProvider
from szr_integtest_libs.scalrctl		import	ScalrCtl, EC2_ROLE_DEFAULT_SETTINGS, EC2_MYSQL_ROLE_DEFAULT_SETTINGS
from szr_integtest_libs.ssh_tool import execute
from copy import copy


opts = EC2_MYSQL_ROLE_DEFAULT_SETTINGS
opts.update(EC2_ROLE_DEFAULT_SETTINGS)
dp = MysqlDataProvider(arch='i386', role_settings=opts)
scalrctl = ScalrCtl(dp.farm_id)


class ReplaceTest(unittest.TestCase):
	
	def test_replace_master(self):
		master = dp.master()
		dp.wait_for_hostup(master)
		ssh    = master.ssh()
		execute(ssh, "/etc/init.d/scalarizr stop")
		execute(ssh, "rm -f /etc/scalr/private.d/.update")
		execute(ssh, 'cp /etc/scalr/logging-debug.ini /etc/scalr/logging.ini')
		execute(ssh, "echo 'deb http://local.webta.net/apt/dev scalr/' > /etc/apt/sources.list.d/scalr.list")
		execute(ssh, "/etc/init.d/scalarizr_update start")
		execute(ssh, "/etc/init.d/scalarizr start")
		reader = master.log.tail()
		dp.farmui.create_pma_users()
		reader.expect("Message 'Mysql_CreatePmaUserResult' delivered", 60)
		cpmau = master.get_message(message_name="Mysql_CreatePmaUserResult")
		self.assertTrue('<status>ok' in cpmau)
		dp.farmui.create_databundle()
		reader.expect("Message 'Mysql_CreateDataBundleResult' delivered", 120)
		cdbr = master.get_message(message_name="Mysql_CreateDataBundleResult")
		self.assertTrue('<status>ok' in cdbr)		
		
	def test_replace_slave(self):
		dp.sync()
		local_opts = copy(opts)
		local_opts.update({'scaling.min_instances' : '2'})
		dp.role_opts = local_opts
		
		slave = dp.slave()
		ssh = slave.ssh()
		execute(ssh, "/etc/init.d/scalarizr stop")
		execute(ssh, "rm -f /etc/scalr/private.d/.update")
		execute(ssh, "rm -f /etc/scalr/private.d/.state")
		execute(ssh, 'cp /etc/scalr/logging-debug.ini /etc/scalr/logging.ini')
		execute(ssh, "echo 'deb http://local.webta.net/apt/dev scalr/' > /etc/apt/sources.list.d/scalr.list")
		execute(ssh, "rm -f /etc/scalr/private.d/db.sqlite")
		scalrdb = _mysql.connect("local.webta.net","dbuser","VAdeSN2bJzVArNdZ","scalr_dev_4")
		scalrdb.query("DELETE FROM messages WHERE server_id='%s'" % slave.scalr_id)
		execute(ssh, "/etc/init.d/scalarizr_update start")
		execute(ssh, "/etc/init.d/scalarizr start")
		dp.wait_for_szr_port(slave.public_ip)
		dp.wait_for_hostup(slave)
		
class ReplaceSuite(unittest.TestSuite):
	def __init__(self, tests=()):
		self._tests = []
		self.addTest(ReplaceTest('test_replace_master'))
		self.addTest(ReplaceTest('test_replace_slave'))
		
if __name__ == "__main__":
	#import sys;sys.argv = ['', 'Test.testName']
	unittest.main()