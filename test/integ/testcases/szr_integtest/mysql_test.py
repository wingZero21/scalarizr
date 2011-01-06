'''
Created on Jan 5, 2011

@author: spike
'''
from szr_integtest_libs.scalrctl		import	ScalrCtl, EC2_ROLE_DEFAULT_SETTINGS, EC2_MYSQL_ROLE_DEFAULT_SETTINGS
from szr_integtest_libs.datapvd.mysql	import	MysqlDataProvider
from szr_integtest.ec2.import_test		import	_init_server

import unittest
import logging
import re
import time
from szr_integtest_libs.ssh_tool import execute


class StartupMasterHostUpFailed(unittest.TestCase):

	def test_master_hostup_failed(self):
		opts = EC2_MYSQL_ROLE_DEFAULT_SETTINGS
		opts.update(EC2_MYSQL_ROLE_DEFAULT_SETTINGS)
		opts.update({'system.timeouts.launch' : '60'})
		dp = MysqlDataProvider(farm_settings=opts)
		master = dp.master()
		reader = master.log.head()
		reader.expect("Message 'HostInit' delivered", 60)
		ssh = master.ssh()
		execute(ssh, '/etc/init.d/scalarizr stop', 15)
		scalrctl = ScalrCtl(dp.farm_id)
		searcher = re.compile("Server \\\\'%s\\\\' did not send.+Terminating instance" % master.scalr_id)
		while True:
			poll = scalrctl.exec_cronjob('Poller')
			res = re.search(searcher, poll)
			if res:
				break
			time.sleep(5)
		new_master = dp.slave()
		scalrctl.exec_cronjob('ScalarizrMessaging')
		reader = new_master.log.head()
		reader.expect("Message 'HostInit' delivered", 60)
		
		ssh = new_master.ssh()
		execute(ssh, '/etc/init.d/scalarizr stop', 15)
		execute(ssh, 'sed -i "s/^behaviour.*$/behaviour = /g\" /etc/scalr/public.d/config.ini', 10)
		execute(ssh, '/etc/init.d/scalarizr start', 15)
		
		scalrctl.exec_cronjob('ScalarizrMessaging')
		reader.expect("Received ingoing message 'HostInitResponse' in queue control", 30)
		msg_id = reader.expect("\(message_id: (?P<msg_id>[\d-]+)\)", 10).group('msg_id')
		message = new_master.get_message(msg_id)
		res = re.search('<replication_master>(?P<repl_master>\d)>', message)
		if not res:
			raise Exception("HostInitResponse doesn't contain replication master option.")
		repl_master = res.group('repl_master')
		self.assertEqual('1', repl_master)
		
		dp.farmui.terminate()									
if __name__ == "__main__":
	unittest.main()