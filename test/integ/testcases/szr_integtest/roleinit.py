import logging
from szr_integtest import config, get_selenium, MutableLogFile
from szr_integtest_libs.scalrctl import FarmUI, ScalrCtl
from szr_integtest_libs import SshManager
import re
import time


class RoleHandler:
	
	def __init__(self, role_name, role_opts):
		self.role_opts = role_opts
		self._logger = logging.getLogger(__name__)
		self.role_name = role_name
		self.farm_id = config.get('./test-farm/farm_id')
		self.farm_key = config.get('./test-farm/farm_key')
		self.server_id_re = re.compile(
				'\[FarmID:\s+%s\].*?%s\s+scaling\s+\up.*?ServerID\s+=\s+(?P<server_id>[\w-]+)' \
				% (self.farm_id, self.role_name), re.M)
		self.scalr_ctl = ScalrCtl(self.farm_id)

	
	def test_init(self, sequence):
		self.farm = FarmUI(get_selenium())
		self._logger.info("Launching farm")
		self.farm.use(self.farm_id)
		#self.farm.remove_all_roles()
		#self.farm.add_role(self.role_name, 1, 2, self.role_opts)
		#self.farm.save()
		self.farm.launch()
		
		self._logger.info("Farm launched")
		out = self.scalr_ctl.exec_cronjob('Scaling')

		result = re.search(self.server_id_re, out)
		if not result:
			raise Exception('Farm hasn\'t been scaled up. Out:\n%s' % out)
		
		self.server_id = result.group('server_id')
		self._logger.info("New server id: %s" % self.server_id)
		self.ip        = self.farm.get_public_ip(self.server_id, 180)
		self.inst_id   = self.farm.get_instance_id(self.server_id, 180)
		self._logger.info("New server's ip: %s" % self.ip)
		
		self.ssh = SshManager(self.ip, self.farm_key, 180)
		self._logger.info('Sleeping for 15 sec while instance stands up')
		time.sleep(15)
		self.ssh.connect()
		self._logger.info("Connected to instance")
		
		# Temporary solution
		#self._logger.info("Deploying dev branch")
		#channel = self.ssh.get_root_ssh_channel()
		#exec_command(channel, '/etc/init.d/scalarizr stop')
		#exec_command(channel, 'echo "" > /var/log/scalarizr.log')
		#deployer = ScalarizrDeploy(self.ssh)
		#deployer.apply_changes_from_tarball()
		#del(deployer)		
		#self.ssh.close_all_channels()
#		
		channel = self.ssh.get_root_ssh_channel()
##
#		exec_command(channel, 'rm -f /etc/scalr/private.d/.state')
		#exec_command(channel, '/etc/init.d/scalarizr start')
#		
		log = MutableLogFile(channel)
		reader = log.head()
		reader.expect("Message 'HostInit' delivered", 180)
		self.scalr_ctl.exec_cronjob('ScalarizrMessaging')
	
		self.expect_sequence(reader, sequence)
			
		self._logger.info('>>>>>> Role has been successfully initialized. <<<<<<')
		
	def expect_sequence(self, reader, sequence, timeout = 120):
		for regexp in sequence:
			ret = reader.expect(regexp, timeout)
			self._logger.info("%s appeared in scalarizr.log", ret.group(0))