'''
Created on Sep 23, 2010

@author: marat
'''

from szr_integtest import config
from szr_integtest_libs.ssh_tool import execute, SshManager

import logging
import time
import os
from scalarizr.util import wait_until
import urllib
import httplib2
import simplejson
import copy

log_path = os.path.expanduser('~/.scalr-dev/logs')
server_info_url = 'http://scalr-dev.local.webta.net/servers/extendedInfo'

class FarmUIError(Exception):
	pass

EC2_ROLE_DEFAULT_SETTINGS = {
	'aws.availability_zone' : 'us-east-1a',
	'aws.instance_type' : 't1.micro',
}

EC2_MYSQL_ROLE_DEFAULT_SETTINGS = {
	'mysql.ebs_volume_size' : '1'
}

class ScalrConsts:
	class Platforms:
		PLATFORM_EC2 	= 'Amazon EC2'
		PLATFORM_RACKSPACE 	= 'Rackspace'
	class Behaviours:
		BEHAVIOUR_BASE  = 'Base'
		BEHAVIOUR_APP   = 'Apache'
		BEHAVIOUR_MYSQL = 'MySQL' 
		BEHAVIOUR_WWW = 'Nginx'
		BEHAVIOUR_MEMCACHED = 'Memcached'


class FarmUI:
	sel = None
	farm_id = None	
	servers = None
	
	def _login(f):
		def g(self, *args, **kwargs):
			if not hasattr(self.sel, '_logged_in') or not self.sel._logged_in:
				ui_login(self.sel)
			return f(self, *args, **kwargs)
		return g	
	
	
	
	def __init__(self, sel):
		self.sel = sel
		self.servers = []
		self.farm_id = config.get('test-farm/farm_id')
	
	def use(self, farm_id):
		if 'farms_builder.php?id=%s' % self.farm_id in self.sel.get_location():
			return
		self.servers = []
		ui_login(self.sel)
		self.sel.open('farms_builder.php?id=%s' % self.farm_id)
		#wait_until(lambda: not self.sel.is_element_present('//html/body/div[@class="ext-el-mask-msg x-mask-loading"]/div'), timeout=10, sleep=0.5)
		wait_until(lambda: self.sel.is_element_present('//span[text()="Roles"]'), sleep=0.1, timeout=10)
	
	@_login
	def add_role(self, role_name, min_servers=1, max_servers=2, settings=None):
		# TODO: Check location for role edit page presence
		settings = settings or dict()
		if not 'aws.instance_type' in settings:
			settings['aws.instance_type'] = 't1.micro'
		settings['scaling.min_instances'] = settings.get('scaling.min_instances', min_servers)
		settings['scaling.max_instances'] = settings.get('scaling.max_instances', max_servers)
		if not 'farms_builder.php?id=' in self.sel.get_location():
			self.sel.open("farms_builder.php?id=%s" % self.farm_id)
			#raise FarmUIError("Farm's settings page hasn't been opened. Use farm first")
		wait_until(lambda: self.sel.is_element_present('//span[text()="Roles"]'), sleep=0.1, timeout=10)
		self.sel.click('//span[text()="Roles"]')
		self.sel.click('//div[@class="viewers-selrolesviewer-blocks viewers-selrolesviewer-add"]')
		self.sel.wait_for_condition(
				"selenium.browserbot.getCurrentWindow().document.getElementById('viewers-addrolesviewer')", 5000)
		try:
			self.sel.click('//li[@itemname="%s"]' % role_name)
			time.sleep(0.5)
			self.sel.click('//li[@itemname="%s"]/div[@class="info"]/img[1]' % role_name)
			if self.sel.is_element_present('//label[text()="Location:"]'):
				self.sel.click('//label[text()="Platform:"]')
				self.sel.click('//div[text()="Amazon EC2"]')
				self.sel.click('//label[text()="Location:"]')
				self.sel.click('//div[@class="x-combo-list-inner"]/div[text()="AWS / US East 1"]')
				self.sel.click('//button[text()="Add"]')
		except:
			raise Exception("Role '%s' doesn't exist" % role_name)
		
		
		self.edit_role(role_name, settings)
		
	def _role_in_farm(self, role_name):
		if not 'farms_builder.php?id=' in self.sel.get_location():
			self.sel.open("farms_builder.php?id=%s" % self.farm_id)
		
		wait_until(lambda: self.sel.is_element_present('//span[text()="Roles"]'), sleep=0.1, timeout=10)
		self.sel.click('//span[text()="Roles"]')
		
		try:
			try:
				self.sel.click('//span[@class="short" and text()="%s"]' % role_name)
			except:				
				self.sel.click('//div[@class="full" and text()="%s"]' % role_name)
		except:
			return False
		return True
	
	@_login
	def edit_role(self, role_name, settings=None):
		if not 'farms_builder.php?id=' in self.sel.get_location():
			self.sel.open("farms_builder.php?id=%s" % self.farm_id)
		
		role_opts = copy.copy(settings)
		
		wait_until(lambda: self.sel.is_element_present('//span[text()="Roles"]'), sleep=0.1, timeout=10)
		self.sel.click('//span[text()="Roles"]')

		if not self._role_in_farm(role_name):
			raise Exception("Farm '%s' doesn't have role '%s'" % (self.farm_id, role_name))
		
		i = 1
		while role_opts:
			try:
				self.sel.click('//div[@class="viewers-farmrolesedit-tabs"]/div[not(@style)][%s]' % i)
				time.sleep(0.5)
				wait_until(lambda: not self.sel.is_element_present('//html/body/div[@class="ext-el-mask-msg x-mask-loading"]/div'), timeout=10, sleep=0.5)
				time.sleep(0.5)
				for option, value in settings.iteritems():
					el_xpath = '//input[@name = "%s"]' % option
					if self.sel.is_element_present(el_xpath) and self.sel.is_visible(el_xpath):
						try:
							id = self.sel.get_attribute('//div[@class=" x-panel x-panel-noborder"]//*[@name="%s"]/@id' % option)
							self.sel.run_script("with (Ext.getCmp('%s')) { setValue('%s'); fireEvent('select'); }" % (id, value))
							del(role_opts[option])
						except:
							pass
				time.sleep(0.5)
				i += 1
			except (Exception, BaseException), e:
				break
				
		self.sel.click('//div[@class="viewers-selrolesviewer-blocks viewers-selrolesviewer-add"]')
	@_login
	def remove_role(self, role_name):
		if not 'farms_builder.php?id=' in self.sel.get_location():
			#raise Exception("Farm's settings page hasn't been opened. Use farm first")
			self.use(self.farm_id)
		
		self.sel.click('//span[text()="Roles"]')
		
		try:
			self.sel.click('//div[text()="%s"]/../a' % role_name)
			self.sel.click('//button[text()="Yes"]')
		except:
			raise Exception("Role '%s' doesn't exist" % role_name)

	@_login		
	def remove_all_roles(self):
		if not 'farms_builder.php?id=' in self.sel.get_location():
			raise Exception("Farm's settings page hasn't been opened. Use farm first")

		self.sel.click('//span[text()="Roles"]')
		while True:
			try:
				self.sel.click('//div[@id="viewers-selrolesviewer"]/ul/li/a/')
				self.sel.click('//button[text()="Yes"]')
			except:
				break
	@_login	
	def save(self):
		if not 'farms_builder.php?id=' in self.sel.get_location():
			raise Exception("Farm's settings page hasn't been opened. Use farm first")
		
		wait_until(lambda: self.sel.is_element_present('//button[text() = "Save"]'), sleep=0.1, timeout=20)
		self.sel.click('//button[text() = "Save"]')
		wait_until(lambda: not self.sel.is_element_present('//div[text() = "Please wait while saving..."]'), sleep=0.2, timeout=20)
		while True:
			try:
				text = self.sel.get_text('//div[@id="top-messages"]/div[last()]')
				break
			except:
				continue

		if text != 'Farm successfully saved':
			raise FarmUIError('Something wrong with saving farm %s : %s' % (self.farm_id, text))
		
	@_login
	@property
	def state(self):
		if not hasattr(self, 'farm_id'):
			raise FarmUIError("Can't launch farm without farm_id: use the farm first")
		
		self.sel.open('/farms_view.php?farmid=%s' % self.farm_id)
		#self.sel.wait_for_page_to_load(30000)
		#wait_until(lambda: self.sel.is_element_present('//html/body/div[@class="ext-el-mask-msg x-mask-loading"]/div'), timeout=10, sleep=0.5)
		wait_until(lambda: self.sel.is_element_present('//dt[@dataindex="status"]/em/span'), timeout=10, sleep=0.5)
		time.sleep(0.5)
		return self.sel.get_text('//dt[@dataindex="status"]/em/span').lower()
	
	@_login
	def launch(self):
		if not hasattr(self, 'farm_id'):
			raise FarmUIError("Can't launch farm without farm_id: use the farm first")
		
		self.sel.open('/farms_control.php?farmid=%s' % self.farm_id)
		self.sel.wait_for_page_to_load(30000)
		#self._wait_for_page_to_load()

		if self.sel.is_text_present("Would you like to launch"):
			self.sel.click('cbtn_2')
			self.sel.wait_for_page_to_load(30000)
		else:
			self.sel.open('/')
			raise Exception('Farm %s has been already launched' % self.farm_id)
	
	@_login
	def terminate(self, keep_ebs=False, remove_from_dns=True):
		if not hasattr(self, 'farm_id'):
			raise FarmUIError("Can't launch farm without farm_id: use the farm first")

		#TODO: use 'keep_ebs' argument
		self.sel.open('/farms_control.php?farmid=%s' % self.farm_id)
		if self.sel.is_text_present("You haven't saved your servers"):
			self.sel.click('cbtn_3')
			self.sel.wait_for_page_to_load(30000)
		if self.sel.is_text_present('Delete DNS zone from nameservers'):
			if remove_from_dns:
				self.sel.check('deleteDNS')
			else:
				self.sel.uncheck('deleteDNS')
			self.sel.click('cbtn_2')
			self.sel.wait_for_page_to_load(30000)
			try:
				self.sel.get_text('//div[@class="viewers-messages viewers-successmessage"]/')
			except:
				try:
					text = self.sel.get_text('//div[@class="viewers-messages viewers-errormessage"]/')
					raise FarmUIError('Something wrong with terminating farm %s : %s' % (self.farm_id, text))
				except FarmUIError, e:
					print str(e)
				except Exception, e:
					print 'Cannot terminate farm for unknown reason'
		else:
			self.sel.open('/')
			raise Exception('Farm %s has been already terminated' % self.farm_id)
	
	def get_public_ip(self, server_id, timeout = 120):
		return self._get_server_info(server_id, ('Public IP',), timeout)
	
	def get_private_ip(self, server_id, timeout = 120):
		return self._get_server_info(server_id, ('Private IP',), timeout)
	
	def get_instance_id(self, server_id, timeout = 120):
		return self._get_server_info(server_id, ('Instance ID', 'Server ID'), timeout)
	@_login
	def _get_server_info(self, server_id, field_labels, timeout):
		"""
		start_time = time.time()
		first_time = True
		while time.time() - start_time < timeout:
			#self.sel.open('server_view_extended_info.php?server_id=%s' % server_id)
			if not first_time:
				self.sel.refresh()
			else:
				first_time = False
				
			self.sel.open('#/servers/%s/extendedInfo' % server_id)
			
			self._wait_for_page_to_load()
			#self.sel.wait_for_page_to_load(15000)
			if self.sel.is_text_present('not available for'):
				self.sel.mouse_over('//div[@class="x-tool x-tool-close"]')
				self.sel.click('//div[@class="x-tool x-tool-close"]')
				continue
			try:
				server_info = self.sel.get_text(search_str).strip()
			except:
				raise FarmUIError('Server %s doesn\'t exist')
			if server_info:
				break
			self.sel.mouse_over('//div[@class="x-tool x-tool-close"]')
			self.sel.click('//div[@class="x-tool x-tool-close"]')
		else:
			raise FarmUIError("Cannot retrieve server's information. Server id: %s " % server_id)
		self.sel.mouse_over('//div[@class="x-tool x-tool-close"]')
		self.sel.click('//div[@class="x-tool x-tool-close"]')
		"""
		try:
			http = httplib2.Http()
			headers = {'Content-type': 'application/x-www-form-urlencoded',
					   'Cookie' : self.sel.get_cookie()}
			body = urllib.urlencode({'id' : server_id})
			start_time = time.time()
			while time.time() - start_time <= timeout:
				content = http.request(server_info_url, 'POST', body=body, headers=headers)[1]
				content = simplejson.loads(content)
				if not content['success']:
					continue
				for block in content['moduleParams']:
					for param_set in block['items']:
						if not 'fieldLabel' in param_set:
							continue
						if not any(map(lambda x: x in param_set['fieldLabel'], field_labels)):
						#if not field_label in param_set['fieldLabel']:
							continue
						if param_set['value']:
							return param_set['value']
						
			else:
				raise Exception('Timeout after %s sec.' % timeout)
		except (Exception, BaseException), e:
			raise FarmUIError("Can't get %s from scalr's interface. %s" % (field_labels[0].lower(), e))

	def create_mysql_backup(self):
		self._open_mysql_status_page()
		self.sel.click('//input[@name="run_bcp"]')

	def create_pma_users(self):
		self._open_mysql_status_page()
		try:
			self.sel.click('//input[@name="pma_request_credentials"]')
		except:
			raise FarmUIError('PhpMyAdmin user creation request has been already sent.')
		
	def create_databundle(self):
		self._open_mysql_status_page()
		try:
			self.sel.click('//input[@name="run_bundle"]')
		except:
			raise FarmUIError("Can't send databundle request")
		
	@_login	
	def _open_mysql_status_page(self):
		if not hasattr(self, 'farm_id'):
			raise FarmUIError("Can't launch farm without farm_id: use the farm first")
		
		self.sel.open('/farm_mysql_info.php?farmid=%s' % self.farm_id)
		self.sel.wait_for_page_to_load(30000)
		if not self.sel.is_text_present('Replication status'):
			raise FarmUIError("Error while opening MySQL status page for farm ID=%s. Make sure your farm has MySQL role enabled." % self.farm_id)
	@_login
	def get_server_list(self, role_name):
		ret = []
		self.sel.open('/farm_roles_view.php?farmid=%s' % self.farm_id )
		self.sel.wait_for_page_to_load(15000)
		time.sleep(1)
		self.sel.click('//a[text()="%s"]/../../../dt[@dataindex="servers"]/em/a' % role_name)
		time.sleep(3)
		server_count = int(self.sel.get_xpath_count('//div[@class="x-list-body-inner"]/descendant::em[text()="Running "]'))
		if not server_count:
			return []
		for i in range(1, server_count+1):
			ip = self.sel.get_text('//div[@class="x-list-body-inner"]/descendant::em[text()="Running "][%d]/../../dt[@dataindex="remote_ip"]/em' % i).strip()
			if ip:
				if self.sel.get_text('//div[@class="x-list-body-inner"]/descendant::em[text()="Running "][%d]/../../dt[@dataindex="farm_id"]/em/' % i).strip().endswith('(Master)'):
					ret.insert(0, ip)
				else:
					ret.append(ip)
		return ret
		# TODO: Handle situation when there is no master in role
	@_login	
	def get_role_name(self, scalr_srv_id):
		self.use(self.farm_id)
		self.sel.open('#/servers/view')  
		#self._wait_for_page_to_load()
		wait_until(lambda: self.sel.is_element_present('//div[@class="x-list-body-inner"]'), sleep=1)
		time.sleep(0.5)
		try:
			return self.sel.get_text('//a[contains(@href, "%s")]/../../../dt[@dataindex="farm_id"]/em/a[2]' % scalr_srv_id)
		except:
			raise Exception("Server with id '%s' doesn't exist." % scalr_srv_id)
	@_login
	def get_role_id(self, role_name, platform):
		server_info_url = 'http://scalr-dev.local.webta.net/roles/xListViewRoles/'
		platforms = {'ec2':'Amazon EC2', 
					'rackspace':'Rackspace'}	
		
		self.use(self.farm_id)
		http = httplib2.Http()

		body = urllib.urlencode({'query' : 'szr-apache-unstable-ubuntu1004-64', 'limit' : '10'})
		headers = {'Content-type': 'application/x-www-form-urlencoded',
                        'Cookie' : self.sel.get_cookie()}
		
		content = http.request(server_info_url, 'POST', body=body, headers=headers)
		data = simplejson.loads(content[1])
		
		for role in data['data']:
			if role['platforms'] == platforms[platform]:
				return role['id']
		else:
			raise Exception('Cannot determine role_id of %s' % role_name)
	@_login	
	def get_farm_role_id(self, role_name, platform):
		server_info_url = 'http://scalr-dev.local.webta.net/server/grids/farm_roles_list.php?a=1&farmid=%s' % self.farm_id
		http = httplib2.Http()

		headers = {'Content-type': 'application/x-www-form-urlencoded',
                        'Cookie' : self.sel.get_cookie()}
		
		content = http.request(server_info_url, 'POST', body={}, headers=headers)
		data = simplejson.loads(content[1])
		
		for farm_role in data['data']:
			if farm_role['platform'] == platform and farm_role['name'] == role_name:
				print 'Bingo!', farm_role
				print 'ID: %s' % farm_role['id']
				return farm_role['id']
		else:
			raise Exception('Cannot determine farm role id of %s' % role_name)		
		
	def _wait_for_page_to_load(self):
		path = '//span[text()="Please wait ..."]'
		wait_until(lambda: self.sel.is_element_present(path) and not self.sel.is_visible(path), sleep=0.5)
	@_login	
	def configure_vhost(self, domain, role_name, platform):
		role_id = self.get_role_id(role_name, platform)
		document_root = os.path.join('/var/www/', domain)

		self.sel.open('/apache_vhost_add.php')		
		self.sel.type('domain_name', domain)
		self.sel.type('farm_target', self.farm_id)
		self.sel.type('role_target', role_id)
		self.sel.uncheck('isSslEnabled')
		self.sel.type('document_root_dir', document_root)
		self.sel.type('server_admin', 'admin@%s' % domain)		
		self.sel.click('button_js')	
	@_login	
	def configure_vhost_ssl(self, domain, role_name, platform):
		document_root = '/var/www/ssl.dima2.com/'
		ssl_cert = '~/.scalr/apache/server.crt'
		ssl_key = '~/.scalr/apache/server.key'
		ca_cert = '~/.scalr/apache/ca.crt'
		
		role_id = self.get_role_id(role_name, platform)

		self.sel.open('/apache_vhost_add.php')
		self.sel.type('domain_name', domain)
		self.sel.type('farm_target', self.farm_id)
		self.sel.type('role_target', role_id)
		self.sel.check('isSslEnabled')
		
		self.sel.type('ssl_cert', ssl_cert)
		self.sel.type('ssl_key', ssl_key)
		self.sel.type('ca_cert', ca_cert)
		
		self.sel.type('document_root_dir', document_root)
		self.sel.type('server_admin', 'admin@%s' % domain)	
		self.sel.click('button_js')	
		
def ui_import_server(sel, platform_name, behaviour, host, role_name):
	'''
	@return: import shell command
	'''
	ui_login(sel)
	sel.open('szr_server_import.php')
	
	platforms = sel.get_select_options('//td[@class="Inner_Gray"]/table/tbody/tr[2]/td[2]/select')
	if not platform_name in platforms:
		raise Exception('Unknown platform: %s' % platform_name)
	sel.select('//td[@class="Inner_Gray"]/table/tbody/tr[2]/td[2]/select', platform_name)
	
	behaviours = sel.get_select_options('//td[@class="Inner_Gray"]/table/tbody/tr[3]/td[2]/select')
	if not behaviour in behaviours:
		raise Exception('Unknown behaviour: %s' % behaviour)
	sel.select('//td[@class="Inner_Gray"]/table/tbody/tr[3]/td[2]/select', behaviour)
	
	sel.type('//td[@class="Inner_Gray"]/table/tbody/tr[4]/td[2]/input', host)
	sel.type('//td[@class="Inner_Gray"]/table/tbody/tr[5]/td[2]/input', role_name)
	sel.click('cbtn_2')
	sel.wait_for_page_to_load(15000)
	if not sel.is_text_present('Step 2'):
		try:
			text = sel.get_text('//div[@class="viewers-messages viewers-errormessage"]/span')			
			raise FarmUIError('Something wrong with importing server: %s' % text)
		except FarmUIError, e:
			raise
		except:
			raise Exception("Can't import server for unknow reason (Step 1)")
		
	return sel.get_text('//td[@class="Inner_Gray"]/table/tbody/tr[3]/td[1]/textarea')
	
def ui_login(sel):
	
	if hasattr(sel, '_logged_in') and sel._logged_in:
		return
	try:
		login = config.get('./scalr/admin_login')
		password = config.get('./scalr/admin_password')
	except:
		raise Exception("User's ini file doesn't contain username or password")
	
	sel.delete_all_visible_cookies()
	sel.open('/')
	sel.click('//div[@class="login-trigger-header"]/a')
	wait_until(lambda: sel.is_element_present('//div[@id="login-panel"]'), sleep=0.1, timeout=15)
	sel.type('login', login)
	sel.type('pass', password)
	sel.check('keep_session')
	sel.click('//form/button')
	sel.wait_for_page_to_load(30000)
	#if sel.get_location().find('/client_dashboard.php') == -1:
	if not sel.is_element_present('//div[@id="navmenu"]'):
		raise Exception('Login failed.')
	sel._logged_in = True

def reset_farm(ssh, farm_id):
	pass

class ScalrCtl:
	def __init__(self, farmid=None):
		self._logger = logging.getLogger(__name__)
		
		self.farmid = farmid
		scalr_host = config.get('./scalr/hostname')
		ssh_key_path = config.get('./scalr/ssh_key_path')
		
		if not os.path.exists(ssh_key_path):
			raise Exception("Key file %s doesn't exist" % ssh_key_path)
		ssh_key_password = config.get('./scalr/ssh_key_password')
		self.ssh = SshManager(scalr_host, ssh_key_path, key_pass = ssh_key_password)
		self.ssh.connect()
		
		self.channel = self.ssh.get_root_ssh_channel()
		self._logger.info('Estabilished connection to %s' % scalr_host)
		if not os.path.isdir(log_path):
			os.makedirs(log_path)		

	def exec_cronjob(self, name):
		if self.channel.closed:
			print "channel was closed. getting new one."
			self.channel = self.ssh.get_root_ssh_channel()
			
		cron_keys = ['BundleTasksManager']
		cron_ng_keys = ['ScalarizrMessaging', 'MessagingQueue', 'Scaling', 'Poller']
		
		if not name in cron_keys and not name in cron_ng_keys:
			raise Exception('Unknown cronjob %s' % name)
	
		cron_php_path = ('cron-ng/' if name in cron_ng_keys else 'cron/') +'cron.php'	
		
		home_path = config.get('./scalr/home_path')
		self._logger.info('channel: %s' % type(self.channel))
		#clean_output(self.channel, 5)
		
		self._logger.info('cd %s' % home_path)
		execute(self.channel, 'cd ' + home_path)
		farm_str = ('--farm-id=%s' % self.farmid) if (self.farmid and name in ('ScalarizrMessaging', 'Scaling')) else ''
		job_cmd = 'php -q ' + cron_php_path + ' --%s %s' % (name, farm_str)
		self._logger.info('Starting cronjob: %s' % job_cmd)
		out = execute(self.channel, job_cmd)
		log_filename = name + time.strftime('_%d_%b_%H-%M') + '.log'
		try:
			fp = open(os.path.join(log_path, log_filename), 'w')
			fp.write(out)
			fp.close()
		except:
			pass
		return out
	
	def enable_svn_access(self, ip):
		if self.channel.closed:
			self.channel = self.ssh.get_root_ssh_channel()
			
		out = execute(self.channel, 'svn2allow %s' % ip)
		
		if not 'Successfully enabled SVN access' in out:
			raise Exception("Can't enable SVN access to %s. Output: \n%s" % (ip, out))
		
		
