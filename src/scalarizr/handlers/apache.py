'''
Created on Dec 25, 2009

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler
from scalarizr.messaging import Messages
from scalarizr.util import disttool, system
import logging
import os
import re
import subprocess
import shutil
#from scalarizr.handlers import hooks

def get_handlers ():
	return [ApacheHandler()]

class ApacheHandler(Handler):
	_logger = None
	_queryenv = None

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self.name_vhost_regexp = re.compile(r'NameVirtualHost\s+\*[^:]')
		self.vhost_regexp = re.compile('<VirtualHost\s+\*>')
		self.strip_comments_regexp = re.compile( r"#.*\n")
		self.errorlog_regexp = re.compile( r"ErrorLog\s+(\S*)", re.IGNORECASE)
		self.customlog_regexp = re.compile( r"CustomLog\s+(\S*)", re.IGNORECASE)
		self.load_module_regexp = re.compile(r"LoadModule\s+mod_ssl",re.IGNORECASE)
		bus.define_events('apache_reload')

	def on_VhostReconfigure(self, message):
		self._logger.debug("Entering on_VhostReconfigure")
		
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self._reload_apache()

	def _update_vhosts(self):
				
		config = bus.config
		vhosts_path = config.get('behaviour_app','vhosts_path')
		httpd_conf_path = config.get('behaviour_app','httpd_conf_path')
		cert_path = bus.etc_path + '/private.d/keys'	
			
		try:
			self._logger.debug("Retrieving virtual hosts list from Scalr.")
			received_vhosts = self._queryenv.list_virtual_hosts()
		except:
			self._logger.error('Can`t retrieve virtual hosts list from Scalr.')
			raise
		
		if [] != received_vhosts:	
			self._logger.debug("Clean up old configuration.")			
			if not os.path.exists(vhosts_path):
				self._logger.warning('Virtual hosts directory %s doesn`t exist', vhosts_path)
				list_vhosts = []
				try:
					os.makedirs(vhosts_path)
				except OSError, e:
					self._logger.error('Couldn`t create directory %s. %s', 
							vhosts_path, e.strerror)
				else:
					self._logger.info('Virtual hosts directory has been created: %s', 
							vhosts_path)  
				
			else:
				list_vhosts = os.listdir(vhosts_path)
			
			if [] == list_vhosts:
				self._logger.info('Virtual hosts list is empty.')
			
			else:
				for fname in list_vhosts:
					if '000-default' == fname:
						continue
					vhost_file = vhosts_path + '/' + fname
					if os.path.isfile(vhost_file):
						try:
							os.remove(vhost_file)
						except OSError, e:
							self._logger.error('Couldn`t remove vhost file %s. %s', 
									vhost_file, e.strerror)
					
					if os.path.islink(vhost_file):
						try:
							os.unlink(vhost_file)
						except OSError, e:
								self._logger.error('Couldn`t remove vhost link %s. %s', 
										vhost_file, e.strerror)
			for vhost in received_vhosts:
				if (None == vhost.hostname) or (None == vhost.raw):
					continue
				
				if vhost.https:
					try:
						self._logger.debug("Retrieving ssl cert and private key from Scalr.")
						https_certificate = self._queryenv.get_https_certificate()
						self._logger.debug('Received certificate as %s type', type(https_certificate))
					except:
						self._logger.error('Can`t retrieve ssl cert and private key from Scalr.')
						raise
					else: 
						self._logger.info("Saving SSL certificates for %s",vhost.hostname)
						try:
							file = open(cert_path + '/' + 'https.key', 'w')
							file.write(https_certificate[1])
							file.close()
							
							file = open(cert_path + '/' + vhost.hostname + '.key', 'w')
							file.write(https_certificate[1])
							file.close()
						
							file = open(cert_path + '/' + 'https.crt', 'w')
							file.write(https_certificate[0])
							file.close()
						
							file = open(cert_path  + '/' + vhost.hostname + '.crt', 'w')
							file.write(https_certificate[0])
							file.close()
						
						except IOError, e:
							self._logger.error('Couldn`t write SSL certificate files to %s. %s', 
									cert_path, e.strerror)
					
					self._logger.info('Enabling SSL virtual host %s', vhost.hostname)
					
					try:
						vhost_fullpath = vhosts_path + '/' + vhost.hostname + '-ssl.vhost.conf'
						file = open(vhost_fullpath, 'w')
						file.write(vhost.raw.replace('/etc/aws/keys/ssl',cert_path))
						file.close()
					except IOError, e:
						self._logger.error('Couldn`t write to vhost file %s. %s', 
								vhost_fullpath, e.strerror)
					self._apache_vhost_create_paths(vhost_fullpath) 	
				
					self._logger.debug("Checking apache SSL mod")
					self._check_mod_ssl(httpd_conf_path)	
				
				elif not vhost.https:
					self._logger.info('Enabling virtual host %s', vhost.hostname)
					try:
						vhost_fullpath = vhosts_path + '/' + vhost.hostname + '.vhost.conf'
						file = open(vhost_fullpath, 'w')
						file.write(vhost.raw)
						file.close()
					except IOError, e:
						self._logger.error('Couldn`t write to vhost file %s. %s', 
										   vhost_fullpath, e.strerror)
					self._apache_vhost_create_paths(vhost_fullpath)
				else:
					self._logger.info('SSL is neither 0 or 1, skipping virtual host %s', vhost.hostname)
			
			if disttool.is_debian_based():
				self._apache_default_conf_patch_deb(vhosts_path)
			
			self._logger.debug("Checking if vhost directory included in main apache config")
			index = 0
			include_string = 'Include ' + vhosts_path + '/*'
			try:
				httpd_conf_file = open(httpd_conf_path, 'r')
				text = httpd_conf_file.read()
				httpd_conf_file.close()
				index = text.find(include_string)
			except IOError, e: 
				self._logger.error('Couldn`t read main config file %s. %s', 
						httpd_conf_path, e.strerror)
			if index == -1:
				self.make_backup_copy(httpd_conf_path)
				try:
					
					self._logger.debug("Writing changes to main config file %s.", 
							httpd_conf_path)
					httpd_conf_file = open(httpd_conf_path, 'a')
					httpd_conf_file.write(include_string)
					httpd_conf_file.close()
				except IOError, e:
					self._logger.error('Couldn`t write to main config file %s. %s', 
							httpd_conf_path, e.strerror)
	
	def _check_mod_ssl(self, httpd_conf_path):
		if disttool.is_debian_based():
			self._check_mod_ssl_deb(httpd_conf_path)
		elif disttool.is_redhat_based():
			self._check_mod_ssl_redhat(httpd_conf_path)
			
	def _check_mod_ssl_deb(self, httpd_conf_path):
		mods_available = os.path.dirname(httpd_conf_path) + '/mods-available'
		mods_enabled = os.path.dirname(httpd_conf_path) + '/mods-enabled'
		if not os.path.exists(mods_enabled + '/ssl.conf') and not os.path.exists(mods_enabled + '/ssl.load'):
			if os.path.exists(mods_available) and os.path.exists(mods_available+'/ssl.conf') and os.path.exists(mods_available+'/ssl.load'):
				if not os.path.exists(mods_enabled):
					try:
						self._logger.debug("Creating directory %s.",  
								mods_enabled)
						os.makedirs(mods_enabled)
					except OSError, e:
						self._logger.error('Couldn`t create directory %s. %s',  
								mods_enabled, e.strerror)
				try:
					self._logger.debug("Creating symlinks for mod_ssl files.",  
								mods_enabled)
					os.symlink(mods_available+'/ssl.conf', mods_enabled+'/ssl.conf')
					os.symlink(mods_available+'/ssl.load', mods_enabled+'/ssl.load')
					self._logger.info('SSL module has been enabled')
				except OSError, e:
						self._logger.error('Couldn`t create symlinks for ssl.conf and ssl.load in %s. %s', 
								mods_enabled, e.strerror)
			else:
				self._logger.error('%s directory doesn`t exist or doesn`t contain valid ssl.conf and ssl.load files', 
						mods_available)
				
	def _check_mod_ssl_redhat(self, httpd_conf_path):
		modules_dir = os.path.dirname(httpd_conf_path) + '/modules'
		mod_ssl_file = modules_dir + '/mod_ssl.so'
		include_mod_ssl = 'LoadModule mod_ssl modules/mod_ssl.so'
		if not os.path.isfile(mod_ssl_file) and not os.path.islink(mod_ssl_file):
			self._logger.error('mod_ssl file %s does not exist. Try "sudo yum install mod_ssl" ',
						mod_ssl_file)
		else:
			text = ''
			
			try:
				httpd_conf_file = open(httpd_conf_path, 'r')
				text = httpd_conf_file.read()
				httpd_conf_file.close()
			except IOError, e: 
				self._logger.error('Couldn`t read main config file %s. %s', 
						httpd_conf_path, e.strerror)
			else:
				if not text:
					self._logger.error('%s is empty', httpd_conf_path)
			index = text.find('mod_ssl.so')

			if text and index == -1:
				
				self.make_backup_copy(httpd_conf_path)
					
				self._logger.info('%s does not contain loading mod_ssl string. Trying to patch.',
							httpd_conf_path)
				
				position = text.find("LoadModule")
				if position == -1:
					updated_text = text + '\n' + include_mod_ssl + '\n'
				else:
					updated_text = text[:position] + '\n' + include_mod_ssl  + '\n' + text[position:]
				try:
					self._logger.debug("Writing changes to main config file %s.", 
							httpd_conf_path)
					httpd_conf_file = open(httpd_conf_path, 'w')
					httpd_conf_file.write(updated_text)
					httpd_conf_file.close()
				except IOError, e:
					self._logger.error('Couldn`t write to main config file %s. %s', 
							httpd_conf_path, e.strerror)
	
	def make_backup_copy(self, path):
		backup_file = path + '.bak'
		self._logger.debug('Copying %s to %s',
				path, backup_file)
		if not os.path.exists(backup_file):
			try:
				shutil.copy(path, backup_file)
			except IOError, e:
				self._logger.error('Couldn`t backup %s to %s', 
						path, backup_file, e.strerror)
	
	def _reload_apache(self):
		if disttool.is_debian_based():
			init_script = '/etc/init.d/apache2'
		elif disttool.is_redhat_based():
			init_script = '/etc/init.d/httpd'
		else:
			init_script = '/etc/init.d/httpd'
		reload_command = [init_script, "reload"]
		if os.path.exists(init_script) and os.access(init_script, os.X_OK):
			self._logger.info("Trying to reload apache..")
			try:
				out, err, retcode = system(reload_command, shell=False)
				if retcode or (out and out.find("FAILED") != -1):
					self._logger.error("Apache reloading failed. %s", out)
				else:
					self._logger.info("Apache was successfully reloaded")
					bus.fire('apache_reload')
						
			except OSError, e:
				self._logger.error('Apache realoading failed by running %s. %s', 
						''.join(reload_command), e.strerror)	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return Behaviours.APP in behaviour and message.name == Messages.VHOST_RECONFIGURE
	
	def _apache_default_conf_patch_deb(self, vhosts_path):
		self._logger.debug("Replacing NameVirtualhost and Virtualhost ports especially for debian-based linux")
		default_vhost_path = vhosts_path + '/' + '000-default'
		if os.path.exists(default_vhost_path):
			try:
				default_vhost_file = open(default_vhost_path, 'r')
				default_vhost = default_vhost_file.read()
				default_vhost_file.close()
			except IOError, e: 
				self._logger.error('Couldn`t read default vhost config file %s. %s', 
						default_vhost_path, e.strerror)
			else:
				default_vhost = self.name_vhost_regexp.sub('NameVirtualHost *:80\n', default_vhost)
				default_vhost = self.vhost_regexp.sub( '<VirtualHost *:80>', default_vhost)
				try:
					default_vhost_file = open(default_vhost_path, 'w')
					default_vhost_file.write(default_vhost)
					default_vhost_file.close()
				except IOError, e:
					self._logger.error('Couldn`t write to default vhost config file %s. %s', 
							default_vhost_path, e.strerror)

	def _apache_vhost_create_paths(self, vhost_path):
		if os.path.exists(vhost_path):
			try:
				vhost_file = open(vhost_path, 'r')
				vhost = vhost_file.read()
				vhost_file.close()
			except IOError, e: 
						self._logger.error('Couldn`t read vhost config file %s. %s', 
								vhost_path, e.strerror)
			else:
				vhost = re.sub(self.strip_comments_regexp, '', vhost, re.S)
				list_logs = self.errorlog_regexp.findall(vhost) + self.customlog_regexp.findall(vhost)
				
				dir_list = []
				for log_file in list_logs: 
					log_dir = os.path.dirname(log_file)
					if (log_dir not in dir_list) and (not os.path.exists(log_dir)): 
						dir_list.append(log_dir)
						
				for log_dir in dir_list:
					try:
						os.makedirs(log_dir)
						self._logger.info('Created log directory %s', log_dir)
					except OSError, e:
						self._logger.error('Couldn`t create directory %s. %s', 
								log_dir, e.strerror)
		