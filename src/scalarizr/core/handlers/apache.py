'''
Created on Dec 25, 2009

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.core import Bus, BusEntries, Behaviours
from scalarizr.core.handlers import Handler
from scalarizr.messaging import Messages
import logging
import os
import platform
import re
import subprocess
#from scalarizr.core.handlers import hooks

def get_handlers ():
	return [ApacheHandler()]

class ApacheHandler(Handler):
	_logger = None
	_queryenv = None
	_bus = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = Bus()[BusEntries.QUERYENV_SERVICE]
		self.name_vhost_regexp = re.compile(r'NameVirtualHost\s+\*[^:]')
		self.vhost_regexp = re.compile('<VirtualHost\s+\*>')
		self.strip_comments_regexp = re.compile( r"#.*\n")
		self.errorlog_regexp = re.compile( r"ErrorLog\s+(\S*)", re.IGNORECASE)
		self.customlog_regexp = re.compile( r"CustomLog\s+(\S*)", re.IGNORECASE)
		self.bus = Bus()
		self.bus.define_events('apache_reload')
	
	def on_VhostReconfigure(self, message):
		self._logger.debug("Entering on_VhostReconfigure")
		
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self._reload_apache()

	def _update_vhosts(self):
				
		bus = Bus()
		config = bus[BusEntries.CONFIG]
		vhosts_path = config.get("handler_apache","vhosts_path")
		if vhosts_path[:-1] != os.sep:
			vhosts_path += os.sep
		httpd_conf_path = config.get("handler_apache","httpd_conf_path")
		
		received_vhosts = []
		try:
			received_vhosts = self._queryenv.list_virtual_hosts()
		except:
			self._logger.error('Can`t retrieve virtual hosts list from Scalr')
			raise
		if [] != received_vhosts:	
			#clean up old configuration
			list_vhosts = os.listdir(vhosts_path)
			
			if not os.path.exists(vhosts_path):
				self._logger.error('vhosts_path ' + vhosts_path + ' doesn`t exist')
			
			elif [] == list_vhosts:
				self._logger.info("Virtualhost list is empty")
			
			elif [] != list_vhosts:
				for fname in list_vhosts:
					if '000-default' == fname:
						continue
					vhost_file = vhosts_path + fname
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
									self._logger.error('Couldn`t remove vhost link ' + \
													   vhost_file + " : " + str(e.strerror))
				
				for vhost in received_vhosts:
					if (None == vhost.hostname) or (None == vhost.raw):
						continue
					
					if True == vhost.https:
						try:
							https_certificate = self._queryenv.get_https_certificate()
						except:
							self._logger.error('Can`t retrieve ssl cert and private key from Scalr')
							raise
						else: 
							try:
								cert_path = "etc/.keys/"
								print os.path.realpath(cert_path)
								
								file = open(cert_path + "https.key", "w")
								file.write(https_certificate[1])
								file.close()
								
								file = open(cert_path + vhost.hostname + ".key", "w")
								file.write(https_certificate[1])
								file.close()
							
								file = open(cert_path + "https.crt", "w")
								file.write(https_certificate[0])
								file.close()
							
								file = open(cert_path  + vhost.hostname + ".crt", "w")
								file.write(https_certificate[0])
								file.close()
							
							except IOError, e:
								self._logger.error('Couldn`t write SSL certificate files to ' + \
												   cert_path + " : " + str(e.strerror))
						
						self._logger.info("Enabling SSL virtual host " + vhost.hostname)
						try:
							vhost_fullpath = vhosts_path + vhost.hostname + "-ssl.vhost.conf"
							file = open(vhost_fullpath, "w")
							file.write(vhost.raw)
							file.close()
						except IOError, e:
							self._logger.error('Couldn`t write to vhost file ' + \
											   vhost_fullpath + " : " + str(e.strerror))
						self._apache_vhost_create_paths(vhost_fullpath) 	
					
						#it also needs to take care of apache SSL mod
						self._check_mod_ssl(httpd_conf_path)	
					
					elif False == vhost.https:
						self._logger.info("Enabling virtual host " + vhost.hostname)
						try:
							vhost_fullpath = vhosts_path + vhost.hostname + ".vhost.conf"
							file = open(vhost_fullpath, "w")
							file.write(vhost.raw)
							file.close()
						except IOError, e:
							self._logger.error('Couldn`t write to vhost file ' + \
											   vhost_fullpath + " : " + str(e.strerror))
						self._apache_vhost_create_paths(vhost_fullpath)
					else:
						self._logger.info("SSL is neither 0 or 1, skipping virtual host " + vhost.hostname)
				
				if ["Ubuntu","Debian"].count(platform.dist()[0]):
					self._apache_deault_conf_patch_deb(vhosts_path)
				
				#Check if vhost directory included in main apache config
				index = 0
				include_string = 'Include "' + vhosts_path + '*"'
				try:
					text = open(httpd_conf_path, "r").read()
					index = text.find(include_string)
				except IOError, e: 
					self._logger.error('Couldn`t read main config file ' + \
											   httpd_conf_path + " : " + str(e.strerror))
				if index == -1:
					try:
						httpd_conf_file = open(httpd_conf_path, "w")
						httpd_conf_file.write(include_string)
						httpd_conf_file.close()
					except IOError, e:
						self._logger.error('Couldn`t write to main config file ' + \
													   httpd_conf_path + " : " + str(e.strerror))
	
	def _check_mod_ssl(self, httpd_conf_path):
		if ["Ubuntu","Debian"].count(platform.dist()[0]):
			self._check_mod_ssl_deb(httpd_conf_path)
			
	def _check_mod_ssl_deb(self, httpd_conf_path):
		mods_available = os.path.dirname(httpd_conf_path) + "/mods_available/"
		mods_enabled = os.path.dirname(httpd_conf_path) + "/mods_enabled/"
		if not os.path.exists(mods_enabled + "ssl.conf") and not os.path.exists(mods_enabled + "ssl.load"):
			if os.path.exists(mods_available) and os.path.exists(mods_available+"ssl.conf") and os.path.exists(mods_available+"ssl.load"):
				if not os.path.exists(mods_enabled):
					try:
						os.makedirs(mods_enabled)
					except OSError, e:
						self._logger.error('Couldn`t create directory ' + \
										   mods_enabled + " : " + str(e.strerror))
				try:
					os.symlink(mods_available+"ssl.conf", mods_enabled+"ssl.conf")
					os.symlink(mods_available+"ssl.load", mods_enabled+"ssl.load")
					self._logger.info("SSL module has been enabled")
				except OSError, e:
						self._logger.error('Couldn`t create symlinks for ssl.conf and ssl.load in ' + \
										   mods_enabled + " : " + str(e.strerror))
			else:
				self._logger.error(mods_available + ' directory doesn`t exist or doesn`t contain valid ssl.conf and ssl.load files')
		
	
	def _reload_apache(self):
		if ["Ubuntu","Debian"].count(platform.dist()[0]):
			self._reload_apache_deb()
			
	def _reload_apache_deb(self):
		apache_run_script = '/etc/init.d/apache2'
		apache_run_args = 'reload'
		reload_command = [apache_run_script, apache_run_args]
		if os.path.exists(apache_run_script) and os.access(apache_run_script, os.X_OK):
			try:
				p = subprocess.Popen(reload_command, 
									 stdin=subprocess.PIPE, 
									 stdout=subprocess.PIPE, 
									 stderr=subprocess.PIPE)								
				stdout, stderr = p.communicate()
					
				is_start_failed = p.poll()
								
				if is_start_failed:
					self._logger.error(stderr)
				
				else:
					self.bus.fire('apache_reload')	
									
				if None != stdout:
					self._logger.info(stdout)	
			except OSError, e:
				self._logger.error(str('Apache realoading failed by running ' + str(reload_command)))		
			
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return behaviour == Behaviours.APP and message.name == Messages.VHOST_RECONFIGURE
	
	def _apache_deault_conf_patch_deb(self, vhosts_path):
		#Replace NameVirtualhost and Virtualhost ports specifically for debian-based linux
		default_vhost_path = vhosts_path + "000-default"
		if os.path.exists(default_vhost_path):
			try:
				default_vhost = open(default_vhost_path, "r").read()
			except IOError, e: 
				self._logger.error('Couldn`t read default vhost config file ' + \
								   default_vhost_path + " : " + str(e.strerror))
			else:
				default_vhost = self.name_vhost_regexp.sub('NameVirtualHost *:80\n', default_vhost)
				default_vhost = self.vhost_regexp.sub( '<VirtualHost *:80>', default_vhost)
				try:
					default_vhost_file = open(default_vhost_path, "w")
					default_vhost_file.write(default_vhost)
					default_vhost_file.close()
				except IOError, e:
					self._logger.error('Couldn`t write to default vhost config file ' + \
									   default_vhost_path + " : " + str(e.strerror))

	def _apache_vhost_create_paths(self, vhost_path):
		if os.path.exists(vhost_path):
			try:
				vhost = open(vhost_path, "r").read()
			except IOError, e: 
						self._logger.error('Couldn`t read vhost config file ' + \
											   vhost_path + " : " + str(e.strerror))
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
					except OSError, e:
						self._logger.error('Couldn`t create directory ' + \
										   log_dir + " : " + str(e.strerror))
		