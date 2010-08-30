'''
Created on Dec 25, 2009

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Messages
from scalarizr.util import disttool, backup_file, initd, configtool, \
	cached, firstmatched, validators
from scalarizr.util.filetool import read_file, write_file
from scalarizr.util.initd import InitdError
import logging
import os
import re




BEHAVIOUR = BuiltinBehaviours.APP
CNF_SECTION = BEHAVIOUR
CNF_NAME = BEHAVIOUR + '.ini'


if disttool.is_redhat_based():
	initd_script = "/etc/init.d/httpd"
	pid_file = "/var/run/httpd/httpd.pid"
elif disttool.is_debian_based():
	initd_script = "/etc/init.d/apache2"
	
	pid_file = None
	# Find option value
	
	env_vars = read_file("/etc/apache2/envvars")
	if env_vars:
		m = re.search("export\sAPACHE_PID_FILE=(.*)", env_vars)
		if m:
			pid_file = m.group(1)
	
else:
	initd_script = "/etc/init.d/apache2"
	
if not os.path.exists(initd_script):
	raise HandlerError("Cannot find Apache init script at %s. Make sure that apache web server is installed" % initd_script)

# Register apache service
logger = logging.getLogger(__name__)
logger.debug("Explore apache service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("apache", initd_script, pid_file)


# Export behavior configuration
class ApacheOptions(Configurator.Container):
	'''
	app behavior
	'''
	cnf_name = CNF_NAME
	
	class apache_conf_path(Configurator.Option):
		'''
		Apache configuration file location.
		'''
		name = CNF_SECTION + '/apache_conf_path'
		required = True
		
		@property 
		@cached
		def default(self):
			return firstmatched(lambda p: os.path.exists(p),
					('/etc/apache2/apache2.conf', '/etc/httpd/conf/httpd.conf'), '')
		
		@validators.validate(validators.file_exists)		
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
		
		value = property(Configurator.Option._get_value, _set_value)

	
	class vhosts_path(Configurator.Option):
		'''
		Directory to create virtual hosts configuration in.
		All Apache virtual hosts, created in the Scalr user interface are placed in a separate
		directory and included to the main Apache configuration file.
		'''
		name = CNF_SECTION + '/vhosts_path'
		default = 'private.d/vhosts'
		required = True



def get_handlers ():
	return [ApacheHandler()]

class ApacheHandler(Handler):
	_logger = None
	_queryenv = None
	_cnf = None
	'''
	@type _cnf: scalarizr.config.ScalarizrCnf
	'''

	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._cnf = bus.cnf
		self.name_vhost_regexp = re.compile(r'NameVirtualHost\s+\*[^:]')
		self.vhost_regexp = re.compile('<VirtualHost\s+\*>')
		self.strip_comments_regexp = re.compile( r"#.*\n")
		self.errorlog_regexp = re.compile( r"ErrorLog\s+(\S*)", re.IGNORECASE)
		self.customlog_regexp = re.compile( r"CustomLog\s+(\S*)", re.IGNORECASE)
		self.load_module_regexp = re.compile(r"\nLoadModule\s+",re.IGNORECASE)
		self.server_root_regexp = re.compile(r"ServerRoot\s+\"(\S*)\"",re.IGNORECASE)
		self.ssl_conf_name_vhost_regexp = re.compile(r"NameVirtualHost\s+\*:\d+\n",re.IGNORECASE)
		self.ssl_conf_listen_regexp = re.compile(r"Listen\s+\d+\n",re.IGNORECASE)
		bus.define_events('apache_reload')
		bus.on("init", self.on_init)

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and message.name == Messages.VHOST_RECONFIGURE

	def on_VhostReconfigure(self, message):
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self._reload_apache()

	def on_init(self):
		bus.on("start", self.on_start)
		bus.on('before_host_up', self.on_before_host_up)
		bus.on("before_host_down", self.on_before_host_down)
				

	def on_start(self, *args):
		if self._cnf.state == ScalarizrState.RUNNING:
			try:
				self._logger.info("Starting Apache")
				initd.start("apache")
			except initd.InitdError, e:
				self._logger.error(e)

	on_before_host_up = on_start
		
	def on_before_host_down(self, *args):
		try:
			self._logger.info("Stopping Apache")
			initd.stop("apache")
		except initd.InitdError, e:
			self._logger.error("Cannot stop apache")
			if initd.is_running("apache"):
				raise

	def _update_vhosts(self):
				
		config = bus.config
		vhosts_path = os.path.join(bus.etc_path,config.get(CNF_SECTION,'vhosts_path'))
		httpd_conf_path = config.get(CNF_SECTION,'apache_conf_path')
		cert_path = bus.etc_path + '/private.d/keys'	
		
		self.server_root = self._get_server_root(httpd_conf_path)
			
		self._logger.debug("Requesting virtual hosts list")
		received_vhosts = self._queryenv.list_virtual_hosts()
		self._logger.debug("Virtual hosts list obtained (num: %d)", len(received_vhosts))
		
		if [] != received_vhosts:
			if not os.path.exists(vhosts_path):
				if not vhosts_path:
					self._logger.error('Property vhosts_path is empty.')
				else:
					self._logger.warning("Virtual hosts dir %s doesn't exist. Create it", vhosts_path)
					try:
						os.makedirs(vhosts_path)
						self._logger.debug("Virtual hosts dir %s created", vhosts_path)
					except OSError, e:
						self._logger.error("Cannot create dir %s. %s", vhosts_path, e.strerror)
						raise
			
			list_vhosts = os.listdir(vhosts_path)
			if [] != list_vhosts:
				self._logger.debug("Deleting old vhosts configuration files")
				for fname in list_vhosts:
					if '000-default' == fname:
						continue
					vhost_file = vhosts_path + '/' + fname
					if os.path.isfile(vhost_file):
						try:
							os.remove(vhost_file)
						except OSError, e:
							self._logger.error('Cannot delete vhost file %s. %s', vhost_file, e.strerror)
					
					if os.path.islink(vhost_file):
						try:
							os.unlink(vhost_file)
						except OSError, e:
							self._logger.error('Cannot delete vhost link %s. %s', vhost_file, e.strerror)
				self._logger.debug("Old vhosts configuration files deleted")

			self._logger.debug("Creating new vhosts configuration files")
			for vhost in received_vhosts:
				if (None == vhost.hostname) or (None == vhost.raw):
					continue
				
				self._logger.debug("Processing %s", vhost.hostname)
				if vhost.https:
					try:
						self._logger.debug("Retrieving ssl cert and private key from Scalr.")
						https_certificate = self._queryenv.get_https_certificate()
						self._logger.debug('Received certificate as %s type', type(https_certificate))
					except:
						self._logger.error('Cannot retrieve ssl cert and private key from Scalr.')
						raise
					else: 
						if not https_certificate[0]:
							self._logger.error("Scalar returned empty SSL cert")
						elif not https_certificate[1]:
							self._logger.error("Scalar returned empty SSL key")
						else:
							self._logger.debug("Saving SSL certificates for %s",vhost.hostname)
							
							key_error_message = 'Cannot write SSL key files to %s.' % cert_path
							cert_error_message = 'Cannot write SSL certificate files to %s.' % cert_path
							
							for key_file in ['https.key', vhost.hostname + '.key']:
								write_file(cert_path + '/' + key_file, https_certificate[1], error_msg=key_error_message, logger=self._logger)
														
							for cert_file in ['https.crt', vhost.hostname + '.crt']:
								write_file(cert_path + '/' + cert_file, https_certificate[0], error_msg=cert_error_message, logger=self._logger)
					
					self._logger.debug('Enabling SSL virtual host %s', vhost.hostname)
					
					vhost_fullpath = vhosts_path + '/' + vhost.hostname + '-ssl.vhost.conf'
					vhost_error_message = 'Cannot write vhost file %s.' % vhost_fullpath
					write_file(vhost_fullpath, vhost.raw.replace('/etc/aws/keys/ssl',cert_path), error_msg=vhost_error_message, logger = self._logger)
					
					self._create_vhost_paths(vhost_fullpath) 	
				
					self._logger.debug("Checking apache SSL mod")
					self._check_mod_ssl(httpd_conf_path)
					
					self._logger.debug("Changing paths in ssl.conf")
					self._patch_ssl_conf(cert_path)
			
				else:
					self._logger.debug('Enabling virtual host %s', vhost.hostname)
					vhost_fullpath = vhosts_path + '/' + vhost.hostname + '.vhost.conf'
					vhost_error_message = 'Cannot write vhost file %s.' % vhost_fullpath
					write_file(vhost_fullpath, vhost.raw, error_msg=vhost_error_message, logger=self._logger)
					
				self._logger.debug("Done %s processing", vhost.hostname)
			self._logger.debug("New vhosts configuration files created")
			
			if disttool.is_debian_based():
				self._patch_default_conf_deb(httpd_conf_path)
			
			self._logger.debug("Checking that vhosts directory included in main apache config")
			index = 0
			include_string = 'Include ' + vhosts_path + '/*'
			
			error_message = 'Cannot read main config file %s' % httpd_conf_path
			text = read_file(httpd_conf_path, error_msg=error_message, logger=self._logger)
			if text:
				index = text.find(include_string)

			if index == -1:
				backup_file(httpd_conf_path)
				msg = "Writing changes to main config file %s." % httpd_conf_path
				error_message = 'Cannot write to main config file %s' % httpd_conf_path
				write_file(httpd_conf_path, include_string, msg=msg, mode = 'a', error_msg=error_message, logger=self._logger)


	def _patch_ssl_conf(self, cert_path):
		key_path = cert_path + '/https.key'
		crt_path = cert_path + '/https.crt'
		
		if disttool.is_debian_based():
			ssl_conf_path = self.server_root + '/sites-available/default-ssl'
		elif disttool.is_redhat_based():
			ssl_conf_path = self.server_root + '/conf.d/ssl.conf'
			
		ssl_conf = read_file(ssl_conf_path,logger = self._logger)
		if ssl_conf:
			cert_file_re = re.compile('^([^#\n]*SSLCertificateFile).*?$', re.M)
			cert_key_file_re = re.compile('^([^#\n]*SSLCertificateKeyFile).*?$', re.M)
			
			new_ssl_conf = re.sub(cert_file_re, '\\1\t'+crt_path, ssl_conf)
			new_ssl_conf = re.sub(cert_key_file_re, '\\1\t'+key_path, new_ssl_conf)
			
			write_file(ssl_conf_path, new_ssl_conf, logger = self._logger)
	
	
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
						self._logger.error('Cannot create directory %s. %s',  
								mods_enabled, e.strerror)
				try:
					self._logger.debug("Creating symlinks for mod_ssl files.", mods_enabled)
					os.symlink(mods_available+'/ssl.conf', mods_enabled+'/ssl.conf')
					os.symlink(mods_available+'/ssl.load', mods_enabled+'/ssl.load')
					self._logger.debug('SSL module has been enabled')
				except OSError, e:
					self._logger.error('Cannot create symlinks for ssl.conf and ssl.load in %s. %s', 
							mods_enabled, e.strerror)
			else:
				self._logger.error('%s directory doesn`t exist or doesn`t contain valid ssl.conf and ssl.load files', 
						mods_available)
			
				
	def _check_mod_ssl_redhat(self, httpd_conf_path):
		include_mod_ssl = 'LoadModule ssl_module modules/mod_ssl.so'			
		mod_ssl_file = self.server_root + '/modules/mod_ssl.so'
		
		if not os.path.isfile(mod_ssl_file) and not os.path.islink(mod_ssl_file):
			self._logger.error('%s does not exist. Try "sudo yum install mod_ssl" ',
						mod_ssl_file)
		else:			
			#ssl.conf part
			ssl_conf_path = self.server_root + '/conf.d/ssl.conf'
			ssl_conf_minimal = "Listen 443\nNameVirtualHost *:443\n"
			ssl_conf_file = None
			ssl_conf_str = None	
			ssl_conf_str_updated = None
			
			if not os.path.exists(ssl_conf_path):
				self._logger.error("SSL config %s doesn`t exist", ssl_conf_path)
			else:
				
				error_message = 'Cannot read SSL config file %s' % ssl_conf_path
				ssl_conf_str = read_file(ssl_conf_path, error_msg=error_message, logger=self._logger)
				
				if not ssl_conf_str:
					self._logger.error("SSL config file %s is empty. Filling in with minimal configuration.", ssl_conf_path)
					ssl_conf_str_updated = ssl_conf_minimal
						
				else:
					
					if not self.ssl_conf_name_vhost_regexp.search(ssl_conf_str):
						self._logger.debug("NameVirtualHost directive not found in %s", ssl_conf_path)
						listen_pos = self.ssl_conf_listen_regexp.search(ssl_conf_str)
						if not listen_pos:
							self._logger.debug("Listen directive not found in %s. ", ssl_conf_path)
							self._logger.debug("Patching %s with Listen & NameVirtualHost directives.",
										ssl_conf_path)
							ssl_conf_str_updated = ssl_conf_minimal + ssl_conf_str
						else:
							self._logger.debug("NameVirtualHost directive inserted after Listen directive.")
							listen_index = listen_pos.end()
							ssl_conf_str_updated = ssl_conf_str[:listen_index] + '\nNameVirtualHost *:443\n' + ssl_conf_str[listen_index:]
				if ssl_conf_str_updated:
					error_message = 'Cannot save SSL config file %s' % ssl_conf_path
					write_file(ssl_conf_path, ssl_conf_str_updated, error_msg=error_message, logger=self._logger)
			
			#apache.conf part
			error_message = 'Cannot read httpd config file %s' % httpd_conf_path
			conf_str = read_file(httpd_conf_path, error_msg=error_message, logger=self._logger)
			index = conf_str.find('mod_ssl.so')
			
			if conf_str and index == -1:
				error_message = 'Cannot read mod_ssl config %s' % httpd_conf_path
				ssl_conf_str = read_file(mod_ssl_file, error_msg=error_message, logger=self._logger)
				index = ssl_conf_str.find('mod_ssl.so')
				
				if ssl_conf_str and index == -1:
					backup_file(httpd_conf_path)
					self._logger.debug('%s does not contain mod_ssl include. Patching httpd conf.',
								httpd_conf_path)
					
					pos = self.load_module_regexp.search(conf_str)
					conf_str_updated = conf_str[:pos.start()] + '\n' + include_mod_ssl  + '\n' + conf_str[pos.start():] if pos else \
							conf_str + '\n' + include_mod_ssl + '\n'
						
					self._logger.debug("Writing changes to httpd config file %s.", httpd_conf_path)
					error_message = 'Cannot save httpd config file %s' % httpd_conf_path
					write_file(httpd_conf_path, conf_str_updated, error_msg=error_message, logger=self._logger)
		
	
	def _get_server_root(self, httpd_conf_path):
		if disttool.is_debian_based():
			server_root = '/etc/apache2'
		
		elif disttool.is_redhat_based():
			self._logger.debug("Searching in apache config file %s to find server root",
					httpd_conf_path)
			error_message = 'Cannot read httpd config file %s' % httpd_conf_path
			conf_str = read_file(httpd_conf_path, error_msg=error_message, logger=self._logger)
	
			if conf_str:		
				server_root_entries = self.server_root_regexp.findall(conf_str)
			
			if server_root_entries:
				server_root = server_root_entries[0]
				self._logger.debug("ServerRoot found: %s", server_root)
			else:
				self._logger.warning("ServerRoot not found in apache config file %s", httpd_conf_path)
				server_root = os.path.dirname(httpd_conf_path)
				self._logger.debug("Use %s as ServerRoot", server_root)
				
		return server_root
	
	
	def _reload_apache(self):
		try:
			initd.reload("apache")
		except InitdError, e:
			self._logger.error(e)
	
	
	def _patch_default_conf_deb(self, httpd_conf_path):
		self._logger.debug("Replacing NameVirtualhost and Virtualhost ports especially for debian-based linux")
		default_vhost_path = os.path.dirname(httpd_conf_path) + '/sites-enabled' + '/' + '000-default'
		
		error_message = 'Cannot read default vhost config file %s' % default_vhost_path
		default_vhost = read_file(default_vhost_path, error_msg=error_message, logger=self._logger)
		
		if default_vhost:
			default_vhost = self.name_vhost_regexp.sub('NameVirtualHost *:80\n', default_vhost)
			default_vhost = self.vhost_regexp.sub( '<VirtualHost *:80>', default_vhost)
			error_message = 'Cannot write to default vhost config file %s' % default_vhost_path
			write_file(default_vhost_path, default_vhost, error_msg=error_message, logger=self._logger)


	def _create_vhost_paths(self, vhost_path):
		error_message = 'Couldn`t read vhost config file %s' % vhost_path
		vhost = read_file(vhost_path, error_msg=error_message, logger=self._logger)
		
		if vhost:
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
					self._logger.debug('Created log directory %s', log_dir)
				except OSError, e:
					self._logger.error('Couldn`t create directory %s. %s', 
							log_dir, e.strerror)
		