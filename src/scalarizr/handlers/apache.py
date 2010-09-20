'''
Created on Dec 25, 2009

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import Handler, HandlerError, ServiceCtlHanler
from scalarizr.messaging import Messages
from scalarizr.util import disttool, backup_file,  \
	cached, firstmatched, validators, software
from scalarizr.util.filetool import read_file, write_file
import logging
import os
import re
from scalarizr.util import initdv2, system
from telnetlib import Telnet
from scalarizr.libs.metaconf import Configuration, ParseError, MetaconfError,\
	PathNotExistsError
from scalarizr.service import CnfPresetStore, CnfPreset, CnfController, Options


BEHAVIOUR = BuiltinBehaviours.APP
CNF_SECTION = BEHAVIOUR
CNF_NAME = BEHAVIOUR + '.ini'
APP_CONF_PATH = 'apache_conf_path'
class ApacheInitScript(initdv2.ParametrizedInitScript):
	
	def __init__(self):
		if disttool.is_redhat_based():
			initd_script = "/etc/init.d/httpd"
		elif disttool.is_debian_based():
			initd_script = "/etc/init.d/apache2"
	
		if not os.path.exists(initd_script):
			raise HandlerError("Cannot find Apache init script at %s. Make sure that apache web server is installed" % initd_script)

		pid_file = None
		
		env_vars = read_file("/etc/apache2/envvars")
		if env_vars:
			m = re.search("export\sAPACHE_PID_FILE=(.*)", env_vars)
			if m:
				pid_file = m.group(1)
		
		initdv2.ParametrizedInitScript.__init__(self, 'apache', 
				initd_script, pid_file, socks=[initdv2.SockParam(80)])
		
	def status(self):
		status = initdv2.ParametrizedInitScript.status(self)
		if not status and self.socks:
			ip, port = self.socks[0].conn_address
			telnet = Telnet(ip, port)
			telnet.write('hello\n')
			if 'apache' in telnet.read_all().lower():
				return initdv2.Status.RUNNING
			return initdv2.Status.NOT_RUNNING
		return status
	
	def configtest(self):
		if not hasattr(self, 'app_ctl'):
			if disttool.is_redhat_based():
				app_ctl = "apachectl"
			elif disttool.is_debian_based():
				app_ctl = "apache2ctl"
		out = system(app_ctl +' configtest')[1]
		if 'error' in out.lower():
			raise initdv2.InitdError("Configuration isn't valid: %s" % out)

initdv2.explore('apache', ApacheInitScript)



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

class ApacheCnfController(CnfController):
	
	class OptionSpec:
		name = None
		default_value = None
		supported_from = None
		
		def __init__(self, name, default_value = None, supported_from = None):
			self.name = name			
			self.default_value = default_value
			self.supported_from = supported_from
					
	options = Options(
		# http://httpd.apache.org/docs/current/mod/core.html
		OptionSpec(name = 'DocumentRoot', default_value = '/usr/local/apache/htdocs'),
		OptionSpec(name = 'EnableMMAP', default_value = 'On'),
		OptionSpec(name = 'EnableSendfile', default_value = 'On'),
		OptionSpec(name = 'ErrorDocument'),
		OptionSpec(name = 'ErrorLog', default_value = 'logs/error_log'),
		OptionSpec(name = 'DefaultType', default_value = 'text/plain'),
		OptionSpec(name = 'KeepAlive', default_value = 'On'),
		OptionSpec(name = 'KeepAliveTimeout', default_value = '5'),
		OptionSpec(name = 'LimitInternalRecursion', default_value = '10'),
		OptionSpec(name = 'LimitRequestBody', default_value = '0'),
		OptionSpec(name = 'LimitRequestFields', default_value = '100'),
		OptionSpec(name = 'LimitRequestFieldSize', default_value = '8190'),
		OptionSpec(name = 'LimitRequestLine', default_value = '8190'),
		OptionSpec(name = 'LimitXMLRequestBody', default_value = '1000000'),
		OptionSpec(name = 'LogLevel', default_value = 'warn'),
		OptionSpec(name = 'MaxKeepAliveRequests', default_value = '100'),
		OptionSpec(name = 'NameVirtualHost'),
		OptionSpec(name = 'Options', default_value = 'All'),
		#OptionSpec(name = 'Require'), #Context:	directory, .htaccess
		OptionSpec(name = 'RLimitCPU'), #Default:	Unset; uses operating system defaults
		OptionSpec(name = 'RLimitMEM'), #Default:	Unset; uses operating system defaults
		OptionSpec(name = 'RLimitNPROC'), #Default:	Unset; uses operating system defaults
		OptionSpec(name = 'Satisfy', default_value = 'All'),
		OptionSpec(name = 'ScriptInterpreterSource', default_value = 'Script'),
		OptionSpec(name = 'ServerAdmin'),
		#OptionSpec(name = 'ServerAlias'), #Context:	virtual host
		OptionSpec(name = 'ServerName'),
		#OptionSpec(name = 'ServerPath'), #Context:	virtual host
		OptionSpec(name = 'ServerRoot', default_value = '/usr/local/apache'),
		OptionSpec(name = 'ServerSignature', default_value = 'Off'),
		OptionSpec(name = 'ServerTokens', default_value = 'Full'),
		OptionSpec(name = 'SetHandler'),
		OptionSpec(name = 'SetInputFilter'),
		OptionSpec(name = 'SetOutputFilter'),
		OptionSpec(name = 'TimeOut', default_value = '300'),
		OptionSpec(name = 'TraceEnable', default_value = 'on'),
		OptionSpec(name = 'UseCanonicalName', default_value = 'Off'),
		)
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._config = ini.get(CNF_SECTION, APP_CONF_PATH)
		
	def current_preset(self):
		self._logger.debug('Getting current Apache preset')
		preset = CnfPreset(name='current')
		
		conf = Configuration('apache')
		conf.read(self._config)
		
		vars = {}
		
		for option_spec in self.options:
			try:
				vars[option_spec.name] = conf.get(option_spec.name)
			except PathNotExistsError:
				self._logger.debug('%s does not exist in %s. Using default value' 
						%(option_spec.name, self._config))

				if option_spec.default_value:
					vars[option_spec.name] = option_spec.default_value
				else:
					self._logger.error('default value for %s not found'%(option_spec.name))
				
		preset.settings = vars
		return preset
	

	def apply_preset(self, preset):
		self._logger.debug('Applying %s preset' % (preset.name if preset.name else 'undefined'))
				
		conf = Configuration('apache')
		conf.read(self._config)
		
		for option_spec in self.options:
			if preset.settings.has_key(option_spec.name):
				
				# Skip unsupported
				if option_spec.supported_from and option_spec.supported_from > self._get_apache_version():
					self._logger.debug('%s supported from %s. Cannot apply.' 
							% (option_spec.name, option_spec.supported_from))
					continue
								
				if not option_spec.default_value:
					self._logger.debug('No default value for %s' % option_spec.name)
					
				elif preset.settings[option_spec.name] == option_spec.default_value:
					try:
						conf.remove(option_spec.name)
						self._logger.debug('%s value is equal to default. Removed from config.' % option_spec.name)
					except PathNotExistsError:
						pass
					continue	

					if conf.get(option_spec.name) == preset.settings[option_spec.name]:
						self._logger.debug('Variable %s wasn`t changed. Skipping.' % option_spec.name)
					else:
						self._logger.debug('Setting variable %s to %s' % (option_spec.name, preset.settings[option_spec.name]))
						conf.set(option_spec.name, preset.settings[option_spec.name], force=True)

		conf.write(open(self._config + '_test', 'w'))

	_apache_version = None

	def _get_apache_version(self):
		self._logger.debug('Getting nginx version')
		if not self._apache_version:
			info = software.software_info('apache')
			self._apache_version = info.version
		return self._apache_version		
		
		
def get_handlers ():
	return [ApacheHandler()]

def reload_apache_conf(f):
	def g(*args):
		inst = f.__self__
		inst._config = Configuration('apache')
		try:
			inst._config.read(inst._httpd_conf_path)
		except (OSError, MetaconfError, ParseError), e:
			raise HandlerError('Cannot read Apache config %s : %s' % (inst._httpd_conf_path, str(e)))
		f(*args)
	return g

class ApacheHandler(Handler):
	
	_config = None
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
		
		ini = self._cnf.rawini
		self._httpd_conf_path = ini.get(CNF_SECTION, APP_CONF_PATH)		
		self._initd = initdv2.lookup('apache')
		bus.define_events('apache_reload')
		ServiceCtlHanler.__init__(self, BEHAVIOUR, self._initd, ApacheCnfController())
		bus.on("init", self.on_init)
		

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and \
			(message.name == Messages.HOST_UP or \
			message.name == Messages.HOST_DOWN or \
			message.name == Messages.VHOST_RECONFIGURE or \
			message.name == Messages.UPDATE_SERVICE_CONFIGURATION)

	@reload_apache_conf
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
				self._initd.start()
			except initdv2.InitdError, e:
				self._logger.error(e)

	on_before_host_up = on_start
		
	def on_before_host_down(self, *args):
		try:
			self._logger.info("Stopping Apache")
			self._initd.stop()
		except initdv2.InitdError, e:
			self._logger.error("Cannot stop apache")
			if self._initd.running:
				raise

	def _update_vhosts(self):
		
		config = bus.config
		vhosts_path = os.path.join(bus.etc_path,config.get(CNF_SECTION,'vhosts_path'))
		cert_path = bus.etc_path + '/private.d/keys'	
		
		self.server_root = self._get_server_root()
			
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
							self._logger.error("Scalr returned empty SSL cert")
						elif not https_certificate[1]:
							self._logger.error("Scalr returned empty SSL key")
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
					self._check_mod_ssl()
					
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
				self._patch_default_conf_deb()
			
			self._logger.debug("Checking that vhosts directory included in main apache config")
			
			includes = self._config.get_list('Include')
			if not vhosts_path + '/*' in includes:
				self._config.add('Include', vhosts_path + '/*')
				self._config.write(open(self._httpd_conf_path, 'w'))			

	def _patch_ssl_conf(self, cert_path):
		key_path = cert_path + '/https.key'
		crt_path = cert_path + '/https.crt'

		ssl_conf_path = self.server_root + ('/conf.d/ssl.conf' if disttool.is_redhat_based() else '/sites-available/default-ssl')
		if os.path.exists(ssl_conf_path):			
			ssl_conf = Configuration('apache')
			ssl_conf.read(ssl_conf_path)
			ssl_conf.set(".//SSLCertificateFile", crt_path)
			ssl_conf.set(".//SSLCertificateKeyFile", key_path)
			ssl_conf.write(open(ssl_conf_path, 'w'))
		else:
			raise HandlerError("Apache's ssl configuration file %s doesn't exist" % ssl_conf_path)

	
	def _check_mod_ssl(self):
		if disttool.is_debian_based():
			self._check_mod_ssl_deb()
		elif disttool.is_redhat_based():
			self._check_mod_ssl_redhat()


	def _check_mod_ssl_deb(self):
		mods_available = os.path.dirname(self._httpd_conf_path) + '/mods-available'
		mods_enabled = os.path.dirname(self._httpd_conf_path) + '/mods-enabled'
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
			
				
	def _check_mod_ssl_redhat(self):
		include_mod_ssl = 'LoadModule ssl_module modules/mod_ssl.so'
		mod_ssl_file = self.server_root + '/modules/mod_ssl.so'
		
		if not os.path.isfile(mod_ssl_file) and not os.path.islink(mod_ssl_file):
			self._logger.error('%s does not exist. Try "sudo yum install mod_ssl" ',
						mod_ssl_file)
		else:			
			#ssl.conf part
			ssl_conf_path = self.server_root + '/conf.d/ssl.conf'
			
			if not os.path.exists(ssl_conf_path):
				self._logger.error("SSL config %s doesn`t exist", ssl_conf_path)
			else:
				ssl_conf = Configuration('apache')
				ssl_conf.read(ssl_conf_path)
							
				"""
				error_message = 'Cannot read SSL config file %s' % ssl_conf_path
				ssl_conf_str = read_file(ssl_conf_path, error_msg=error_message, logger=self._logger)
				
				if not ssl_conf_str:
					self._logger.error("SSL config file %s is empty. Filling in with minimal configuration.", ssl_conf_path)
					ssl_conf_str_updated = ssl_conf_minimal
				"""
				
				if ssl_conf.empty:
					self._logger.error("SSL config file %s is empty. Filling in with minimal configuration.", ssl_conf_path)
					ssl_conf.add('Listen', '443')
					ssl_conf.add('NameVirtualHost', '*:443')
				else:
					if not ssl_conf.get_list('NameVirtualHost'):
						self._logger.debug("NameVirtualHost directive not found in %s", ssl_conf_path)
						if not ssl_conf.get_list('Listen'):
							self._logger.debug("Listen directive not found in %s. ", ssl_conf_path)
							self._logger.debug("Patching %s with Listen & NameVirtualHost directives.",	ssl_conf_path)
							ssl_conf.add('Listen', '443')
							ssl_conf.add('NameVirtualHost', '*:443')
						else:
							self._logger.debug("NameVirtualHost directive inserted after Listen directive.")
							ssl_conf.add('NameVirtualHost', '*:443', 'Listen')
				ssl_conf.write(open(ssl_conf_path, 'w'))
			
			loaded_in_main = [module for module in self._config.get_list('LoadModule') if 'mod_ssl.so' in module]
			
			if not loaded_in_main:
				if os.path.exists(ssl_conf_path):
					loaded_in_ssl = [module for module in ssl_conf.get_list('LoadModule') if 'mod_ssl.so' in module]
					if not loaded_in_ssl:
						self._config.add('LoadModule', 'ssl_module modules/mod_ssl.so')
						self._config.write(open(self._httpd_conf_path, 'w'))
						
				
				"""
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
				"""
	
	def _get_server_root(self):
		if disttool.is_debian_based():
			server_root = '/etc/apache2'
		
		elif disttool.is_redhat_based():
			self._logger.debug("Searching in apache config file %s to find server root", self._httpd_conf_path)
			
			try:
				server_root = self._config.get('ServerRoot')
			except PathNotExistsError:
				self._logger.warning("ServerRoot not found in apache config file %s", self._httpd_conf_path)
				server_root = os.path.dirname(self._httpd_conf_path)
				self._logger.debug("Use %s as ServerRoot", server_root)
		return server_root
	
	
	def _reload_apache(self):
		self._initd.reload()
	
	
	def _patch_default_conf_deb(self):
		self._logger.debug("Replacing NameVirtualhost and Virtualhost ports especially for debian-based linux")
		default_vhost_path = os.path.dirname(self._httpd_conf_path) + '/sites-enabled' + '/' + '000-default'
		if os.path.exists(default_vhost_path):
			default_vhost = Configuration('apache')
			default_vhost.read(default_vhost_path)
			default_vhost.set('NameVirtualHost', '*:80', force=True)
			default_vhost.set('VirtualHost', '*:80')
			default_vhost.write(open(default_vhost_path, 'w'))
		else:
			self._logger.error('Cannot read default vhost config file %s' % default_vhost_path)

	def _create_vhost_paths(self, vhost_path):
		if os.path.exists(vhost_path):
			vhost = Configuration('apache')
			vhost.read(vhost_path)
			list_logs = vhost.get_list('.//ErrorLog') + vhost.get_list('.//CustomLog')

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
		