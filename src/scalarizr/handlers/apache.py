'''
Created on Dec 25, 2009

@author: marat
@author: Dmytro Korsakov
'''

# Core
from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours, ScalarizrState
from scalarizr.service import CnfController
from scalarizr.handlers import HandlerError, ServiceCtlHanler
from scalarizr.messaging import Messages

# Libs
from scalarizr.libs.metaconf import Configuration, ParseError, MetaconfError,\
	NoPathError
from scalarizr.util import disttool, cached, firstmatched, validators, software,\
	wait_until
from scalarizr.util import initdv2, system
from scalarizr.util.filetool import read_file, write_file

# Stdlibs
import logging, os, re
from telnetlib import Telnet
from scalarizr.util.initdv2 import InitdError
import time

SERVICE_NAME = 'apache'
BEHAVIOUR = BuiltinBehaviours.APP
CNF_SECTION = BEHAVIOUR
CNF_NAME = BEHAVIOUR + '.ini'
APP_CONF_PATH = 'apache_conf_path'

class ApacheInitScript(initdv2.ParametrizedInitScript):
	_apachectl = None
	
	def __init__(self):
		if disttool.is_redhat_based():
			self._apachectl = '/usr/sbin/apachectl'
			initd_script 	= '/etc/init.d/httpd'			
			pid_file 		= '/var/run/httpd.pid'
		elif disttool.is_debian_based():
			self._apachectl = '/usr/sbin/apache2ctl'			
			initd_script 	= '/etc/init.d/apache2'

			env_vars = read_file("/etc/apache2/envvars")
			m = re.search("export\sAPACHE_PID_FILE=(.*)", env_vars)
			if m:
				pid_file 	= m.group(1)
			else:
				pid_file	= '/var/run/apache2.pid'
		else:
			self._apachectl = '/usr/sbin/apachectl'			
			initd_script 	= '/etc/init.d/apache2'
			pid_file 		= '/var/run/apache2.pid'
		
		initdv2.ParametrizedInitScript.__init__(
			self, 
			'apache', 
			initd_script,
			pid_file = pid_file,
			socks=[initdv2.SockParam(80)]
		)
		
	def reload(self):
		if self.pid_file and os.path.exists(self.pid_file):
			out, err, retcode = system(self._apachectl + ' graceful')
			if retcode > 0:
				raise initdv2.InitdError('Cannot reload apache: %s' % err)
		else:
			raise InitdError('Service "%s" is not running' % self.name, InitdError.NOT_RUNNING)
		
	def status(self):		
		status = initdv2.ParametrizedInitScript.status(self)
		# If 'running' and socks were passed
		if not status and self.socks:
			ip, port = self.socks[0].conn_address
			telnet = Telnet(ip, port)
			telnet.write('HEAD / HTTP/1.0\n\n')
			if 'server: apache' in telnet.read_all().lower():
				return initdv2.Status.RUNNING
			return initdv2.Status.NOT_RUNNING
		return status
	
	def configtest(self):
		out = system(self._apachectl +' configtest')[1]
		if 'error' in out.lower():
			raise initdv2.InitdError("Configuration isn't valid: %s" % out)
		
	def start(self):
		ret = initdv2.ParametrizedInitScript.start(self)
		#out = system(self._apachectl +' start')[1]
		#if 'error' in out.lower():
		#	raise initdv2.InitdError("Error while starting apache: %s" % out)

		#if disttool.is_redhat_based():
		#	time.sleep(1)
		if self.pid_file:
			try:
				wait_until(lambda: os.path.exists(self.pid_file), sleep=0.2, timeout=5)
			except:
				raise initdv2.InitdError('Cannot start Apache: pid file hasn\'t been created')
		time.sleep(0.5)
		return True
	
	def restart(self):
		ret = initdv2.ParametrizedInitScript.restart(self)
		#out = system(self._apachectl +' restart')[1]
		#if 'error' in out.lower():
		#	raise initdv2.InitdError("Error while restarting apache: %s" % out)
		#time.sleep(0.5)
		
		if self.pid_file:
			try:
				wait_until(lambda: os.path.exists(self.pid_file), sleep=0.2, timeout=5)
			except:
				raise initdv2.InitdError('Cannot restart Apache: pid file hasn\'t been created')
		time.sleep(0.5)
		return ret

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

	def __init__(self):
		cnf = bus.cnf; ini = cnf.rawini
		CnfController.__init__(self, BEHAVIOUR, ini.get(CNF_SECTION, APP_CONF_PATH), 'apache', {'1':'on','0':'off'})
		
	@property
	def _software_version(self):
		return software.software_info('apache').version
		
		
def get_handlers ():
	return [ApacheHandler()]

def reload_apache_conf(f):
	def g(self,*args):
		self._config = Configuration('apache')
		try:
			self._config.read(self._httpd_conf_path)
		except (OSError, MetaconfError, ParseError), e:
			raise HandlerError('Cannot read Apache config %s : %s' % (self._httpd_conf_path, str(e)))
		f(self,*args)
	return g


class ApacheHandler(ServiceCtlHanler):
	
	_config = None
	_logger = None
	_queryenv = None
	_cnf = None
	'''
	@type _cnf: scalarizr.config.ScalarizrCnf
	'''

	def __init__(self):
		ServiceCtlHanler.__init__(self, BEHAVIOUR, initdv2.lookup('apache'), ApacheCnfController())		
		
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._cnf = bus.cnf

		ini = self._cnf.rawini
		self._httpd_conf_path = ini.get(CNF_SECTION, APP_CONF_PATH)
		self._config = Configuration('apache')
		self._config.read(self._httpd_conf_path)
		
		bus.on("init", self.on_init)
		bus.define_events(
			'apache_rpaf_reload'
		)

	def on_init(self):
		bus.on(
			start = self.on_start, 
			before_host_up = self.on_before_host_up
		)		

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and \
			(message.name == Messages.VHOST_RECONFIGURE or \
			message.name == Messages.UPDATE_SERVICE_CONFIGURATION or \
			message.name == Messages.HOST_UP or \
			message.name == Messages.HOST_DOWN or \
			message.name == Messages.BEFORE_HOST_TERMINATE)

	def on_start(self):
		if self._cnf.state == ScalarizrState.RUNNING:
			self._update_vhosts()			
			self._rpaf_reload()

	def on_before_host_up(self, message):
		self._update_vhosts()
		self._rpaf_reload()
		bus.fire('service_configured', service_name=SERVICE_NAME)

	def on_HostUp(self, message):
		if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
			self._rpaf_modify_proxy_ips([message.local_ip], operation='add')
	
	def on_HostDown(self, message):
		if message.local_ip and message.behaviour and BuiltinBehaviours.WWW in message.behaviour:
			self._rpaf_modify_proxy_ips([message.local_ip], operation='remove')
	
	on_BeforeHostTerminate = on_HostDown

	@reload_apache_conf
	def on_VhostReconfigure(self, message):
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self._reload_service()
		
	def _rpaf_modify_proxy_ips(self, ips, operation=None):
		self._logger.debug('Modify RPAFproxy_ips (operation: %s, ips: %s)', operation, ','.join(ips))
		file = firstmatched(
			lambda x: os.access(x, os.F_OK), 
			('/etc/httpd/conf.d/mod_rpaf.conf', '/etc/apache2/mods-available/rpaf.conf')
		)
		if file:
			rpaf = Configuration('apache')
			rpaf.read(file)
			
			if operation == 'add' or operation == 'remove':
				proxy_ips = set(re.split(r'\s+', rpaf.get('//RPAFproxy_ips')))
				if operation == 'add':
					proxy_ips |= set(ips)
				else:
					proxy_ips -= set(ips)
			elif operation == 'update':
				proxy_ips = set(ips)
			if not proxy_ips:
				proxy_ips.add('127.0.0.1')
				
			self._logger.info('RPAFproxy_ips: %s', ' '.join(proxy_ips))
			rpaf.set('//RPAFproxy_ips', ' '.join(proxy_ips))
			rpaf.write(file)
			
			self._reload_service()
		else:
			self._logger.debug('Nothing to do with rpaf: mod_rpaf configuration file not found')

		
	def _rpaf_reload(self):
		lb_hosts = []
		for role in self._queryenv.list_roles(behaviour=BuiltinBehaviours.WWW):
			for host in role.hosts:
				lb_hosts.append(host.internal_ip)
		self._rpaf_modify_proxy_ips(lb_hosts, operation='update')
		bus.fire('apache_rpaf_reload')
		
		
	def _update_vhosts(self):
		
		config = bus.config
		vhosts_path = os.path.join(bus.etc_path,config.get(CNF_SECTION, 'vhosts_path'))
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
		
		cert_path = bus.etc_path + '/private.d/keys'	
		
		self.server_root = self._get_server_root()
			
		self._logger.debug("Requesting virtual hosts list")
		received_vhosts = self._queryenv.list_virtual_hosts()
		self._logger.debug("Virtual hosts list obtained (num: %d)", len(received_vhosts))
		
		if [] != received_vhosts:
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
							ca_cert_error_message = 'Cannot write CA certificate to %s.' % cert_path
							
							for key_file in ['https.key', vhost.hostname + '.key']:
								write_file(cert_path + '/' + key_file, https_certificate[1], error_msg=key_error_message, logger=self._logger)
								os.chmod(cert_path + '/' + key_file, 0644)
														
							for cert_file in ['https.crt', vhost.hostname + '.crt']:
								write_file(cert_path + '/' + cert_file, https_certificate[0], error_msg=cert_error_message, logger=self._logger)
								os.chmod(cert_path + '/' + cert_file, 0644)
								
							if https_certificate[2]:
								for filename in ('https-ca.crt', vhost.hostname + '-ca.crt'):
									write_file(os.path.join(cert_path, filename), https_certificate[2], error_msg=ca_cert_error_message, logger=self._logger)
									os.chmod(os.path.join(cert_path, filename), 0644)
					
					self._logger.debug('Enabling SSL virtual host %s', vhost.hostname)
					
					vhost_fullpath = vhosts_path + '/' + vhost.hostname + '-ssl.vhost.conf'
					vhost_error_message = 'Cannot write vhost file %s.' % vhost_fullpath
					write_file(vhost_fullpath, vhost.raw.replace('/etc/aws/keys/ssl', cert_path), error_msg=vhost_error_message, logger = self._logger)
					
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
				self._config.write(self._httpd_conf_path)			

	def _patch_ssl_conf(self, cert_path):
		key_path = cert_path + '/https.key'
		crt_path = cert_path + '/https.crt'
		ca_crt_path = cert_path + '/https-ca.crt'

		ssl_conf_path = self.server_root + ('/conf.d/ssl.conf' if disttool.is_redhat_based() else '/sites-available/default-ssl')
		if os.path.exists(ssl_conf_path):			
			ssl_conf = Configuration('apache')
			ssl_conf.read(ssl_conf_path)
			ssl_conf.set(".//SSLCertificateFile", crt_path)
			ssl_conf.set(".//SSLCertificateKeyFile", key_path)
			if os.path.exists(ca_crt_path):
				try:
					ssl_conf.set('.//SSLCACertificateFile', ca_crt_path)
				except NoPathError:
					# XXX: ugly hack
					parent = ssl_conf.etree.find('.//SSLCertificateFile/..')
					before_el = ssl_conf.etree.find('.//SSLCertificateFile')
					ch = ssl_conf._provider.create_element(ssl_conf.etree, './/SSLCACertificateFile', ca_crt_path)
					ch.text = ca_crt_path
					parent.insert(list(parent).index(before_el), ch)
					
			ssl_conf.write(ssl_conf_path)
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
		if not os.path.exists(mods_enabled + '/ssl.conf') and \
				not os.path.exists(mods_enabled + '/ssl.load'):
			if os.path.exists(mods_available) and \
				 os.path.exists(mods_available+'/ssl.conf') and \
				 os.path.exists(mods_available+'/ssl.load'):
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
		mod_ssl_file = self.server_root + '/modules/mod_ssl.so'
		
		if not os.path.exists(mod_ssl_file):
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
				ssl_conf.write(ssl_conf_path)
			
			loaded_in_main = [module for module in self._config.get_list('LoadModule') if 'mod_ssl.so' in module]
			
			if not loaded_in_main:
				if os.path.exists(ssl_conf_path):
					loaded_in_ssl = [module for module in ssl_conf.get_list('LoadModule') if 'mod_ssl.so' in module]
					if not loaded_in_ssl:
						self._config.add('LoadModule', 'ssl_module modules/mod_ssl.so')
						self._config.write(self._httpd_conf_path)
						
				
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
				server_root = re.sub(r'^["\'](.+)["\']$', r'\1', server_root)
			except NoPathError:
				self._logger.warning("ServerRoot not found in apache config file %s", self._httpd_conf_path)
				server_root = os.path.dirname(self._httpd_conf_path)
				self._logger.debug("Use %s as ServerRoot", server_root)
		return server_root
	
	def _patch_default_conf_deb(self):
		self._logger.debug("Replacing NameVirtualhost and Virtualhost ports especially for debian-based linux")
		default_vhost_path = os.path.dirname(self._httpd_conf_path) + '/sites-enabled' + '/' + '000-default'
		if os.path.exists(default_vhost_path):
			default_vhost = Configuration('apache')
			default_vhost.read(default_vhost_path)
			default_vhost.set('NameVirtualHost', '*:80', force=True)
			default_vhost.set('VirtualHost', '*:80')
			default_vhost.write(default_vhost_path)
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
		