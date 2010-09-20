'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import Handler, HandlerError, ServiceCtlHanler
from scalarizr.messaging import Messages
from scalarizr.util import system, cached, firstmatched,\
	validators, software
from scalarizr.util.filetool import read_file, write_file
import os
import re
import shutil
import logging
from datetime import datetime
from scalarizr.util import initdv2
from scalarizr.libs.metaconf import Configuration, PathNotExistsError
from telnetlib import Telnet
from scalarizr.service import CnfPresetStore, CnfPreset, CnfController, Options


BEHAVIOUR = BuiltinBehaviours.WWW
CNF_NAME = BEHAVIOUR
CNF_SECTION = BEHAVIOUR

BIN_PATH = 'binary_path'
APP_PORT = 'app_port'
HTTPS_INC_PATH = 'https_include_path'
APP_INC_PATH = 'app_include_path'

class NginxInitScript(initdv2.ParametrizedInitScript):
	def __init__(self):
		initd_script = "/etc/init.d/nginx"
		
		if not os.path.exists(initd_script):
			raise HandlerError("Cannot find Nginx init script at %s. Make sure that nginx is installed" % initd_script)

		pid_file = None
		try:
			out = system("nginx -V")[1]
			m = re.search("--pid-path=(.*?)\s", out)
			if m:
					pid_file = m.group(1)
		except:
			pass
		
		initdv2.ParametrizedInitScript.__init__(self, 'nginx', 
				initd_script, pid_file, socks=[initdv2.SockParam(80)])

	def status(self):
		status = initdv2.ParametrizedInitScript.status(self)
		if not status and self.socks:
			ip, port = self.socks[0].conn_address
			telnet = Telnet(ip, port)
			telnet.write('hello\n')
			if 'nginx' in telnet.read_all().lower():
				return initdv2.Status.RUNNING
			return initdv2.Status.UNKNOWN
		return status

	def configtest(self):
		out = system('nginx -t')[1]
		if 'failed' in out.lower():
			raise initdv2.InitdError("Configuration isn't valid: %s" % out)
		
initdv2.explore('nginx', NginxInitScript)

# Nginx behaviours configuration options
class NginxOptions(Configurator.Container):
	'''
	www behaviour
	'''
	cnf_name = CNF_NAME
	
	class binary_path(Configurator.Option):
		'''
		Path to nginx binary
		'''
		name = CNF_SECTION + '/binary_path'
		required = True
		
		@property
		@cached
		def default(self):
			return firstmatched(lambda p: os.access(p, os.F_OK | os.X_OK), 
					('/usr/sbin/nginx',	'/usr/local/nginx/sbin/nginx'), '')

		@validators.validate(validators.executable)
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
			
		value = property(Configurator.Option._get_value, _set_value)


	class app_port(Configurator.Option):
		'''
		App role port
		'''
		name = CNF_SECTION + '/app_port'
		default = '80'
		required = True
		
		@validators.validate(validators.portnumber())
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
		
		value = property(Configurator.Option._get_value, _set_value)
		

	class app_include_path(Configurator.Option):
		'''
		App upstreams configuration file path.
		'''
		name = CNF_SECTION + '/app_include_path'
		default = '/etc/nginx/app-servers.include'
		required = True
		
	class https_include_path(Configurator.Option):
		'''
		HTTPS configuration file path.
		'''
		name = CNF_SECTION + '/https_include_path'
		default = '/etc/nginx/https.include'
		required = True


def get_handlers():
	return [NginxHandler()]


class NginxCnfController(CnfController):
	
	class OptionSpec:
		context = None
		name = None
		supported_from = None
		default_value = None
		
		def __init__(self, name, context=None, default_value = None, supported_from = None):
			self.name = name			
			self.context = context
			self.supported_from = supported_from
			self.default_value = default_value
					
			
	options = Options(
		# http://wiki.nginx.org/NginxDirectiveIndex
		OptionSpec(name = 'daemon', default_value = 'on'),
		OptionSpec('env', default_value = 'TZ'), #multiple
		OptionSpec('debug_points', default_value = 'none'),
		OptionSpec('error_log', default_value = '${prefix}/logs/error.log'), #multiple
		#OptionSpec('log_not_found'), location only
		OptionSpec('include', default_value = 'none'),
		OptionSpec('lock_file'), #default_value = #compile-time option 
		OptionSpec('master_process', default_value = 'on'), 
		OptionSpec('pid'), #default_value = #compile-time option 
		OptionSpec('ssl_engine'), #default_value = system dependent
		OptionSpec('timer_resolution', default_value = 'none'),
		OptionSpec('user', default_value = 'nobody nobody'),
		OptionSpec('worker_cpu_affinity', default_value = 'none'),
		OptionSpec('worker_priority', default_value = 'on'),
		OptionSpec('worker_processes', default_value = '1'),
		OptionSpec('worker_rlimit_core'), #no default value in documentation
		OptionSpec('worker_rlimit_nofile'),  #no default value in documentation
		OptionSpec('worker_rlimit_sigpending'),  #no default value in documentation
		OptionSpec('working_directory', default_value = '--prefix'),

		OptionSpec('accept_mutex', 'events', 'on'),
		OptionSpec('accept_mutex_delay', 'events', '500ms'),
		OptionSpec('debug_connection', 'events', 'none'),
		OptionSpec('devpoll_changes', 'events', '32'), 
		OptionSpec('devpoll_events', 'events', '32'),  
		OptionSpec('kqueue_changes', 'events', '512'), 
		OptionSpec('kqueue_events', 'events', '512'), 
		OptionSpec('epoll_events', 'events', '512'), 
		OptionSpec('multi_accept', 'events', 'off'),
		OptionSpec('rtsig_signo', 'events', '40'),
		OptionSpec('rtsig_overflow_events', 'events', '16'),
		OptionSpec('rtsig_overflow_test', 'events', '32'),
		OptionSpec('rtsig_overflow_threshold', 'events'), #no default value in documentation
		OptionSpec('use', 'events'),  #no default value in documentation
		OptionSpec('worker_connections', 'events'), #no default value in documentation
		
		OptionSpec('keepalive_timeout', 'http', '75'),
		OptionSpec('keepalive_requests', 'http', '100', supported_from = (0,8,0)),
		OptionSpec('tcp_nodelay', 'http','on'),
		OptionSpec('tcp_nopush', 'http', 'off'),
		OptionSpec('directio', 'http', 'off'),
		OptionSpec('sendfile', 'http', 'off'),
		OptionSpec('large_client_header_buffers', 'http', '4 4k/8k'),
		OptionSpec('limit_rate', 'http', 'no'),
		OptionSpec('limit_rate_after', 'http', '1m'),
		OptionSpec('log_not_found', 'http', 'on'),
		OptionSpec('msie_padding', 'http', 'on'),
		OptionSpec('msie_refresh', 'http', 'off'),
		OptionSpec('open_file_cache', 'http', 'off'), #multiple
		OptionSpec('open_file_cache_errors', 'http', 'off'),
		OptionSpec('open_file_cache_min_uses', 'http', '1'),
		OptionSpec('open_file_cache_valid', 'http', '60'),
		OptionSpec('optimize_server_names', 'http', 'on'),
		OptionSpec('port_in_redirect', 'http', 'on'),
		OptionSpec('error_page', 'http', 'no'), #multiple
		OptionSpec('resolver', 'http', 'no'),
		OptionSpec('resolver_timeout', 'http', '30s'),
		OptionSpec('root', 'http', 'html'),
		OptionSpec('send_timeout', 'http', '60'),
		#server_name - exists only in server context
		OptionSpec('server_name_in_redirect', 'http', 'on'),
		OptionSpec('server_names_hash_max_size', 'http', '512'),
		OptionSpec('server_names_hash_bucket_size', 'http', '32/64/128'),
		OptionSpec('server_tokens', 'http', 'on'),
		OptionSpec('listen', 'http', '80'),
		#0.7.x syntax: listen address:port [ default [ backlog=num | rcvbuf=size | sndbuf=size | accept_filter=filter | deferred | bind | ssl ] ]
		#0.8.x syntax: listen address:port [ default_server [ backlog=num | rcvbuf=size | sndbuf=size | accept_filter=filter | deferred | bind | ssl ] ]
		
		OptionSpec('client_body_in_file_only', 'http', 'off'),
		OptionSpec('client_body_in_single_buffer', 'http', 'off'),
		OptionSpec('client_body_buffer_size', 'http', '8k/16k'),
		OptionSpec('client_body_temp_path', 'http', 'client_body_temp'),
		OptionSpec('client_body_timeout', 'http', '60'),
		OptionSpec('client_header_buffer_size', 'http', '1k'),
		OptionSpec('client_header_timeout', 'http', '60'),
		OptionSpec('client_max_body_size', 'http', '1m'),
		
		OptionSpec('proxy_buffer_size', 'http', '4k/8k'),
		OptionSpec('proxy_buffering', 'http', 'on'),
		OptionSpec('proxy_buffers', 'http', '8 4k/8k'),
		OptionSpec('proxy_busy_buffers_size', 'http', '["#proxy buffer size"] * 2'),
		OptionSpec('proxy_cache', 'http', 'None'),
		OptionSpec('proxy_cache_key', 'http', '$scheme$proxy_host$request_uri'),
		OptionSpec('proxy_cache_path', 'http', 'None'),
		OptionSpec('proxy_cache_methods', 'http', 'GET HEAD'),
		OptionSpec('proxy_cache_min_uses', 'http', '1'),
		OptionSpec('proxy_cache_valid', 'http', 'None'),
		OptionSpec('proxy_cache_use_stale', 'http', 'off'),
		OptionSpec('proxy_connect_timeout', 'http', '60'),
		OptionSpec('proxy_headers_hash_bucket_size', 'http', '64'),
		OptionSpec('proxy_headers_hash_max_size', 'http', '512'),
		OptionSpec('proxy_hide_header', 'http'), #no default value in docs
		OptionSpec('proxy_ignore_client_abort', 'http', 'off'),
		OptionSpec('proxy_ignore_headers', 'http', 'none'),
		OptionSpec('proxy_intercept_errors', 'http', 'off'),
		OptionSpec('proxy_max_temp_file_size', 'http', '1G'),
		OptionSpec('proxy_method', 'http', 'None'),
		OptionSpec('proxy_next_upstream', 'http', 'error timeout'),
		OptionSpec('proxy_no_cache', 'http', 'None'), #multiple
		OptionSpec('proxy_pass_header', 'http'), #no default value in docs
		OptionSpec('proxy_pass_request_body', 'http', 'on'),
		OptionSpec('proxy_pass_request_headers', 'http', 'on'),
		OptionSpec('proxy_redirect', 'http', 'default'),
		OptionSpec('proxy_read_timeout', 'http', '60'),
		#OptionSpec('proxy_redirect_errors', 'http'), #deprecated
		OptionSpec('proxy_send_lowat', 'http', 'off'),
		OptionSpec('proxy_send_timeout', 'http', '60'),
		OptionSpec('proxy_set_body', 'http', 'off'),
		OptionSpec('proxy_set_header', 'http', 'Host and Connection'),
		OptionSpec('proxy_store', 'http', 'off'),
		OptionSpec('proxy_store_access', 'http', 'user:rw'), #multiple
		OptionSpec('proxy_temp_file_write_size', 'http', '["#proxy buffer size"] * 2'),
		OptionSpec('proxy_temp_path', 'http', '$NGX_PREFIX/proxy_temp'),
		#OptionSpec('proxy_upstream_max_fails', 'http'), #deprecated since 0.5.0
		#OptionSpec('proxy_upstream_fail_timeout', 'http'), #deprecated since 0.5.0
		OptionSpec('access_log', 'http', 'log/access.log combined'), #multiple
		OptionSpec('log_format', 'http', 'combined "..."'),
		OptionSpec('open_log_file_cache', 'http', 'off'), #multiple
	)
	
	_nginx_version = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._cnf = bus.cnf
		ini = self._cnf.rawini
		self._include = ini.get(CNF_SECTION, APP_INC_PATH)
		config_dir = os.path.dirname(self._include)
		self.nginx_conf_path = config_dir + '/nginx.conf'
		
	def current_preset(self):
		self._logger.debug('Getting current Nginx preset')
		preset = CnfPreset(name='current')
		
		conf = Configuration('nginx')
		conf.read(self.nginx_conf_path)
		
		vars = {}
		
		for option_spec in self.options:
			try:
				if option_spec.context:
					vars[option_spec.name] = conf.get('%s/%s'%(option_spec.context, option_spec.name))
				else:
					vars[option_spec.name] = conf.get(option_spec.name)
			except PathNotExistsError:
				self._logger.debug('%s does not exist in %s. Using default value' 
						%(option_spec.name, self.nginx_conf_path))

				if option_spec.default_value:
					vars[option_spec.name] = option_spec.default_value
				else:
					self._logger.error('default value for %s not found'%(option_spec.name))
				
		preset.settings = vars
		return preset
	
	
	def apply_preset(self, preset):
		self._logger.debug('Applying %s preset' % (preset.name if preset.name else 'undefined'))
		
		conf = Configuration('nginx')
		conf.read(self.nginx_conf_path)
		
		for option_spec in self.options:
			if preset.settings.has_key(option_spec.name):
				
				var = option_spec.name if not option_spec.context else '%s/%s'%(option_spec.context, option_spec.name)
	
				# Skip unsupported
				if option_spec.supported_from and option_spec.supported_from > self._get_nginx_version():
					self._logger.debug('%s supported from %s. Cannot apply.' 
							% (option_spec.name, option_spec.supported_from))
					continue
								
				if not option_spec.default_value:
					self._logger.debug('No default value for %s' % option_spec.name)
					
				elif preset.settings[option_spec.name] == option_spec.default_value:
					try:
						conf.remove(var)
						self._logger.debug('%s value is equal to default. Removed from config.' % option_spec.name)
					except PathNotExistsError:
						pass
					continue	

				if conf.get(var) == preset.settings[option_spec.name]:
					self._logger.debug('Variable %s wasn`t changed. Skipping.' % option_spec.name)
				else:
					self._logger.debug('Setting variable %s to %s' % (option_spec.name, preset.settings[option_spec.name]))
					conf.set(var, preset.settings[option_spec.name], force=True)

		conf.write(open(self.nginx_conf_path + '_test', 'w'))
				
	def _get_nginx_version(self):
		self._logger.debug('Getting nginx version')
		if not self._nginx_version:
			info = software.software_info('nginx')
			self._nginx_version = info.version
		return self._nginx_version

		
class NginxHandler(Handler):
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service	
		self._cnf = bus.cnf
		self._initd = initdv2.lookup('nginx')
		
		ini = self._cnf.rawini
		self._https_conf_path = ini.get(CNF_SECTION, HTTPS_INC_PATH)
		self._nginx_binary = ini.get(CNF_SECTION, BIN_PATH)
		self._app_port = ini.get(CNF_SECTION, APP_PORT)
		self._include = ini.get(CNF_SECTION, APP_INC_PATH)
		bus.define_events("nginx_upstream_reload")
		ServiceCtlHanler.__init__(self, BEHAVIOUR, self._initd, NginxCnfController())
		bus.on("init", self.on_init)

		
	def on_init(self):
		bus.on("start", self.on_start)
		bus.on('before_host_up', self.on_before_host_up)
		bus.on("before_host_down", self.on_before_host_down)
		
	def on_start(self, *args):
		if self._cnf.state == ScalarizrState.RUNNING:
			try:
				self._logger.info("Starting Nginx")
				self._initd.start()
			except initdv2.InitdError, e:
				self._logger.error(e)	
				
	on_before_host_up = on_start
	
	def on_HostUp(self, message):
		self.nginx_upstream_reload()
	
	def on_HostDown(self, message):
		self.nginx_upstream_reload()
	
	def nginx_upstream_reload(self, force_reload=False):

		config_dir = os.path.dirname(self._include)
		nginx_conf_path = config_dir + '/nginx.conf'
		
		if not hasattr(self, '_config'):
			try:
				self._config = Configuration('nginx')
				self._config.read(nginx_conf_path)
			except (Exception, BaseException), e:
				raise HandlerError('Cannot read/parse nginx main configuration file: %s' % str(e))

		template_path = os.path.join(self._cnf.public_path(), "nginx/app-servers.tpl")
		
		backend_include = Configuration('nginx')
		if not os.path.exists(template_path):
			'''
			template_content = """\nupstream backend {\n\tip_hash;\n\n\t${upstream_hosts}\n}\n"""
			log_message = "nginx template '%s' doesn't exists. Creating default template" % (template_path,)
			write_file(template_path, template_content, msg = log_message, logger = self._logger)
			'''
			backend_include.add('upstream', 'backend')
			backend_include.add('upstream/ip_hash')
			backend_include.write(open(template_path, 'w'))
		else:
			backend_include.read(template_path)

		# Create upstream hosts configuration
		for app_serv in self._queryenv.list_roles(behaviour = BuiltinBehaviours.APP):
			for app_host in app_serv.hosts :
				server_str = '%s:%s' % (app_host.internal_ip, self._app_port)
				backend_include.add('upstream/server', server_str)
		if not backend_include.get_list('upstream/server'):
			self._logger.debug("Scalr returned empty app hosts list. Adding localhost only.")
			backend_include.add('upstream/server', '127.0.0.1:80')
		
		#HTTPS Configuration
		# openssl req -new -x509 -days 9999 -nodes -out cert.pem -keyout cert.key
		cert_path = self._cnf.key_path("https.crt")
		pk_path = self._cnf.key_path("https.key") 
		if os.path.isfile(bus.etc_path+"/nginx/https.include") and 	os.path.isfile(cert_path) \
															   and  os.path.isfile(pk_path):
			https_include = bus.etc_path + "/nginx/https.include;"
			self._logger.debug("Adding %s to template", https_include)
			backend_include.add('include', https_include)
			#Determine, whether configuration was changed or not
		
		old_include = None
		if os.path.isfile(self._include):
			self._logger.debug("Reading old configuration from %s" % self._include)
			old_include = Configuration('nginx')
			old_include.read(self._include)
			
		if old_include and not force_reload and \
							backend_include.get_list('upstream/server') == old_include.get_list('upstream/server') :
			self._logger.debug("nginx upstream configuration wasn`t changed.")
		else:
			self._logger.debug("nginx upstream configuration was changed.")
			self._logger.debug("Creating backup config files.")
			if os.path.isfile(self._include):
				shutil.move(self._include, self._include+".save")
			else:
				self._logger.debug('%s does not exist. Nothing to backup.' % self._include)
				
			self._logger.debug("Writing template to %s" % self._include)
			backend_include.write(open(self._include, 'w'))
			#Patching main config file
			if 'http://backend' in self._config.get_list('http/server/location/proxy_pass') and \
						self._include in self._config.get_list('http/include'):
				
				self._logger.debug("File %s already included into nginx main config %s", 
								self._include, nginx_conf_path)
			else:
				self._config.comment('http/server')
				self._config.read(os.path.join(self._cnf.private_path(), "nginx/server.tpl"))
				self._config.add('http/include', self._include)
				self._config.write(open(nginx_conf_path, 'w'))
			
				self._logger.info("Testing new configuration.")
			
				try:
					self._initd.configtest()
				except initdv2.InitdError, e:
					self._logger.error("Configuration error detected:" +  str(e) + " Reverting configuration.")
					if os.path.isfile(self._include):
						shutil.move(self._include, self._include+".junk")
					else:
						self._logger.debug('%s does not exist', self._include)
					if os.path.isfile(self._include+".save"):
						shutil.move(self._include+".save", self._include)
					else:
						self._logger.debug('%s does not exist', self._include+".save")
				else:
					# Reload nginx
					self._initd.reload()

		bus.fire("nginx_upstream_reload")	
	
	def on_before_host_down(self, *args):
		try:
			self._logger.info("Stopping Nginx")
			self._initd.stop()
		except initdv2.InitdError, e:
			self._logger.error("Cannot stop nginx: %s" % str(e))
			if self._initd.running:
				raise

		
	def on_BeforeHostTerminate(self, message):
		config = bus.config
		include_path = config.get(CNF_SECTION, "app_include_path")
		if not os.path.exists(include_path):
			return

		include = Configuration('nginx')	
		include.read(include_path)
		server_ip = '%s:80' % message.local_ip or message.remote_ip
		if server_ip in include.get_list('upstream/server'):
			include.remove('upstream/server', server_ip)
		include.write(open(include_path, 'w'))
		self._initd.restart()

	def _update_vhosts(self):
		self._logger.debug("Requesting virtual hosts list")
		received_vhosts = self._queryenv.list_virtual_hosts()
		self._logger.debug("Virtual hosts list obtained (num: %d)", len(received_vhosts))
		
		https_config = ''
		
		if [] != received_vhosts:
			
			https_certificate = self._queryenv.get_https_certificate()
			
			cert_path = self._cnf("https.crt")
			pk_path = self._cnf("https.key")
			
			if https_certificate[0]:
				msg = 'Writing ssl cert' 
				cert = https_certificate[0]
				write_file(cert_path, cert, msg=msg, logger=self._logger)
			else:
				self._logger.error('Scalr returned empty SSL Cert')
				return
				
			if len(https_certificate)>1 and https_certificate[1]:
				msg = 'Writing ssl key'
				pk = https_certificate[1]
				write_file(pk_path, pk, msg=msg, logger=self._logger)
			else:
				self._logger.error('Scalr returned empty SSL Cert')
				return

			for vhost in received_vhosts:
				if vhost.hostname and vhost.type == 'nginx': #and vhost.https
					raw = vhost.raw.replace('/etc/aws/keys/ssl/https.crt',cert_path)
					raw = raw.replace('/etc/aws/keys/ssl/https.key',pk_path)
					https_config += raw + '\n'

		else:
			self._logger.debug('Scalr returned empty virtualhost list')

		if https_config:

			if os.path.exists(self._https_conf_path) and read_file(self._https_conf_path, logger=self._logger):
				time_suffix = str(datetime.now()).replace(' ','.')
				shutil.move(self._https_conf_path, self._https_conf_path + time_suffix)

			msg = 'Writing virtualhosts to https.include'
			write_file(self._https_conf_path, https_config, msg=msg, logger=self._logger)
		

	def on_VhostReconfigure(self, message):
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self.nginx_upstream_reload(True)
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and \
			(message.name == Messages.HOST_UP or \
			message.name == Messages.HOST_DOWN or \
			message.name == Messages.BEFORE_HOST_TERMINATE or \
			message.name == Messages.VHOST_RECONFIGURE or \
			message.name == Messages.UPDATE_SERVICE_CONFIGURATION)	