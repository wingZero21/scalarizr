'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours, ScalarizrState
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Messages
from scalarizr.util import configtool, disttool, system, initd, cached, firstmatched,\
	validators
from scalarizr.util.filetool import read_file, write_file
import os
import re
import shutil
import subprocess
import logging
from datetime import datetime


BEHAVIOUR = BuiltinBehaviours.WWW
CNF_NAME = BEHAVIOUR
CNF_SECTION = BEHAVIOUR

# Explore service to initd utility
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

logger = logging.getLogger(__name__)
logger.debug("Explore Nginx service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("nginx", initd_script, pid_file, tcp_port=80)


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

class NginxHandler(Handler):
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service	
		self._cnf = bus.cnf
		bus.define_events("nginx_upstream_reload")
		bus.on("init", self.on_init)
		
	def on_init(self):
		bus.on("start", self.on_start)
		bus.on('before_host_up', self.on_before_host_up)
		bus.on("before_host_down", self.on_before_host_down)
		
	def on_start(self, *args):
		if self._cnf.state == ScalarizrState.RUNNING:
			try:
				self._logger.info("Starting Nginx")
				initd.start("nginx")
			except initd.InitdError, e:
				self._logger.error(e)	
				
	on_before_host_up = on_start
	
	def on_HostUp(self, message):
		self.nginx_upstream_reload()
	
	def on_HostDown(self, message):
		self.nginx_upstream_reload()
	
	def nginx_upstream_reload(self, force_reload=False):
		config = bus.config
		nginx_binary = config.get(CNF_SECTION, "binary_path")
		app_port = config.get(CNF_SECTION, "app_port") or "80"
		include = config.get(CNF_SECTION, "app_include_path")
		config_dir = os.path.dirname(include)
		nginx_conf_path = config_dir + '/nginx.conf'
		default_conf_path = config_dir + '/sites-enabled/' + 'default'
		initd_script = "/etc/init.d/nginx"
		
		template_path = os.path.join(bus.etc_path, "public.d/nginx/app-servers.tpl")
		
		if not os.path.exists(template_path):
			template_content = """\nupstream backend {\n\tip_hash;\n\n\t${upstream_hosts}\n}\n"""
			log_message = "nginx template '%s' doesn't exists. Creating default template" % (template_path,)
			write_file(template_path, template_content, msg = log_message, logger = self._logger)
		template = read_file(template_path, logger = self._logger)			

		# Create upstream hosts configuration
		upstream_hosts = ""
		for app_serv in self._queryenv.list_roles(behaviour = BuiltinBehaviours.APP):
			for app_host in app_serv.hosts :
				upstream_hosts += "\tserver %s:%s;\n" % (app_host.internal_ip, app_port)
		if not upstream_hosts:
			self._logger.debug("Scalr returned empty app hosts list. Adding localhost only.")
			upstream_hosts = "\tserver 127.0.0.1:80;\n"
		
		if template:
			self._logger.debug("Replacing data in template")
			template = template.replace("${upstream_hosts}", upstream_hosts)
		else:
			self.logger.error("Template is empty. Using internal data instead.")
			template = template_content.replace("${upstream_hosts}", upstream_hosts)

		#HTTPS Configuration		
		# openssl req -new -x509 -days 9999 -nodes -out cert.pem -keyout cert.key
		cert_path = configtool.get_key_filename("https.crt", private=True)
		pk_path = configtool.get_key_filename("https.key", private=True) 
		if os.path.isfile(bus.etc_path+"/nginx/https.include") and \
				os.path.isfile(cert_path) and os.path.isfile(pk_path):
			https_include = bus.etc_path + "/nginx/https.include;"
			self._logger.debug("Adding %s to template", https_include)
			template += "include "  + https_include
			
		#Determine, whether configuration was changed or not
		
		old_include = None
		if os.path.isfile(include):
			log_message = "Reading old configuration from %s" % include
			old_include = read_file(include, msg = log_message, logger = self._logger)

		if template == old_include and not force_reload:
			self._logger.debug("nginx upstream configuration wasn`t changed.")
		else:
			self._logger.debug("nginx upstream configuration was changed.")
			self._logger.debug("Creating backup config files.")
			if os.path.isfile(include):
				shutil.move(include, include+".save")
			else:
				self._logger.debug('%s does not exist. Nothing to backup.' % include)
				
			log_message = "Writing template to %s" % include			
			write_file(include, template, msg = log_message, logger = self._logger)
			
			
			#Patching main config file
			if not os.path.isfile(nginx_conf_path):
				self._logger.error("nginx main config file %s does not exist", nginx_conf_path)
			else:
				log_message = "Reading nginx config "
				nginx_conf = read_file(nginx_conf_path, msg=log_message, logger=self._logger)
							
				if nginx_conf:
					backend_re = re.compile('^[^#\n]*?proxy_pass\s*http://backend\s*;\s*$', re.MULTILINE)
					# If configuration hasn't been patched before
					if not re.search(backend_re, nginx_conf):
						new_nginx_conf = ''
						server_re = re.compile('^\s*server\s*\{\s*(#.*?)?$')
						fp = open(nginx_conf_path,'r')
						opened = 0
						open_close_re = re.compile('^[^#\n]*([\{\}]).*?$')
						include_re = re.compile('^[^#\n]*include\s*/etc/nginx/(conf\.d/\*|sites-enabled/\*).*?$')
						# Comment all server sections and includes of default configuration files
						while 1:
							line = fp.readline()
							if not line:
								break
							
							if re.match(include_re, line):
								new_nginx_conf += '###' + line
								continue

							if re.match(server_re, line):
								new_nginx_conf += '###' + line
								opened = 1
								while 1:
									new_line = fp.readline()
									new_nginx_conf += '###' + new_line
									res = re.match(open_close_re, new_line)
									if res:
										opened += 1 if res.group(1) == '{' else -1
									if not opened:
										break
							else:
								new_nginx_conf += line
								
						# Adding upstream include to main nginx config		
						http_sect_re = re.compile('(^\s*http\s*\{.*?$)(.*)', re.S | re.MULTILINE)
						# Find http section in main nginx config
						if re.search(http_sect_re, new_nginx_conf):
							self._logger.debug("Including generated config to main nginx config %s", nginx_conf_path)
							
							server_sect = read_file(os.path.join(bus.etc_path, "public.d/handler.nginx/server.tpl"),logger=self._logger)
							
							new_nginx_conf = re.sub(http_sect_re, '\\1\n' + '    include '+ include + ';\n'
												                          + server_sect + '\\2', new_nginx_conf)				
						
						write_file(nginx_conf_path, new_nginx_conf, logger = self._logger)
						
					else:
						self._logger.debug("File %s already included into nginx main config %s", 
									include, nginx_conf_path)

			
			
			self._logger.info("Testing new configuration.")
			
			if os.path.isfile(nginx_binary):
			
				nginx_test_command = [nginx_binary, "-t"]
			
				p = subprocess.Popen(nginx_test_command, 
						stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
				stdout, stderr = p.communicate()
				is_nginx_test_failed = p.poll()
				
				if is_nginx_test_failed:
					self._logger.error("Configuration error detected:" +  stderr + " Reverting configuration.")
					if os.path.isfile(include):
						shutil.move(include, include+".junk")
					else:
						self._logger.debug('%s does not exist', include)
					if os.path.isfile(include+".save"):
						shutil.move(include+".save", include)
					else:
						self._logger.debug('%s does not exist', include+".save")
				
				else:
					# Reload nginx
					self._reload_nginx()

		bus.fire("nginx_upstream_reload")
	
	
	def on_before_host_down(self, *args):
		try:
			self._logger.info("Stopping Nginx")
			initd.stop("nginx")
		except initd.InitdError, e:
			self._logger.error("Cannot stop nginx")
			if initd.is_running("nginx"):
				raise

		
	def on_BeforeHostTerminate(self, message):
		config = bus.config
		include_path = config.get(CNF_SECTION, "app_include_path")
		include = read_file(include_path, logger = self._logger)
		if include and message.local_ip:
			new_include = include.replace("\tserver %s:80;\n" % message.local_ip,"")
			write_file(include_path, new_include, logger=self._logger)
	

	def _update_vhosts(self):
		self._logger.debug("Requesting virtual hosts list")
		received_vhosts = self._queryenv.list_virtual_hosts()
		self._logger.debug("Virtual hosts list obtained (num: %d)", len(received_vhosts))
				
		if [] != received_vhosts:
			
			https_certificate = self._queryenv.get_https_certificate()
			
			cert_path = configtool.get_key_filename("https.crt", private=True)
			pk_path = configtool.get_key_filename("https.key", private=True)
			
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
			
			https_config = ''			
			for vhost in received_vhosts:
				if vhost.hostname and vhost.type == 'nginx': #and vhost.https
					raw = vhost.raw.replace('/etc/aws/keys/ssl/https.crt',cert_path)
					raw = raw.replace('/etc/aws/keys/ssl/https.key',pk_path)
					https_config += raw + '\n'
					
		else:
			self._logger.debug('Scalr returned empty virtualhost list')
		
		if https_config:
			https_conf_path = bus.etc_path + '/nginx/https.include'
			
			if os.path.exists(https_conf_path) and read_file(https_conf_path, logger=self._logger):
				time_suffix = str(datetime.now()).replace(' ','.')
				shutil.move(https_conf_path, https_conf_path + time_suffix)
				
			msg = 'Writing virtualhosts to https.include' 	
			write_file(https_conf_path, https_config, msg=msg, logger=self._logger)
		

	def on_VhostReconfigure(self, message):
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self.nginx_upstream_reload(True)
	
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and \
			(message.name == Messages.HOST_UP or \
			message.name == Messages.HOST_DOWN or \
			message.name == Messages.BEFORE_HOST_TERMINATE or \
			message.name == Messages.VHOST_RECONFIGURE)	
	
	def _reload_nginx(self):
		nginx_pid = read_file(pid_file, logger = self._logger)
		if nginx_pid and nginx_pid.strip():
			try:
				self._logger.info("Reloading Nginx")
				initd.reload("nginx")
				self._logger.debug("nginx reloaded")
			except:
				self._logger.error("Cannot reloaded nginx")
				raise

