'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.handlers import Handler, HandlerError, lifecircle
from scalarizr.behaviour import Behaviours
from scalarizr.messaging import Messages
from scalarizr.util import configtool, disttool, system, initd
from scalarizr.util.filetool import read_file, write_file
import os
import re
import shutil
import subprocess
import logging

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

def get_handlers():
	return [NginxHandler()]

class NginxHandler(Handler):
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service	
		bus.define_events("nginx_upstream_reload")
		bus.on("start", self.on_start)
		bus.on("before_host_down", self.on_before_host_down)
		
	def on_start(self):
		if lifecircle.get_state() == lifecircle.STATE_RUNNING:
			try:
				self._logger.info("Starting Nginx")
				initd.start("nginx")
			except initd.InitdError, e:
				self._logger.error(e)	
	
	def on_HostUp(self, message):
		self.nginx_upstream_reload()
	
	def on_HostDown(self, message):
		self.nginx_upstream_reload()
	
	def nginx_upstream_reload(self):
		config = bus.config
		section = configtool.get_behaviour_section_name(Behaviours.WWW)
		nginx_binary = config.get(section, "binary_path")
		app_port = config.get(section, "app_port") or "80"
		include = config.get(section, "app_include_path")
		config_dir = os.path.dirname(include)
		nginx_conf_path = config_dir + '/nginx.conf'
		default_conf_path = config_dir + '/sites-enabled/' + 'default'
		initd_script = "/etc/init.d/nginx"
		
		template_path = os.path.join(bus.etc_path, "public.d/handler.nginx/app-servers.tpl")
		
		if not os.path.exists(template_path):
			template_content = """\nupstream backend {\n\tip_hash;\n\n\t${upstream_hosts}\n}\n"""
			log_message = "nginx template '%s' doesn't exists. Creating default template" % (template_path,)
			write_file(template_path, template_content, msg = log_message, logger = self._logger)
		template = read_file(template_path, logger = self._logger)			

		# Create upstream hosts configuration
		upstream_hosts = ""
		for app_serv in self._queryenv.list_roles(behaviour = Behaviours.APP):
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

		if template == old_include:
			self._logger.info("nginx upstream configuration wasn`t changed.")
		else:
			self._logger.info("nginx upstream configuration was changed.")
			self._logger.debug("Creating backup config files.")
			if os.path.isfile(include):
				shutil.move(include, include+".save")
				
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
					if os.path.isfile(include+".save"):
						shutil.move(include+".save", include)
				
				else:
					# Reload nginx
					self._reload_nginx()

		bus.fire("nginx_upstream_reload")
		
	
	def on_before_host_down(self):
		try:
			self._logger.info("Stopping nginx")
			initd.stop("nginx")
		except initd.InitdError, e:
			self._logger.error("Cannot stop nginx")
			if initd.is_running("nginx"):
				raise

		
	def on_BeforeHostTerminate(self, message):
		config = bus.config
		section = configtool.get_behaviour_section_name(Behaviours.WWW)
		include_path = config.get(section, "app_include_path")
		include = read_file(include_path, logger = self._logger)
		if include and message.local_ip:
			new_include = include.replace("\tserver %s:80;\n" % message.local_ip,"")
			write_file(include_path, new_include, logger=self._logger)
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return Behaviours.WWW in behaviour and \
			(message.name == Messages.HOST_UP or \
			message.name == Messages.HOST_DOWN or \
			message.name == Messages.BEFORE_HOST_TERMINATE)	
	
	def _reload_nginx(self):
		nginx_pid = read_file(pid_file, logger = self._logger)
		if nginx_pid and nginx_pid.strip():
			try:
				self._logger.info("Reloading nginx")
				initd.reload("nginx")
				self._logger.debug("nginx reloaded")
			except:
				self._logger.error("Cannot reloaded nginx")
				raise

logger = logging.getLogger(__name__)