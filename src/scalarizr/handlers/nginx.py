'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.behaviour import Behaviours
from scalarizr.messaging import Messages
from scalarizr.util import configtool, disttool
import os
import re
import shutil
import subprocess
import logging


def get_handlers():
	return [NginxHandler()]

class NginxHandler(Handler):
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service	
		bus.define_events("nginx_upstream_reload")	
	
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
		
		template_path = os.path.join(bus.etc_path, "public.d/handler.nginx/app-servers.tpl")
		
		if not os.path.exists(template_path):
			self._logger.warning("nginx template '%s' doesn't exists. Creating default template", template_path)
			template_dir = os.path.dirname(template_path)
			if not os.path.exists(template_dir):
				os.makedirs(template_dir)
			
			file = None
			try:
				file = open(template_path, "w+")
				file.write("""\nupstream backend {\n\tip_hash;\n\n\t${upstream_hosts}\n}\n""")
			except IOError, e:
				self._logger.error("Cannot write to %s file: %s" % (template_path, str(e)))
			finally:
				if file:
					file.close()
		
		template = ""
		file = None
		try:			
			file = open(template_path, 'r')
			template = file.read()
		except IOError, e:
			self._logger.error("Cannot read %s file: %s" % (template_path, str(e)))
		finally:
			if file:
				file.close()	

		# Create upstream hosts configuration
		upstream_hosts = ""
		for app_serv in self._queryenv.list_roles(behaviour = Behaviours.APP):
			for app_host in app_serv.hosts :
				upstream_hosts += "\tserver %s:%s;\n" % (app_host.internal_ip, app_port)
		if not upstream_hosts:
			self._logger.debug("Scalr returned empty app hosts list. Filling template with localhost only.")
			upstream_hosts = "\tserver 127.0.0.1:80;\n"
		
		template = template.replace("${upstream_hosts}", upstream_hosts)

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
		
		file = None
		if os.path.isfile(include):
			self._logger.debug("Reading old configuration from %s", include)
			try:
				file = open(include,'r')
				old_include = file.read()
			except IOError, e:
				self._logger.error("Cannot read %s file: %s" % (include, str(e)))
			finally:
				if file:
					file.close()
			
		if template == old_include:
			self._logger.info("nginx upstream configuration wasn`t changed.")
		else:
			self._logger.info("nginx upstream configuration was changed.")
			self._logger.debug("Creating backup config files.")
			if os.path.isfile(include):
				shutil.move(include, include+".save")
			self._logger.debug("Writing template to %s", include)
			
			file = None
			try:
				file = open(include, "w")
				file.write(template)
			except IOError, e:
				self._logger.error("Cannot write to %s file: %s" % (include, str(e)))
			finally:
				if file:
					file.close()
			
			nginx_conf = None
			file = None
			if not os.path.isfile(nginx_conf_path):
				self._logger.error("nginx main config file % does not exist", nginx_conf_path)
			else:
				self._logger.debug("Trying to read nginx main config %s", nginx_conf_path)
				
				try:
					file = open(nginx_conf_path,'r')
					nginx_conf = file.read()
				except IOError, e:
					self._logger.error("Cannot read %s file: %s" % (nginx_conf_path, str(e)))
				finally:
					if file:
						file.close()
												
				if nginx_conf:
					include_regexp = re.compile('^[^#\n]*?include\s*'+include+'\s*;\s*$', re.MULTILINE)
					if not re.search(include_regexp, nginx_conf):
						new_nginx_conf = re.sub(re.compile('(http\s*\{.*?)(\})', re.S),
								'\\1\n' + '    include '+ include + ';\n' + '\\2', nginx_conf)
						self._logger.debug("Including generated config to main nginx config %s", nginx_conf_path)
						
						try:
							file = open(nginx_conf_path,'w')
							file.write(new_nginx_conf)
						except IOError, e:
							self._logger.error("Cannot write to %s file: %s" % (nginx_conf_path, str(e)))
						finally:
							if file:
								file.close()
					
					else:
						self._logger.debug("File %s already included into nginx main config %s", 
									include, nginx_conf_path)
						
			if disttool._is_debian_based and os.path.isfile(default_conf_path):		
				self._logger.debug("Patching nginx default vhost file (for debian-based dists only)")
				
				default_content = None
				file = None
				try:
					file = open(default_conf_path)
					default_content = file.read()
				except IOError, e:
					self._logger.error("Cannot read %s file: %s" % (default_conf_path, str(e)))
				finally:
					if file:
						file.close()
						
				root_locat = re.compile('(?P<loc>^\s*location\s*/\s*\{[^\}]*?)(?P<root>^\s*root.*?;.*?$)(?P<endloc>[^\}]*?\})', re.DOTALL | re.MULTILINE)
				
				if default_content:
					if re.search(root_locat, default_content):
						new_content = re.sub(root_locat, '\\loc'+' '*16 +'proxy_pass http://backend;\\endloc', default_content)
						
						file = None 
						try:
							file = open(default_conf_path, 'w')
							file.write(new_content)
						except IOError, e:
							self._logger.error("Cannot write to %s file: %s" % (default_conf_path, str(e)))
						finally:
							if file:
								file.close()						
					
					else:
						location_re = re.compile('(?P<loc>^\s*location\s*/\s*\{\s*$)(?P<endloc>[^\}]*\})')
					
											
			#FIXME: use initd for starting, stopping & reloading nginx
			self._logger.info("Testing new configuration.")
			nginx_pid_file = "/var/run/nginx.pid"
			nginx_test_command = [nginx_binary, "-t"]
			
			p = subprocess.Popen(nginx_test_command, 
					stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			stdout, stderr = p.communicate()
			is_nginx_test_failed = p.poll()
			
			if os.path.isfile(nginx_binary) and is_nginx_test_failed:
				self._logger.error("Configuration error detected:" +  stderr + " Reverting configuration.")
				if os.path.isfile(include):
					shutil.move(include, include+".junk")
				if os.path.isfile(include+".save"):
					shutil.move(include+".save", include)
			
			elif os.path.isfile(nginx_binary) and os.path.isfile(nginx_pid_file):
				self._logger.info("Reloading nginx.")
				
				nginx_pid = None
				file = None
				try:
					file =  open(nginx_pid_file,'r')
					nginx_pid = file.read()
				except IOError, e:
					self._logger.error("Cannot read nginx pid file %s: %s" % (nginx_pid_file, str(e)))
				finally:
					if file:
						file.close()
						
				if nginx_pid.endswith('\n'):
					nginx_pid = nginx_pid[:-1]
					
				if nginx_pid :
					nginx_restart_command = ["kill","-HUP", nginx_pid]
					subprocess.call(nginx_restart_command)
				else:
					self._logger.info("/var/run/nginx.pid exists but empty (usually it so after configuration tests w/ running). Nginx hasn`t got HUP signal.")
				
			elif not os.path.isfile(nginx_binary):
				self._logger.info("Nginx not found.")
			elif not os.path.isfile(nginx_pid_file):
				self._logger.info("/var/run/nginx.pid does not exist. Probably nginx haven`t been started")
		
		bus.fire("nginx_upstream_reload")
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return Behaviours.WWW in behaviour and \
			(message.name == Messages.HOST_UP or message.name == Messages.HOST_DOWN)	
