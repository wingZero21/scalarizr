'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.handlers import Handler
from scalarizr.behaviour import Behaviours
from scalarizr.messaging import Messages
from scalarizr.util import configtool
import os
import shutil
import subprocess
import logging


def get_handlers():
	return [NginxHandler()]

class NginxHandler(Handler):
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service		
	
	def on_HostUp(self, message):
		self.nginx_upstream_reload()
	
	def on_HostDown(self, message):
		self.nginx_upstream_reload()
	
	def nginx_upstream_reload(self):
		config = bus.config
		sect_name = configtool.get_behaviour_section_name(Behaviours.WWW)
		nginx_bin = config.get(sect_name, "binary_path")
		nginx_incl = config.get(sect_name, "app_include_path")
		app_port = config.get(sect_name, "app_port") or "80"
			
		tpl_filename = os.path.join(bus.etc_path, "public.d/handler.nginx/app-servers.tpl")
		if not os.path.exists(tpl_filename):
			self._logger.warning("nginx template '%s' doesn't exists. Creating default template", tpl_filename)
			f = open(tpl_filename, "w+")
			f.write("""\nupstream backend {\n\tip_hash;\n\n\t${upstream_hosts}\n}\n""")
			f.close()
		include_tpl = open(tpl_filename, 'r').read()

		# Create upstream hosts configuration	
		upstream_hosts = ""
		for app_serv in self._queryenv.list_roles(behaviour = Behaviours.APP):
			for app_host in app_serv.hosts :
				upstream_hosts += "\tserver %s:%s;\n" % (app_host.internal_ip, app_port)

		if not upstream_hosts:
			upstream_hosts = "\tserver 127.0.0.1:80;\n"
		
		include_tpl = include_tpl.replace("${upstream_hosts}", upstream_hosts)

		#HTTPS Configuration		
		# openssl req -new -x509 -days 9999 -nodes -out cert.pem -keyout cert.key
		cert_path = configtool.get_key_filename("https.crt", private=True)
		pk_path = configtool.get_key_filename("https.key", private=True) 
		if os.path.isfile("/etc/nginx/https.include") and \
				os.path.isfile(cert_path) and os.path.isfile(pk_path):
			include_tpl += "include /etc/nginx/https.include;"
			
		#Determine, whether configuration was changed or not
		if os.path.isfile(nginx_incl) and include_tpl == open(nginx_incl,'r').read():
			self._logger.info("nginx upstream configuration wasn`t changed.")
		else:
			self._logger.info("nginx upstream configuration was changed.")
			if os.path.isfile(nginx_incl):
				shutil.move(nginx_incl, nginx_incl+".save")
			file = open(nginx_incl, "w")
			file.write(include_tpl)
			file.close()
			
			self._logger.info("Testing new configuration.")
			nginx_pid_file = "/var/run/nginx.pid"
			nginx_test_command = [nginx_bin, "-t"]
			
			p = subprocess.Popen(nginx_test_command, 
					stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
			stdout, stderr = p.communicate()
			is_nginx_test_failed = p.poll()
			
			if os.path.isfile(nginx_bin) and is_nginx_test_failed:
				self._logger.error("Configuration error detected:" +  stderr + " Reverting configuration.")
				if os.path.isfile(nginx_incl):
					shutil.move(nginx_incl, nginx_incl+".junk")
				if os.path.isfile(nginx_incl+".save"):
					shutil.move(nginx_incl+".save", nginx_incl)
			
			elif os.path.isfile(nginx_bin) and os.path.isfile(nginx_pid_file):
				self._logger.info("Reloading nginx.")
				nginx_pid = open(nginx_pid_file,'r').read()
				if nginx_pid.endswith('\n'):
					nginx_pid = nginx_pid[:-1]
				if "" != nginx_pid :
					nginx_restart_command = ["kill","-HUP", nginx_pid]
					subprocess.call(nginx_restart_command)
				else:
					self._logger.info("/var/run/nginx.pid exists but empty (usually it so after configuration tests w/ running). Nginx hasn`t got HUP signal.")
				
			elif not os.path.isfile(nginx_bin):
				self._logger.info("Nginx not found.")
			elif not os.path.isfile(nginx_pid_file):
				self._logger.info("/var/run/nginx.pid does not exist. Probably nginx haven`t been started")
		#call_user_code lib/nginx_reload
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return behaviour == Behaviours.APP and \
			(message.name == Messages.HOST_UP or message.name == Messages.HOST_DOWN)	
