'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
'''

from scalarizr.core.handlers import Handler

def get_handlers():
	return [NginxHandler()]

class NginxHandler(Handler):
	_logger = None
	_queryenv = None
	
	def on_HostUp(self, message):
		self.nginx_upstream_reload()
	
	def on_HostDown(self, message):
		self.nginx_upstream_reload()
	
	def nginx_upstream_reload(self):
		import os
		import shutil
		from scalarizr.core import Bus, BusEntries
		import logging
		import subprocess
		
		self._logger = logging.getLogger(__package__ + "." + self.__class__.__name__)
		
		bus = Bus()
		self._queryenv = bus[BusEntries.QUERYENV_SERVICE]
		
		config = bus[BusEntries.CONFIG]
		nginx_bin = config.get("handler_nginx","binary_path")
		nginx_incl = config.get("handler_nginx","app_include_path")
		if config.get("handler_nginx","app_port"):
			app_port = config.get("handler_nginx","app_port")
		else:
			app_port = "80"
			
		tmp_incl = ""
		num_of_appservers = 0
		template_file = bus[BusEntries.BASE_PATH]+"/etc/include/handler.nginx/app-servers.tpl"
		if os.path.isfile(template_file):
			upstream_hosts = ""
			ec2_listhosts_app = self._queryenv.list_roles(behaviour = "app")
			for app_serv in ec2_listhosts_app :
				for app_hosts in app_serv.hosts :
				    upstream_hosts += "\tserver " + app_hosts.internal_ip + ":" + app_port + ";\n"
				    num_of_appservers = num_of_appservers + 1
			
			if 0 == num_of_appservers :
				upstream_hosts = "\tserver 127.0.0.1:80;"
			
			if "" != upstream_hosts:
				tmp_incl = open(template_file,'r').read()
				tmp_incl = tmp_incl.replace("${upstream_hosts}",upstream_hosts)

		else:
			tmp_incl += "upstream backend {" + "\tip_hash;\n"
			ec2_listhosts_app = self._queryenv.list_roles(behaviour = "app")
			for app_serv in ec2_listhosts_app : 
				for app_hosts in app_serv.hosts :
					tmp_incl += "\tserver " + app_hosts.internal_ip + ":" + app_port + ";\n"
					num_of_appservers = num_of_appservers + 1
			if 0 == num_of_appservers : 
				tmp_incl += "\tserver 127.0.0.1:80;"
			tmp_incl = tmp_incl + "}"
		
		#HTTPS Configuration		
		# openssl req -new -x509 -days 9999 -nodes -out cert.pem -keyout cert.key
		if os.path.isfile("/etc/nginx/https.include") and \
		os.path.isfile("/etc/aws/keys/ssl/cert.key") and \
		os.path.isfile("/etc/aws/keys/ssl/cert.pem") :
			tmp_incl += "include /etc/nginx/https.include;"
			
		#Determine, whether configuration was changed or not
		if os.path.isfile(nginx_incl) and tmp_incl == open(nginx_incl,'r').read():
			self._logger.info("nginx upstream configuration wasn`t changed.")
		else:
			self._logger.info("nginx upstream configuration changed.")
			if os.path.isfile(nginx_incl):
				shutil.move(nginx_incl, nginx_incl+".save")
			file = open(nginx_incl, "w")
			file.write(tmp_incl)
			file.close()
			
			self._logger.info("Testing new configuration.")
			nginx_pid_file = "/var/run/nginx.pid"
			nginx_test_command = [nginx_bin, "-t"]
			
			p = subprocess.Popen(nginx_test_command, \
								 stdin=subprocess.PIPE, \
								 stdout=subprocess.PIPE, \
								 stderr=subprocess.PIPE)
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
		return behaviour == "app" and (message.name == "HostUp" or message.name == "HostDown")	
