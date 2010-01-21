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
		template_file = bus[BusEntries.BASE_PATH]+"/etc/include/handler.nginx/app-servers.tplm"
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
			
		#Determine, whether configuration was changed or no
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
			nginx_pid = "/var/run/nginx.pid"
			
			if os.path.isfile(nginx_bin) and subprocess.call([nginx_bin, "-t"]):
				self._logger.error("Configuration error detected: '$NG_LOG'. Reverting configuration.")
				if os.path.isfile(nginx_incl):
					shutil.move(nginx_incl, nginx_incl+".junk")
				if os.path.isfile(nginx_incl+".save"):
					shutil.move(nginx_incl+".save", nginx_incl)
			elif os.path.isfile(nginx_bin) and os.path.isfile(nginx_pid):
				self._logger.info("Reloading nginx.")
				#os.system("kill -HUP "+ open(nginx_pid,'r').read())
				import subprocess
				nginx_restart_command = "kill -HUP "+ open(nginx_pid,'r').read()
				proc = subprocess.Popen(nginx_restart_command,
                       shell=True,
                       stdin=subprocess.PIPE,
                       stdout=subprocess.PIPE,
                       stderr=subprocess.PIPE,
                       )
				stdout_value, stderr_value = proc.communicate('through stdin to stdout')
				print '\tpass through:', repr(stdout_value)
				print '\tstderr:', repr(stderr_value)
				
				
			elif not os.path.isfile(nginx_bin):
				self._logger.error("Nginx not found.")
				print nginx_bin
			elif not os.path.isfile(nginx_pid):
				self._logger.debug("/var/run/nginx.pid does not exist. Probably nginx haven`t been started")
		#call_user_code lib/nginx_reload
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return behaviour == "app" and (message.name == "HostUp" or message.name == "HostDown")	
