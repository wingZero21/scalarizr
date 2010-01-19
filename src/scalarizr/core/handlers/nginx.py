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
				    upstream_hosts += "\tserver" + app_hosts.internal_ip + ":" + app_port + ";\n"
				    num_of_appservers = num_of_appservers + 1
			
			if 0 == num_of_appservers :
				upstream_hosts = "\tserver 127.0.0.1:80;"
			
			if "" != upstream_hosts:
				tmp_incl = open(template_file,'r').read()
				tmp_incl = tmp_incl.replace("@@UPSTREAM_HOSTS@@",upstream_hosts)
		else:
			tmp_incl += "upstream backend {" + "\tip_hash;\n"
			for app_serv in ec2_listhosts_app : 
				tmp_incl += "\tserver" + ":" + app_port + ";"
				num_of_appservers = num_of_appservers + 1
			if 0 == num_of_appservers : 
				tmp_incl += "\tserver 127.0.0.1:80;"
			tmp_incl = tmp_incl + "}"
			
		#HTTPS Configuration		
		if os.path.isfile("/etc/nginx/https.include") and \
		os.path.isfile("/etc/aws/keys/ssl/https.key") and \
		os.path.isfile("/etc/aws/keys/ssl/https.cert") :
			#Needs one more file checking (cert)? See in original code line 80
			tmp_incl += "include /etc/nginx/https.include;"
		#Determine, whether configuration was changed or no
		if tmp_incl == open(nginx_incl,'r').read():
			self._logger.info("nginx upstream configuration wasn`t changed.")
			pass
		else:
			self._logger.info("nginx upstream configuration changed.")
			shutil.move(nginx_incl, nginx_incl+".save")
			shutil.move(tmp_incl, nginx_incl)
			self._logger.info("Testing new configuration.")
			if os.path.isfile(nginx_bin) and (not subprocess.call([nginx_bin, "-t"])):
				self._logger.error("Configuration error detected: '$NG_LOG'. Reverting configuration.")
				shutil.move(nginx_incl, nginx_incl+".junk")
				shutil.move(nginx_incl+".save", nginx_incl)
			elif os.path.isfile("/var/run/nginx.pid"):
				self._logger.info("Reloading nginx.")
				os.system("kill -HUP "+ open("/var/run/nginx.pid",'r').read())
		#call_user_code lib/nginx_reload
				
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return behaviour == "app" and (message.name == "HostUp" or message.name == "HostDown")	
