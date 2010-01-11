'''
Created on Jan 6, 2010

@author: marat
'''

from scalarizr.core.handlers import Handler

def get_handlers():
	return [NginxHandler()]

class NginxHandler(Handler):
	
	def on_HostUp(self, message):
		self.nginx_upstream_reload()
	
	def on_HostDown(self, message):
		self.nginx_upstream_reload()
	
	def nginx_upstream_reload(self):
		import ConfigParser
		config = ConfigParser.RawConfigParser()
		config.read("etc/include/handler.nginx.ini")
		binary_path = config.get("handler_nginx","binary_path")
		app_include_path = config.get("handler_nginx","app_include_path")
		if config.get("handler_nginx","app_port"):
			app_port = config.get("handler_nginx","app_port")
		else:
			app_port = "80"
		
		import os
		num_of_appservers = 0
		if os.path.isfile("/usr/local/aws/templates/app-servers.tmpl"):
			upstream_hosts = ""
			# there is some fucking shit I wrote without understanding
			for app_serv in ec2_listhosts_app :
				upstream_hosts += "\tserver" + basename + app_serv + ":" + app_port + ";\n"
				num_of_appservers = num_of_appservers + 1
			
			if 0 == num_of_appservers :
				upstream_hosts = "\tserver 127.0.0.1:80;"
			
			
			

	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return behaviour == "app" and (message.name == "HostUp" or message.name == "HostDown")	
