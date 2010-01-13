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
		import cPickle
		import os
		import shutil
		import ConfigParser
		config = ConfigParser.RawConfigParser()
		config.read("etc/include/handler.nginx.ini")
		nginx_bin = config.get("handler_nginx","binary_path")
		nginx_incl = config.get("handler_nginx","app_include_path")
		if config.get("handler_nginx","app_port"):
			app_port = config.get("handler_nginx","app_port")
		else:
			app_port = "80"
		tmp_incl = ""
		num_of_appservers = 0
		if os.path.isfile("/usr/local/aws/templates/app-servers.tmpl"):
			upstream_hosts = ""
			# At first there need to call list_roles method
			ec2_listhosts_app = None
			basename_app_serv = None
			
			for app_serv in ec2_listhosts_app :
				upstream_hosts += "\tserver" + basename_app_serv + ":" + app_port + ";\n"
				num_of_appservers = num_of_appservers + 1
			
			if 0 == num_of_appservers :
				upstream_hosts = "\tserver 127.0.0.1:80;"
			
			if "" != upstream_hosts:
				#with open(os.environ['TMP_INCL'], 'w') as fo: for line in open('/usr/local/aws/templates/app-servers.tmpl', 'r'): fo.write(line.replace('@@UPSTREAM_HOSTS@@', os.environ.get('UPSTREAM_HOSTS')))
				tmp_incl = cPickle.load(file("/usr/local/aws/templates/app-servers.tmpl",'r'))
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
		
		if os.path.isfile("/etc/nginx/https.include") & os.path.isfile("/etc/aws/keys/ssl/https.key") :
			#Needs one more file checking (cert)? See in original code line 80
			tmp_incl += "include /etc/nginx/https.include;"
		#Determine, whether configuration was changed or no
		if tmp_incl == cPickle.load(file(nginx_incl,'r')):
			#log "nginx upstream configuration wasn`t changed."
			pass
		else:
			#log "nginx upstream configuration changed."
			shutil.move(nginx_incl, nginx_incl+".save")
			shutil.move(tmp_incl, nginx_incl)
			#log "Testing new configuration."
			if os.path.isfile(nginx_bin): #&& NG_LOG=`$NGINX_BIN -t 2>&1`
				#log "Configuration error detected: '$NG_LOG'. Reverting configuration."
				shutil.move(nginx_incl, nginx_incl+".junk")
				shutil.move(nginx_incl+".save", nginx_incl)
			elif os.path.isfile("/var/run/nginx.pid"):
				#log "Reloading nginx."
				os.system("kill -HUP "+ cPickle.load(file("/var/run/nginx.pid",'r')))
		#call_user_code lib/nginx_reload
				
			

	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return behaviour == "app" and (message.name == "HostUp" or message.name == "HostDown")	
