'''
Created on Mar 24, 2010

@author: marat
@author: Dmytro Korsakov
'''
import os
import ConfigParser
from scalarizr.core import Bus, BusEntries

class Behaviours:
	APP = "app"
	MYSQL = "mysql"
	WWW = "www"
	
	
def get_configurator(name):
	if name == Behaviours.APP:
		return AppConfigurator()
	elif name == Behaviours.WWW:
		return WwwConfigurator()
	return None

class BehaviourConfigurator:
	options = {}
	"""
	{name: ("prompt", default_value, finder)}
	"""
	
	platform_section = None
	include_ini_filename = None	
	
	def configure(self, _interactive, **kwargs):
		#fill data from arguments
		for key, value in kwargs.items():
			self.options[key][1] = value
		#fill missing parts from specific functions
		for key, value in self.options.items():
			if not value[1]:
				value[1] = value[2]()
		#interactive section allows change data in manual mode
		if _interactive:	
			for key, value in self.options.items():
				message = value[0] + ' [or press enter to keep %s]:' % (value[1])
				manual_input = raw_input(message)
				if manual_input:
					self.options[key][1] = manual_input
		else:
			#needs to check if all data entries exist
			for key, value in self.options.items():
				if not value[1]:
					raise MissingDataError("Not enough information." + value[0])
		#write to specific ini-file		
		config = ConfigParser.RawConfigParser()
		if os.path.exists(self.include_ini_filename):
			#needs try block
			config.read(self.include_ini_filename)
			if not config.has_section(self.platform_section):
				config.add_section(self.platform_section)
				
		for key, value in self.options.items():
			config.set(self.platform_section, key , value[1])
		configfile = open(self.include_ini_filename, 'w')
		config.write(configfile)
			
class AppConfigurator(BehaviourConfigurator):
	
	def __init__(self):
		self.options = dict(
			httpd_conf_path=["Specify path to apache2 main config file", None, self.find_apache_conf],
			vhosts_path=["Specify path to scalr vhosts dir", None, self.get_scalr_vhosts_dir]
		)
		self.platform_section = 'behaviour_app'
		self.include_ini_filename = os.path.join(Bus()[BusEntries.BASE_PATH], "etc/include/behaviour.app.ini") 
	
	def find_apache_conf(self):
		known_places = ("/etc/apache2/apache2.conf", "/etc/httpd/httpd.conf")
		for config in known_places:
			if os.path.exists(config):
				return config
		return ""
	
	def get_scalr_vhosts_dir(self):
		apache_conf_path = self.find_apache_conf()
		if apache_conf_path:
			return os.path.dirname(apache_conf_path) + '/' + 'scalr-vhosts'
		return "/"		

class WwwConfigurator(BehaviourConfigurator):
	
	def __init__(self):
		self.options = dict(
			binary_path=["Specify path to nginx", None, self.find_nginx_bin],
			app_port=["Specify apache port", None, self.get_app_port],
			app_include_path=["Specify app_include_path", None, self.get_app_include_path],
			https_include_path=["Specify https_include_path", None, self.get_https_include_path]
		)
		self.platform_section = 'behaviour_www'
		self.include_ini_filename = os.path.join(Bus()[BusEntries.BASE_PATH], "etc/include/behaviour.www.ini")
	
	def find_nginx_bin(self):
		known_places = ('/usr/sbin/nginx', '/usr/local/nginx/sbin/nginx')
		for config in known_places:
			if os.path.exists(config):
				return config
		return ""
	
	def get_app_port(self):
		return "80"		
	
	def get_app_include_path(self):
		return "/etc/nginx/app-servers.include"
	
	def get_https_include_path(self):
		return "/etc/nginx/https.include"

				
def get_behaviour_ini_name(name):
	return "behaviour.%s.ini" % name

class MissingDataError(BaseException):
	pass
