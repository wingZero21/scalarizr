'''
Created on Mar 24, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.util import configtool
from ConfigParser import ConfigParser
from optparse import Option
import os

class Behaviours:
	WWW = "www"	
	APP = "app"
	MYSQL = "mysql"
	
	
def get_configurator(name):
	if name == Behaviours.APP:
		return AppConfigurator()
	elif name == Behaviours.WWW:
		return WwwConfigurator()
	else:
		return BehaviourConfigurator()

class BehaviourConfigurator:
	cli_options = []
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
		config = ConfigParser()
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
		self.cli_options = [
			Option("--app-httpd-conf-path", dest="httpd_conf_path", 
					help="Path to your httpd configuration file"),
			Option("--app-vhosts-path", dest="vhosts_path", 
					help="Path to directory where scalarizr will place virtual hosts configurations")
		]
		self.platform_section = configtool.get_behaviour_section_name(Behaviours.APP)
		self.include_ini_filename = configtool.get_behaviour_filename(Behaviours.APP, ret=configtool.RET_PUBLIC) 
	
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
		self.cli_options = (
			Option("--www-binary-path", dest="binary_path", help="Path to nginx binary"),
			Option("--www-app-port", dest="app_port", help="Apache port number"),
			Option("--www-app-include-path", dest="app_include_path", help="TODO: write description"),
			Option("--www-https-include-path", dest="https_include_path", help="TODO: write description")
		)
		self.platform_section = configtool.get_behaviour_section_name(Behaviours.WWW)
		self.include_ini_filename = configtool.get_behaviour_filename(Behaviours.WWW, ret=configtool.RET_PUBLIC)
	
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
