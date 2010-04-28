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
	elif name == Behaviours.MYSQL:
		return MySqlConfigurator()
	return None

class BehaviourConfigurator:
	cli_options = []
	options = {}
	"""
	{name: ("prompt", default_value, finder)}
	"""
	
	section_name = None
	include_ini_filename = None	
	
	def configure(self, interactive, **kwargs):
		config = ConfigParser()
		config.read(self.include_ini_filename)
		sect = configtool.section_wrapper(config, self.section_name)
		
		#fill data from config and kwargs
		for key in self.options:
			self.options[key][1] = (kwargs[key] if key in kwargs else None) or sect.get(key) 
			
		#fill missing parts from specific functions
		for key, value in self.options.items():
			if not value[1]:
				value[1] = value[2]()
				
		#interactive section allows change data in manual mode
		if interactive:	
			for key, value in self.options.items():
				message = '%s [%s]:' % (value[0], value[1]) if value[1] else '%s:' % (value[0])
				manual_input = raw_input(message)
				if manual_input:
					self.options[key][1] = manual_input
		else:
			#needs to check if all data entries exist
			for key, value in self.options.items():
				if not value[1]:
					raise MissingDataError("Option missed. " + value[0])
				
		#write to specific ini-file		
		sections = {self.section_name: {}}
		for key, value in self.options.items():
			sections[self.section_name][key] = value[1]
		configtool.update(self.include_ini_filename, sections)
			
class AppConfigurator(BehaviourConfigurator):
	
	def __init__(self):
		self.options = dict(
			httpd_conf_path=["Enter path to apache2 main config file", None, self.find_apache_conf],
			vhosts_path=["Enter path to scalr vhosts dir", None, self.get_scalr_vhosts_dir]
		)
		self.cli_options = [
			Option("--app-httpd-conf-path", dest="httpd_conf_path", 
					help="Path to your httpd configuration file"),
			Option("--app-vhosts-path", dest="vhosts_path", 
					help="Path to directory where scalarizr will place virtual hosts configurations")
		]
		self.section_name = configtool.get_behaviour_section_name(Behaviours.APP)
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
			binary_path=["Enter path to nginx binary", None, self.find_nginx_bin],
			app_port=["Enter apache port", None, self.get_app_port],
			app_include_path=["Enter app_include_path", None, self.get_app_include_path],
			https_include_path=["Enter https_include_path", None, self.get_https_include_path]
		)
		self.cli_options = (
			Option("--www-binary-path", dest="binary_path", help="Path to nginx binary"),
			Option("--www-app-port", dest="app_port", help="Apache port number"),
			Option("--www-app-include-path", dest="app_include_path", help="TODO: write description"),
			Option("--www-https-include-path", dest="https_include_path", help="TODO: write description")
		)
		self.section_name = configtool.get_behaviour_section_name(Behaviours.WWW)
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

class MySqlConfigurator(BehaviourConfigurator):
	def __init__(self):
		self.section_name = configtool.get_behaviour_section_name(Behaviours.MYSQL)
		self.include_ini_filename = configtool.get_behaviour_filename(Behaviours.MYSQL, ret=configtool.RET_PUBLIC)
				
def get_behaviour_ini_name(name):
	return "behaviour.%s.ini" % name

class MissingDataError(BaseException):
	pass
