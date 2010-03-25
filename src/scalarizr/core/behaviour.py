'''
Created on Mar 24, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.util.disttool import DistTool
import os
import ConfigParser

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
	def configure(self, _interactive, **kwargs):
		pass

class AppConfigurator(BehaviourConfigurator):
	messages = {
			'httpd_conf_path_prompt' : 'Specify path to apache2 main config file',
			'vhosts_path_prompt' : 'Specify path to scalr vhosts dir'}
	
	def configure(self, _interactive, **kwargs):	
		httpd_conf_path_deb = '/etc/apache2/apache2.conf'
		httpd_conf_path_rpm = '/etc/httpd/httpd.conf'
		app_include = "etc/include/behaviour.app.ini"
		app_section = 'behaviour_app'
		httpd_conf_path = None
		vhosts_path = None
		
		if kwargs.has_key('vhosts_path'):
			vhosts_path = kwargs['vhosts_path']
			
		if kwargs.has_key('httpd_conf_path'):
			httpd_conf_path = kwargs['httpd_conf_path']
			
		else:
			if DistTool().is_debian_based() and os.path.exists(httpd_conf_path_deb):
				httpd_conf_path = httpd_conf_path_deb
			elif DistTool().is_redhat_based() and os.path.exists(httpd_conf_path_rpm):
				httpd_conf_path = httpd_conf_path_rpm
							
		if httpd_conf_path and not vhosts_path:
			vhosts_path = os.path.dirname(httpd_conf_path) + '/' + 'scalr-vhosts'
		
		if _interactive:			
			print "Interactive mode:"
			
			message = self.messages['httpd_conf_path_prompt'] + ' [or press enter to keep %s]:' % (httpd_conf_path)
			manual_input = raw_input(message)
			if manual_input:
				httpd_conf_path = manual_input
			
			message = self.messages['vhosts_path_prompt'] + ' [or press enter to keep %s]:' % (vhosts_path)	
			manual_input = raw_input(message)
			if manual_input:
				vhosts_path = manual_input
		
		else:
			if not httpd_conf_path or not vhosts_path:
				raise MissingDataError("Not enough information about apache2")
			
		config = ConfigParser.RawConfigParser()
		if os.path.exists(app_include):
			config.read(app_include)
			if not config.has_section(app_section):
				config.add_section(app_section)
		config.set(app_section, 'httpd_conf_path', httpd_conf_path)
		config.set(app_section, 'vhosts_path', vhosts_path)
		with open(app_include, 'w') as configfile:
			config.write(configfile)


class WwwConfigurator(BehaviourConfigurator):
	messages = {
			'binary_path_prompt' : 'Specify path to nginx',
			#'app_include_path_prompt' : 'Specify path to app includes dir',
			#'https_include_path_prompt' : 'Specify path to https includes dir',
			'port_prompt' : 'Specify nginx port'
			}
	
	def configure(self, _interactive, **kwargs):
		nginx_binary_path_deb = '/usr/sbin/nginx'
		nginx_binary_path_rpm = '/usr/local/nginx/sbin/nginx'
		nginx_binary = None 
		nginx_port = '80'
		www_include = "etc/include/behaviour.www.ini"
		www_section = 'behaviour_www'
		
		if kwargs.has_key('binary_path'):
			nginx_binary = kwargs['binary_path']
		
		if kwargs.has_key('app_port'):
			nginx_port = kwargs['app_port']			
			
		else:
			if DistTool().is_debian_based() and os.path.exists(nginx_binary_path_deb):
				nginx_binary = nginx_binary_path_deb
			elif DistTool().is_redhat_based() and os.path.exists(nginx_binary_path_rpm):
				nginx_binary = nginx_binary_path_rpm	

		if _interactive:			
			print "Interactive mode:"
			
			message = self.messages['binary_path_prompt'] + ' [or press enter to keep %s]:' % (nginx_binary)
			manual_input = raw_input(message)
			if manual_input:
				nginx_binary = manual_input
				
			message = self.messages['port_prompt'] + ' [or press enter to keep %s]:' % (nginx_port)
			manual_input = raw_input(message)
			if manual_input:
				nginx_port = manual_input
		else:
			if not nginx_binary:
				raise MissingDataError("Not enough information about nginx")
									
		config = ConfigParser.RawConfigParser()
		if os.path.exists(www_include):
			config.read(www_include)
			if not config.has_section(www_section):
				config.add_section(www_section)
		config.set(www_section, 'binary_path', nginx_binary)
		config.set(www_section, 'app_port', nginx_port)
		with open(www_include, 'w') as configfile:
			config.write(configfile)
				
def get_behaviour_ini_name(name):
	return "behaviour.%s.ini" % name

class MissingDataError(BaseException):
	pass
