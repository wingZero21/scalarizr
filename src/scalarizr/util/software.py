'''
Created on Sep 10, 2010

@author: marat
'''
from scalarizr.util import disttool, system
import os, re

__all__ = ('all_installed', 'software_info', 'explore', 'whereis')

def all_installed():
	ret = []
	for getinfo_func in software_list.itervalues():
		try:
			ret.append(getinfo_func())
		except:
			pass
	return ret
		

def software_info(name):
	if not software_list.has_key(name):
		raise Exception("Unknown software: %s" % name)
	return software_list[name]()

def explore(name, lookup_fn):
	if name in software_list.keys():
		raise Exception("'%s' software has been already explored" % name)
	software_list[name] = lookup_fn
	
def whereis(name):
	'''
	Search executable in /bin /sbin /usr/bin /usr/sbin /usr/libexec /usr/local/bin /usr/local/sbin
	@rtype: tuple
	'''
	places = ['/bin', '/sbin', '/usr/bin', '/usr/sbin', '/usr/libexec', '/usr/local/bin', '/usr/local/sbin']
	return tuple([os.path.join(place, name) for place in places if os.path.exists(os.path.join(place, name))])

class SoftwareError(BaseException):
	pass

class SoftwareInfo:
	name = None	
	version = None	
	'''
	@param version: tuple(major, minor, bugfix)
	'''
	version_string = None
	
	def __init__(self, name, version, version_string):
		self.name    		= name
		self.version_string = version_string
		self.version		= tuple(version.split('.'))		
	
software_list = dict()

def mysql_software_info():
	
	binaries = whereis('mysqld')
	if not binaries:
		raise SoftwareError("Can't find executable for MySQL server")

	version_string = system((binaries[0], '-V'), False)[0].strip()
	if not version_string:
		raise SoftwareError

	res = re.search('Ver\s+(\d+(\.[\d-]+){2})', version_string)
	if res:
		version = res.group(1)
		return SoftwareInfo('mysql', version, version_string)
	raise SoftwareError


explore('mysql', mysql_software_info)

def nginx_software_info():
	binaries = whereis('nginx')
	if not binaries:
		raise SoftwareError("Can't find executable for Nginx server")
	
	out = system((binaries[0], '-V'), False)[1]
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	res = re.search('\d+(\.[\d-]+){2}', version_string)
	if res:
		version = res.group(0)
		return SoftwareInfo('nginx', version, out)
	raise SoftwareError


explore('nginx', nginx_software_info)

def memcached_software_info():
	binaries = whereis('memcached')
	if not binaries:
		raise SoftwareError("Can't find executable for Memcached")
	
	out = system((binaries[0], '-h'), False)[0]
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	
	res = re.search('memcached\s+(\d+(\.[\d-]+){2})', version_string)
	if res:
		version = res.group(1)
		return SoftwareInfo('memcached', version, version_string)
	raise SoftwareError

explore('memcached', memcached_software_info)

def php_software_info():
	binaries = whereis('php')
	if not binaries:
		raise SoftwareError("Can't find executable for php interpreter")
	
	out = system((binaries[0], '-v'), False)[0]
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	
	res = re.search('PHP\s+(\d+(\.[\d-]+){2})', version_string)
	
	if res:
		version = res.group(1)
		return SoftwareInfo('php',version, out)
	raise SoftwareError

explore('php', php_software_info)

def python_software_info():	
	binaries = whereis('python')
	if not binaries:
		raise SoftwareError("Can't find executable for python interpreter")

	version_string = system((binaries[0], '-V'), False)[1].strip()
	if not version_string:
		raise SoftwareError
	
	version_string = version_string.splitlines()[0]
	
	res = re.search('Python\s+(\d+(\.[\d-]+){2})', version_string)
	
	if res:
		version = res.group(1)
		return SoftwareInfo('python', version, version_string)
	
	raise SoftwareError

explore('python', python_software_info)

def apache_software_info():

	binary_name = "httpd" if disttool.is_redhat_based() else "apache2"
	binaries = whereis(binary_name)
	if not binaries:
		raise SoftwareError("Can't find executable for apache http server")
		
	out = system((binaries[0], '-V'), False)[0]
	if not out:
		raise SoftwareError
	
	version_string = out.splitlines()[0]
	res = re.search('\d+(\.[\d-]+){2}', version_string)
	if res:
		version = res.group(0)
	
		return SoftwareInfo('apache', version, out)
	raise SoftwareError


explore('apache', apache_software_info)

def tomcat_software_info():
	tomcat_dir = [os.path.join('/usr/share', location) for location in os.listdir('/usr/share') if 'tomcat' in location]
	if not tomcat_dir:
		raise SoftwareError("Can't find tomcat server location")
	version_script_path = os.path.join(tomcat_dir[0], 'bin/version.sh')
	if not os.path.exists(version_script_path):
		raise SoftwareError("Version script doesn't exist")
		
	out = system(version_script_path, False)[0]
	if not out:
		raise SoftwareError
	res = re.search(re.compile('^Server\s+version:.*?(\d+(\.[\d-]+){2})\s*$', re.M), out)
	if res:
		version = res.group(1)
		return SoftwareInfo('tomcat', version, out)
	raise SoftwareError

explore('tomcat', tomcat_software_info)

