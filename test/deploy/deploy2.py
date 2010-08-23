'''
Created on 23.08.2010

@author: shaitanich
'''

# sudo apt-get install python-paramiko 

from optparse import OptionParser
import logging.handlers
import ConfigParser
import subprocess
import paramiko
import logging
import sys
import os

class BaseCollector:
	
	#sections
	SETTINGS = 'settings'
	APP = 'app'
	WWW = 'www'
	MYSQL = 'mysql'
	CASSANDRA = 'cassandra'
	MEMCACHED = 'memcached'
	BASE= 'base'
	ALL = 'ALL'
	FILES = 'FILES'
	SCRIPTS = 'scripts'
	LOCAL_SCRIPTS = 'local_scripts'
	
	TAR_FILE = 'scalarizr.tar.gz'
	PACK = 'tar -czf %s ' % TAR_FILE
	UNPACK = 'tar -xzf %s -C ' % TAR_FILE
	REMOTE_LOG = '/var/log/scalarizr.log'

	
	formatter = None
	
	def __init__(self, formatter):
		self.formatter = formatter
	
	def add_data(self):
		pass


class Collector(BaseCollector):
	
	def get_collectors(self):
		return [DefaultDataCollector, ArgsCollector, ConfigCollector]
	
	def add_data(self):
		for collector in self.get_collectors():
			self.data.update(collector.get_formatted_data())
			
			
class DefaultDataCollector(BaseCollector):
	def add_data(self):
		pass	
			
			
class ArgsCollector(BaseCollector):
	
	def _get_execution_params(self):
		
		parser = OptionParser(usage="Usage: %prog key IP platform \
			[source behaviour init_script, post_script debug_log]")
		
		parser.add_option("-k", "--key", dest="key", help="path private key")
		parser.add_option("-i", "--ip", dest="ip", help="instance ip")
		parser.add_option("-p", "--platform", dest="platform", help="[ubuntu8, ubuntu10, centos]")
		
		parser.add_option("-s", "--source", dest="source", help="path to scalarizr src")	
		parser.add_option("-e", "--etc", dest="etc", help="path to scalarizr etc")
		parser.add_option("-b", "--behaviour", dest="behaviour", default='base', help="[base, app, www, mysql, cassandra, memcached]")
		parser.add_option("--init_script", dest="init_script", help=" path to init file. Uses init-%platform% file in current dir by default")
		parser.add_option("--post_script", dest="post_script", help="path to post.sh")
		parser.add_option("-d", "--debug", dest="debug", default='1', help="set debug level")
		parser.add_option('-l', '--get_log', dest="get_log", default='1', help='Fetch log file from instance.')
		
		(options, args) = parser.parse_args()
		
		if not options.key or not options.ip or not options.platform:
			print parser.format_help()
			sys.exit()
			
		if not os.path.exists(options.key):
			print 'Wrong RSA key path. Exit'
			sys.exit()
		
		if options.source and not os.path.exists(options.source):
			print 'Wrong source path. Exit'
			sys.exit()
		
		return {'key':options.key, 'ip':options.ip, 'platform':options.platform, 'source':options.source\
			, 'behaviour':options.behaviour, 'init_script':options.init_script, 'post_script':options.post_script\
			, 'debug':options.debug, 'get_log':options.get_log, 'etc':options.etc}
	
	def add_data(self):
		raw_params = self._get_execution_params()
		self.formatter.build_connection(raw_params['key'], None, raw_params['ip'])
		# call build_local_scripts and other Formatter methods	
		pass 
	
	
class ConfigCollector(BaseCollector):
	def add_data(self):
		pass
	
	
class EnvCollector(BaseCollector):
	def add_data(self):
		pass
		

class Formatter:
	
	data = None
	
	def __init__(self):
		
		self.local_scripts = 'local_scripts'
		self.remote_scripts = 'remote_scripts'
		self.move = 'move'
		self.upload = 'upload'
		self.fetch = 'fetch'
		self.connection = 'connection'
		self.temp = 'temp'
		self.data = {}
		
	def _add(self,formatted_data):
		key = formatted_data.keys()[1]
		if self.data.has_key(key) and type(self.data[key]) == type([]):
			self.data[key] += formatted_data[key]
		else:
			self.data[key] = formatted_data[key]
	
	def build_local_scripts(self, path, arguments):
		self._add( {self.local_scripts : [{'path' : path,'arguments' : arguments}]} )
	
	def build_remote_scripts(self, path, arguments):
		self._add( {self.remote_scripts : [{'path' : path,'arguments' : arguments}]} )
	
	def build_move(self, src, dest):
		self._add( {self.move : [{'src' : src, 'dest' :dest}]} )
	
	def build_upload(self, path, remote_path):
		self._add( {self.upload : [{'path' : path, 'remote_path' : remote_path}]} )
	
	def build_fetch(self, remote_path):
		self._add( {self.fetch : [{'remote_path' : remote_path}]} )
	
	def build_connection(self, key, user, ip):
		self._add( {self.connection : [{'key' : key, 'user' : user, 'ip' : ip}]} )
	
	def add_temp_dir(self, dir):
		self.add({self.data : dir})
		
	
	def get_local_scripts_list(self):
		return self.data[self.local_scripts]
	
	def get_remote_scripts_list(self):
		return self.data[self.remote_scripts]
	
	def get_move_list(self):
		return self.data[self.move]
	
	def get_upload_list(self):
		return self.data[self.upload]
	
	def get_fetch_list(self):
		return self.data[self.fetch]
	
	def get_connection(self):
		return self.data[self.connection]
	
	def _get_temp_dir(self):
		return self.data[self.temp]

class Executor:
	
	formatter = None
	connection = None
	
	def __init__(self, formatter):
		self.connection = formatter.get_connection()
		self.formatter = formatter

		
	def run(self):
		self._local_execution(self.formatter)
		self._remote_execution(self.formatter)
		
	
	def _local_execution(self, formatter):
		for script in formatter.get_local_scripts_list():
			pass
		
	def _remote_execution(self, formatter):
		for script in formatter.get_remote_scripts_list():
			pass
		
	def _move_remote(self, formatter):
		for data in formatter.get_move_list():
			pass
		
	def _upload(self, formatter):
		temp = formatter._get_temp_dir()
		for file in formatter.get_upload_list():
			pass
		
	def _fetch(self, formatter):
		for file in formatter.get_fetch_list():
			pass
		
	
if __name__ == '__main__':
	try:
		
		formatter = Formatter()
		
		collector = Collector(formatter)
		collector.add_data()
		
		executor = Executor(formatter)
		executor.run()
		
	except KeyboardInterrupt:
		print 'Interrupted!'