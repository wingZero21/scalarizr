'''
Created on Aug 19, 2010

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

TAR_FILE = 'scalarizr.tar.gz'
PACK = 'tar -czf %s ' % TAR_FILE
UNPACK = 'tar -xzf %s -C ' % TAR_FILE


class Parametres():
	
	params = None
	
	def __init__(self):
		self.params = {}
	
	def get_params(self):
		execution_params = self._get_execution_params()
		self._add_default_params(execution_params)
		
		platform = execution_params['platform']
		config_params = self._get_config_params(platform)
		
		self.params.update(execution_params)
		self.params.update(config_params)
		self.params.update(self._get_import_command())
		
		return self.params
	
	def _get_execution_params(self):
		
		parser = OptionParser(usage="Usage: %prog key IP platform \
			[source behaviour init_script, post_script debug_log]")
		
		parser.add_option("-k", "--key", dest="key", help="private key")
		parser.add_option("-i", "--ip", dest="ip", help="instance ip")
		parser.add_option("-p", "--platform", dest="platform", help="[ubuntu8, ubuntu10, centos]")
		
		parser.add_option("-s", "--source", dest="source", help="path to scalarizr src dir")	
		parser.add_option("-b", "--behaviour", dest="behaviour", default='base', help="[base, app, www, mysql, cassandra, memcached]")
		parser.add_option("--init_script", dest="init_script", help=" path to init file")
		parser.add_option("--post_script", dest="post_script", help="path to post.sh")
		parser.add_option("-d", "--debug", dest="debug", default='1', help="set debug level")
		
		(options, args) = parser.parse_args()
		
		if not options.key or not options.ip or not options.platform:
			print parser.format_help()
			sys.exit()
		
		return options
	
	def _add_default_params(self,params):
		if not params['init_script'] and os.path.isfile(os.path.realpath('scalarizr')):
			params['init_script'] = os.path.realpath('scalarizr')
			
		if not params['post_script'] and os.path.isfile(os.path.realpath('post.sh')):
			params['post_script'] = os.path.realpath('post.sh')
		
		if not params['source']: 
			local_path = os.path.dirname(__file__)
			if os.path.exists(os.path.join(local_path, 'scalarizr')):
				params['source'] = local_path
	
	def _get_config_params(self, platform):
		
		options = {}
			
		config = ConfigParser.ConfigParser()
		config_path = platform + '.ini'
		config.read(config_path)
		
		try:
			options['login'] = config.get(SETTINGS, 'login')
			options['tempdir'] = config.get(SETTINGS, 'tempdir')
			options['remote_src_path'] = config.get(SETTINGS, 'src_path')
			options['sudo_cmd'] = config.get(SETTINGS, 'sudo_cmd')
			options['repo_cmd'] = config.get(SETTINGS, 'repo_cmd')
			options['install_cmd'] = config.get(SETTINGS, 'install_cmd')
			
			for key in ('login' , 'tempdir' , 'remote_src_path', 'sudo_cmd', 'repo_cmd', 'install_cmd'):
				if not options[key]:
					print 'Not enough parameters in %s behaviour settings file. Exit.'% platform	
					sys.exit()		
		
		except ConfigParser.NoSectionError, e:
			print e
			sys.exit()
		
		if config.has_section(ALL):
			options['apps'] =  config.get(ALL, 'apps')
		
		if config.has_section(options.behaviour):
			options['apps'] += ' ' + config.get(options.behaviour, 'apps')
			options['install_cmd'] = options['install_cmd'].replace('_list_', options['apps'])

		return options
	
	def _get_import_command(self):
		return {'import_cmd':raw_input('Copy input command here:')}
	
class Composer():
	
	commands = None
	
	def __init__(self):
		self.commands = []
		
	def compose(self, params):
		local = self._compose_local_commands(params)
		sftp = self._get_sftp_paths(params)
		remote = self._compose_remote_commands(params)
		return (local, sftp, remote)
	
	def _compose_local_commands(self, params):
		commands = []
		# tar src
		commands.append(PACK + params['source'] + ' scalarizr')
		# FUTURE: tar etc
		return commands
	
	def _compose_remote_commands(self, params):
		commands = []
		# sudo		
		commands.append(params['sudo_cmd'])
		# repo
		commands.append(params['repo_cmd'])
		# install apps
		commands.append(params['install_cmd'])
		# unzip src tar
		commands.append(UNPACK + ['remote_src_path'])
		# mv init
		init_remote_source = os.path.join(params['tempdir'], os.path.basename(params['init_script']))
		init_remote_dest = '/etc/init.d/scalarizr'
		commands.append('mv %s %s' % (init_remote_source, init_remote_dest))
		# chmod init
		commands.append('chmod +x %s' % init_remote_dest)
		# chmod post.sh
		post_remote_path = os.path.join(params['tempdir'], os.path.basename(params['post_script']))
		commands.append('chmod +x %s' % post_remote_path)
		# execute post.sh
		commands.append(post_remote_path)
		#mv logging if logging
		if params['debug']=='1':
			commands.append('mv /etc/scalr/logging-debug.ini /etc/scalr/logging.ini')
		# execute import
		commands.append(params['import_cmd'])
		# FUTURE: unzip etc tar
		return commands
	
	def _get_sftp_paths(self, params):
		paths = {}
		#upload src tar
		paths[TAR_FILE] = os.path.join(params['tempdir'], TAR_FILE)
		#upload init
		paths[params['init_script']] = os.path.join(params['tempdir'], os.path.basename(params['init_script']))
		#upload post.sh
		paths[params['post_script']] = os.path.join(params['tempdir'], os.path.basename(params['post_script']))
		#FUTURE: upload etc tar
		return paths

	
class Executor():
	
	def __init__(self):
		#self.keyfile = keyfile
		#self.user = user
		#self.ip = ip
		
		console = logging.StreamHandler()
		console.setLevel(logging.DEBUG)
		
		LOG_FILENAME = self.ip + '.log'
		handler = logging.FileHandler(LOG_FILENAME)
		
		logger = logging.getLogger('MyLogger')
		logger.setLevel(logging.DEBUG)
		logger.addHandler(console)
		logger.addHandler(handler)
		
		self._logger = logger
	
	def run(self, commands):
		#self._run_with_paramiko(commands)
		self._pseudo_run(commands)
	
	def _pseudo_run(self, commands):	

		local_commands = commands[0]
		sftp_commands = commands[1]
		remote_commands = commands[2]
		
		self._logger.debug('Connecting to %s as %s with %s' 
				% (commands['ip'], commands['login'], commands['key']))
		
		for command in local_commands:
			self._logger(command)
	
		for local,remote in sftp_commands:
			self._logger('Uploading %s to %s' % (local, remote))
		
		for command in remote_commands:
			self.logger.debug('Executing on remote server: %s' % command)
	
	def _run_with_paramiko(self, commands):	

		local_commands = commands[0]
		sftp_commands = commands[1]
		remote_commands = commands[2]
		
		for command in local_commands:
			self.system(command)
		
		ssh = paramiko.SSHClient()
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		ssh.connect(commands['ip'], username=commands['login'], key_filename=commands['key'])
		
		ftp=ssh.open_sftp()  
	
		for local,remote in sftp_commands:
			ftp.put(local, remote)
			self._logger('Uploading %s to %s' % (local, remote))
		
		for command in remote_commands:
			self.logger.debug('Executing on remote server: %s' % command)
			stdin, stdout, stderr = ssh.exec_command(command)
			if stdout:
				self._logger.debug(stdout)
			if stderr:
				self._logger.error(stderr)
				
		ssh.close()
		
	def system(self, args, shell=True):
		self._logger.debug("system: %s", args)
		p = subprocess.Popen(args, shell=shell, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		out, err = p.communicate()
		if out:
			self._logger.debug("stdout: " + out)
		if err:
			self._logger.warning("stderr: " + err)
		return out, err, p.returncode
		
def main():
	
	composer = Composer()
	executor = Executor()
	
	params = Parametres().get_params()
	run_list = composer.compose(params)
	executor.run(run_list)
	
if __name__ == "__main__":
	main()
