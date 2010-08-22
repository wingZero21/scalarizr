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
SCRIPTS = 'scripts'
LOCAL_SCRIPTS = 'local_scripts'

TAR_FILE = 'scalarizr.tar.gz'
PACK = 'tar -czf %s ' % TAR_FILE
UNPACK = 'tar -xzf %s -C ' % TAR_FILE
REMOTE_LOG = '/var/log/scalarizr.log'


class Parametres():
	
	params = None
	
	def __init__(self):
		self.params = {}
	
	def get_params(self, interactive=True):
		if not len(self.params):
			execution_params = self._get_execution_params()
			self._add_default_params(execution_params)
			
			platform = execution_params['platform']
			behaviour = execution_params['behaviour']
			config_params = self._get_config_params(platform, behaviour)
			
			self.params.update(execution_params)
			self.params.update(config_params)
		
		if interactive:
			import_cmd = self._get_import_command()
			if import_cmd:
				self.params.update(import_cmd)
		
		return self.params
	
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
	
	def _add_default_params(self,params):
		if not params['init_script'] and os.path.isfile(os.path.realpath('scalarizr-'+params['platform'])):
			params['init_script'] = os.path.realpath('scalarizr-'+params['platform'])
			
		if not params['post_script'] and os.path.isfile(os.path.realpath('post.sh')):
			params['post_script'] = os.path.realpath('post.sh')
		
		if not params['source']: 
			local_path = os.path.dirname(__file__)
			if os.path.exists(os.path.join(local_path, 'scalarizr')):
				params['source'] = local_path
		
		if not params['etc']: 
			local_path = os.path.dirname(__file__)
			if os.path.exists(os.path.join(local_path, 'etc')):
				params['etc'] = local_path
	
	def _get_config_params(self, platform, behaviour):
		
		options = {}
			
		config = ConfigParser.ConfigParser()
		config_path = platform + '.ini'
		
		if not os.path.exists(config_path):
			print '%s not found. Exit.' % config_path
			sys.exit()
			
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
		
		if config.has_section(behaviour):
			if config.has_option(behaviour, 'apps'):
				options['apps'] += ' ' + config.get(behaviour, 'apps')
			options['install_cmd'] = options['install_cmd'].replace('_list_', options['apps'])
		else:
			print "Section %s not found in %s. Exit." % (behaviour, platform)
			sys.exit()
			
		if config.has_section(FILES):
			options[FILES] = config.items(FILES)
					
		if config.has_section(SCRIPTS):
			options[SCRIPTS] = config.items(SCRIPTS)
					
		if config.has_section(LOCAL_SCRIPTS):
			options[LOCAL_SCRIPTS] = config.items(LOCAL_SCRIPTS)
		
		return options
	
	def get_connection_data(self):
		params = self.get_params(False)
		return (params['key'], params['login'], params['ip'])
		
	def _get_import_command(self):
		import_cmd = raw_input('Copy input command here:')
		if import_cmd:
			return {'import_cmd':import_cmd}
		else: 
			print 'WARNING: Empty import command. You will need to execute it manually!'
			return ''
	
	
class Composer():
	
	commands = None
	
	def __init__(self):
		self.commands = []
		
	def compose(self, params):
		local = self._compose_local_commands(params)
		sftp = self._get_sftp_paths(params)
		remote = self._compose_remote_commands(params)
		fetch = self._get_fetch_paths(params)
		return (local, sftp, remote, fetch)
	
	def _compose_local_commands(self, params):
		commands = []
		# tar src
		commands.append(PACK + params['source'] + ' scalarizr')
		if params.has_key(LOCAL_SCRIPTS):
			for local_script in params[LOCAL_SCRIPTS]:
				commands.append(local_script[0] + ' ' + local_script[1])
		if params.has_key('etc') and params['etc']:
			commands.append('tar -czf etc-scalr.tar.gz ' + params['source'] + ' etc')
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
		commands.append(UNPACK + params['remote_src_path'])
		# mv init
		if params.has_key('init_script') and params['init_script']:
			init_remote_source = os.path.join(params['tempdir'], os.path.basename(params['init_script']))
			init_remote_dest = '/etc/init.d/scalarizr'
			commands.append('mv %s %s' % (init_remote_source, init_remote_dest))
			# chmod init
			commands.append('chmod +x %s' % init_remote_dest)
		# chmod post.sh
		if params.has_key('post_script') and params['post_script']:
			post_remote_path = os.path.join(params['tempdir'], os.path.basename(params['post_script']))
			commands.append('chmod +x %s' % post_remote_path)
			# execute post.sh
			commands.append(post_remote_path)
		#mv logging if logging
		if params['debug']=='1':
			commands.append('mv /etc/scalr/logging-debug.ini /etc/scalr/logging.ini')
		# execute import
		if params.has_key('import_cmd'):
			commands.append(params['import_cmd'])
		#FILES
		if params.has_key(FILES):
			for file in params[FILES]:
				source = os.path.join(params['tempdir'],file[0])
				dest = file[1]
				if source != dest:
					commands.append('mv %s %s' % (source, dest))
		#SCRIPTS
		if params.has_key(SCRIPTS):
			for file in params[SCRIPTS]:
				source = os.path.join(params['tempdir'],file[0])
				dest = file[1].split()[0]
				if source != dest:
					commands.append('mv %s %s' % (source, dest))
				arguments = ''
				for i in range(1,len(file[1].split())):
					arguments += file[1].split()[i] + ' '
				commands.append('%s %s' % (dest, arguments))		
		# FUTURE: unzip etc tar
		return commands
	
	def _get_sftp_paths(self, params):
		paths = {}
		#upload src tar
		paths[TAR_FILE] = os.path.join(params['tempdir'], TAR_FILE)
		#upload init
		if params.has_key('init_script') and params['init_script']:
			paths[params['init_script']] = os.path.join(params['tempdir'], os.path.basename(params['init_script']))
		#upload post.sh
		if params.has_key('post_script') and params['post_script']:
			paths[params['post_script']] = os.path.join(params['tempdir'], os.path.basename(params['post_script']))
		#FILES
		if params.has_key(FILES):
			for file in params[FILES]:
				paths[file[0]] = os.path.join(params['tempdir'], os.path.basename(file[1]))
		#SCRIPTS
		if params.has_key(SCRIPTS):
			for file in params[SCRIPTS]:
				paths[file[0]] = os.path.join(params['tempdir'], os.path.basename(file[1].split()[0]))		
		#FUTURE: upload etc tar
		return paths
	
	def _get_fetch_paths(self,params):
		paths = {}
		#fetch log
		if params['get_log']=='1':
			paths[REMOTE_LOG] = os.path.basename(REMOTE_LOG) + '-' + params['ip']
		return paths
		

class Executor():
	
	def __init__(self, connection):
		
		self.keyfile = connection[0]
		self.user = connection[1]
		self.ip = connection[2]
		
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
		fetch_commands = commands[3]
		
		self._logger.debug('Connecting to %s as %s with %s' 
				% (self.ip, self.user, self.keyfile))
		
		for command in local_commands:
			self._logger.debug(command)
	
		for command in sftp_commands:
			self._logger.debug('Uploading %s to %s' % (command, sftp_commands[command]))
		
		for command in remote_commands:
			self._logger.debug('Executing on remote server: %s' % command)
			
		for command in fetch_commands:
			self._logger.debug('Fetching %s to %s' % (command, fetch_commands[command]))
	
	def _run_with_paramiko(self, commands):	

		local_commands = commands[0]
		sftp_commands = commands[1]
		remote_commands = commands[2]
		fetch_commands = commands[3]
		
		for command in local_commands:
			self.system(command)
		
		ssh = paramiko.SSHClient()
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		ssh.connect(self.ip, username=self.user, key_filename=self.keyfile)
		
		ftp=ssh.open_sftp()  
	
		for command in sftp_commands:
			ftp.put(command, sftp_commands[command])
			self._logger.debug('Uploading %s to %s' % (command, sftp_commands[command]))
		
		for command in remote_commands:
			self.logger.debug('Executing on remote server: %s' % command)
			stdin, stdout, stderr = ssh.exec_command(command)
			if stdout:
				self._logger.debug(stdout)
			if stderr:
				self._logger.error(stderr)
				
		for command in fetch_commands:
			self._logger.debug('Fetching %s to %s' % (command, fetch_commands[command]))
			ftp.get(command, sftp_commands[command])
				
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
	P = Parametres()
	connection = P.get_connection_data()
	params = P.get_params()
	run_list = Composer().compose(params)
	Executor(connection).run(run_list)
	
	
if __name__ == "__main__":
	main()
