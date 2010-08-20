'''
Created on Aug 19, 2010

@author: shaitanich
'''
# sudo apt-get install python-paramiko 
import paramiko
from optparse import OptionParser
import logging
import logging.handlers
import ConfigParser
import os
import sys

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
		#pack_cmd = PACK + params['source'] + ' scalarizr'
	
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

		return options
	
	def _get_import_command(self):
		return {'input_cmd':raw_input('Copy input command here:')}
	
class Composer():
	
	commands = None
	
	def __init__(self):
		self.commands = []
		
	def compose(self, params):
		local = self._compose_local_commands()
		sftp = self._compose_sftp_commands()
		remote = self._compose_remote_commands()
		return (local, sftp, remote)
	
	def _compose_local_commands(self):
		pass
	
	def _compose_remote_commands(self):
		pass
	
	def _compose_sftp_commands(self):
		pass

	
class Executor():
	
	def __init__(self, keyfile, user, ip):
		self.keyfile = keyfile
		self.user = user
		self.ip = ip
		
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
		self.run_with_paramiko(commands)
	
	def _run_with_paramiko(self, commands):	

		local_commands = commands[0]
		sftp_commands = commands[1]
		remote_commands = commands[2]
		
		ssh = paramiko.SSHClient()
		ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		
		for command in remote_commands:
			stdin, stdout, stderr = ssh.exec_command(command)
			if stdin:
				self._logger.info(stdin)
			if stdout:
				self._logger.debug(stdout)
			if stderr:
				self._logger.error(stderr)
		
def main():
	
	composer = Composer()
	executor = Executor()
	
	params = Parametres().get_params()
	run_list = composer.compose(params)
	executor.run(run_list)
	
	
	
	
	parser = OptionParser(usage="Usage: %prog key IP platform \
		[source behaviour init_script, post_script debug_log import_string")
	
	parser.add_option("-k", "--key", dest="key", help="private key")
	parser.add_option("-i", "--ip", dest="ip", help="instance ip")
	parser.add_option("-p", "--platform", dest="platform", help="[ubuntu8, ubuntu10, centos]")
	
	parser.add_option("-s", "--source", dest="source", help="path to scalarizr src dir")	
	parser.add_option("-b", "--behaviour", dest="behaviour", default='base', help="[base, app, www, mysql, cassandra, memcached]")
	parser.add_option("--init_script", dest="init_script", help=" path to init file")
	parser.add_option("--post_script", dest="post_script", help="path to post.sh")
	parser.add_option("-d", "--debug", dest="debug", default=1, help="set debug level")
	
	(options, args) = parser.parse_args()





	console = logging.StreamHandler()
	console.setLevel(logging.DEBUG)
	
	LOG_FILENAME = options.ip + '.log'
	handler = logging.FileHandler(LOG_FILENAME)
	
	logger = logging.getLogger('MyLogger')
	logger.setLevel(logging.DEBUG)
	logger.addHandler(console)
	logger.addHandler(handler)
	
	init = options.init_script
	if not init and os.path.isfile(os.path.realpath('scalarizr')):
		init = os.path.realpath('scalarizr')
		
	post = options.post_script
	if not post and os.path.isfile(os.path.realpath('post.sh')):
		post = os.path.realpath('post.sh')
	
	local_source_path = options.source
	if not options.source:
		local_source_path = os.path.dirname(__file__)
	pack_cmd = PACK + local_source_path + ' scalarizr'
	logger.debug(pack_cmd)
	
	if not options.key or not options.ip or not options.platform:
		print parser.format_help()
		sys.exit()



	config = ConfigParser.ConfigParser()
	config_path = options.platform + '.ini'
	config.read(config_path)
	
	try:
		login = config.get(SETTINGS, 'login')
		tempdir = config.get(SETTINGS, 'tempdir')
		remote_src_path = config.get(SETTINGS, 'src_path')
		sudo_cmd = config.get(SETTINGS, 'sudo_cmd')
		repo_cmd = config.get(SETTINGS, 'repo_cmd')
		install_cmd = config.get(SETTINGS, 'install_cmd')
		
		for data in (login , tempdir , remote_src_path, sudo_cmd, repo_cmd, install_cmd):
			if not data:
				print 'Not enough parameters in %s behaviour settings file. Exit.'% options.platform	
				sys.exit()		
	
	except ConfigParser.NoSectionError, e:
		logger.error(e)
		sys.exit

	if config.has_section(ALL):
		apps =  config.get(ALL, 'apps')
	
	if config.has_section(options.behaviour):
		apps += ' ' + config.get(options.behaviour, 'apps')
	
	#TODO: other_files [decide how to store them in ini file] 
		
	install_cmd = install_cmd.replace('_list_', apps)
	
	logger.debug('Trying to connect to %s as %s with key %s' % (options.ip, login, options.key))
	logger.debug('Going sudo: %s ' % sudo_cmd)
	
	logger.debug(repo_cmd)
	
	logger.debug('Installing apps: %s' % install_cmd)
	
	files = [TAR_FILE, init, post]
	#add other files to list
	for file in files:
		if file and os.path.exists(file):
			logger.debug('Uploading %s to temp dir: %s' % (file, tempdir))

	unpack_cmd = UNPACK + remote_src_path
	logger.debug(unpack_cmd)
	
	if options.debug == '1':
		move_cmd = 'mv /etc/scalr/logging-debug.ini /etc/scalr/logging.ini'	
	
	init_remote_source = os.path.join(tempdir, os.path.basename(init))
	init_remote_dest = '/etc/init.d/scalarizr'
	move_init_cmd = 'mv %s %s' % (init_remote_source, init_remote_dest)
	chmod_init_cmd = 'chmod +x %s' % init_remote_dest
	logger.debug(move_init_cmd)
	logger.debug(chmod_init_cmd)
	
	remote_post_sh = os.path.join(tempdir, os.path.basename(post))
	logger.debug('Executing %s' % remote_post_sh)
	
	#TODO: ask for import command & execute it
	import_cmd = raw_input('Copy input command here:')
	logger.debug(import_cmd)
	
if __name__ == "__main__":
	main()
