'''
Created on Jan 6, 2010

@author: marat
@author: Dmytro Korsakov
@author: spike
'''

# Core components
from scalarizr.bus import bus
from scalarizr.config import Configurator, BuiltinBehaviours
from scalarizr.service import CnfController
from scalarizr.handlers import HandlerError, ServiceCtlHanler
from scalarizr.messaging import Messages

# Libs
from scalarizr.libs.metaconf import Configuration
from scalarizr.util import system, cached, firstmatched,\
	validators, software, initdv2
from scalarizr.util.filetool import read_file, write_file

# Stdlibs
import os, logging, shutil, re, time
from telnetlib import Telnet
from datetime import datetime


BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.WWW
CNF_NAME = BEHAVIOUR
CNF_SECTION = BEHAVIOUR

BIN_PATH = 'binary_path'
APP_PORT = 'app_port'
HTTPS_INC_PATH = 'https_include_path'
APP_INC_PATH = 'app_include_path'

class NginxInitScript(initdv2.ParametrizedInitScript):
	_nginx_binary = None
	
	def __init__(self):
		cnf = bus.cnf; ini = cnf.rawini
		self._nginx_binary = ini.get(CNF_SECTION, BIN_PATH)
		

		pid_file = None
		try:
			nginx = software.whereis('nginx')
			if nginx:
				out = system('%s -V' % nginx[0])[1]
				m = re.search("--pid-path=(.*?)\s", out)
				if m:
						pid_file = m.group(1)
		except:
			pass
						
		initdv2.ParametrizedInitScript.__init__(
			self, 
			'nginx', 
			'/etc/init.d/nginx',
			pid_file = pid_file, 
			socks=[initdv2.SockParam(80)]
		)

	def status(self):
		status = initdv2.ParametrizedInitScript.status(self)
		if not status and self.socks:
			ip, port = self.socks[0].conn_address
			telnet = Telnet(ip, port)
			telnet.write('HEAD / HTTP/1.0\n\n')
			if 'server: nginx' in telnet.read_all().lower():
				return initdv2.Status.RUNNING
			return initdv2.Status.UNKNOWN
		return status

	def configtest(self):
		out = system('%s -t' % self._nginx_binary)[1]
		if 'failed' in out.lower():
			raise initdv2.InitdError("Configuration isn't valid: %s" % out)
		
	def stop(self):
		if not self.running:
			return True
		ret =  initdv2.ParametrizedInitScript.stop(self)
		time.sleep(1)
		return ret
	
	def restart(self):
		ret = initdv2.ParametrizedInitScript.restart(self)
		time.sleep(1)
		return ret
	
		
initdv2.explore('nginx', NginxInitScript)


# Nginx behaviours configuration options
class NginxOptions(Configurator.Container):
	'''
	www behaviour
	'''
	cnf_name = CNF_NAME
	
	class binary_path(Configurator.Option):
		'''
		Path to nginx binary
		'''
		name = CNF_SECTION + '/binary_path'
		required = True
		
		@property
		@cached
		def default(self):
			return firstmatched(lambda p: os.access(p, os.F_OK | os.X_OK), 
					('/usr/sbin/nginx',	'/usr/local/nginx/sbin/nginx'), '')

		@validators.validate(validators.executable)
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
			
		value = property(Configurator.Option._get_value, _set_value)


	class app_port(Configurator.Option):
		'''
		App role port
		'''
		name = CNF_SECTION + '/app_port'
		default = '80'
		required = True
		
		@validators.validate(validators.portnumber())
		def _set_value(self, v):
			Configurator.Option._set_value(self, v)
		
		value = property(Configurator.Option._get_value, _set_value)
		

	class app_include_path(Configurator.Option):
		'''
		App upstreams configuration file path.
		'''
		name = CNF_SECTION + '/app_include_path'
		default = '/etc/nginx/app-servers.include'
		required = True
		
	class https_include_path(Configurator.Option):
		'''
		HTTPS configuration file path.
		'''
		name = CNF_SECTION + '/https_include_path'
		default = '/etc/nginx/https.include'
		required = True


def get_handlers():
	return [NginxHandler()]


class NginxCnfController(CnfController):
	def __init__(self):
		cnf = bus.cnf; ini = cnf.rawini
		nginx_conf_path = os.path.dirname(ini.get(CNF_SECTION, APP_INC_PATH)) + '/nginx.conf'
		CnfController.__init__(self, BEHAVIOUR, nginx_conf_path, 'nginx', {'1':'on','0':'off'})
		
	@property
	def _software_version(self):
		return software.software_info('nginx').version

		
class NginxHandler(ServiceCtlHanler):
	
	def __init__(self):
		ServiceCtlHanler.__init__(self, BEHAVIOUR, initdv2.lookup('nginx'), NginxCnfController())
				
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service	
		self._cnf = bus.cnf
		
		ini = self._cnf.rawini
		self._nginx_binary = ini.get(CNF_SECTION, BIN_PATH)		
		self._https_inc_path = ini.get(CNF_SECTION, HTTPS_INC_PATH)
		self._app_inc_path = ini.get(CNF_SECTION, APP_INC_PATH)		
		self._app_port = ini.get(CNF_SECTION, APP_PORT)
		
		bus.define_events("nginx_upstream_reload")
		bus.on("init", self.on_init)

		
	def on_init(self):
		bus.on('before_host_up', self.on_before_host_up)

	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BEHAVIOUR in behaviour and \
			(message.name == Messages.HOST_UP or \
			message.name == Messages.HOST_DOWN or \
			message.name == Messages.BEFORE_HOST_TERMINATE or \
			message.name == Messages.VHOST_RECONFIGURE or \
			message.name == Messages.UPDATE_SERVICE_CONFIGURATION)		
		
	def on_before_host_up(self, message):
		self._update_vhosts()		
		self._reload_upstream()
		bus.fire('service_configured', service_name=SERVICE_NAME)
	
	
	def on_HostUp(self, message):
		self._reload_upstream()
	
	
	def on_HostDown(self, message):
		self._reload_upstream()


	def on_BeforeHostTerminate(self, message):
		if not os.access(self._app_inc_path, os.F_OK):
			self._logger.debug('File %s not exists. Nothing to do', self._app_inc_path)
			return

		include = Configuration('nginx')	
		include.read(self._app_inc_path)
		
		server_ip = '%s:%s' % (message.local_ip or message.remote_ip, self._app_port)
		backends = include.get_list('upstream/server')
		if server_ip in backends:
			include.remove('upstream/server', server_ip)
			# Add 127.0.0.1 If it was the last backend
			if len(backends) == 1:
				include.add('upstream/server', '127.0.0.1:80')

		include.write(self._app_inc_path)
		self._restart_service()


	def on_VhostReconfigure(self, message):
		self._logger.info("Received virtual hosts update notification. Reloading virtual hosts configuration")
		self._update_vhosts()
		self._reload_upstream(True)


	def _reload_upstream(self, force_reload=False):

		config_dir = os.path.dirname(self._app_inc_path)
		nginx_conf_path = config_dir + '/nginx.conf'
		
		if not hasattr(self, '_config'):
			try:
				self._config = Configuration('nginx')
				self._config.read(nginx_conf_path)
			except (Exception, BaseException), e:
				raise HandlerError('Cannot read/parse nginx main configuration file: %s' % str(e))

		backend_include = Configuration('nginx')
		backend_include.read(os.path.join(bus.share_path, 'nginx/app-servers.tpl'))

		# Create upstream hosts configuration
		for app_serv in self._queryenv.list_roles(behaviour = BuiltinBehaviours.APP):
			for app_host in app_serv.hosts :
				server_str = '%s:%s' % (app_host.internal_ip, self._app_port)
				backend_include.add('upstream/server', server_str)
		if not backend_include.get_list('upstream/server'):
			self._logger.debug("Scalr returned empty app hosts list. Adding localhost only.")
			backend_include.add('upstream/server', '127.0.0.1:80')
		
		# Https configuration
		# openssl req -new -x509 -days 9999 -nodes -out cert.pem -keyout cert.key
		if os.access(self._https_inc_path, os.F_OK) \
				and os.access(self._cnf.key_path("https.crt"), os.F_OK) \
				and os.access(self._cnf.key_path("https.key"), os.F_OK):
			self._logger.debug('Add https include %s', self._https_inc_path)
			backend_include.add('include', self._https_inc_path + ';')
		
		old_include = None
		if os.path.isfile(self._app_inc_path):
			self._logger.debug("Reading old configuration from %s" % self._app_inc_path)
			old_include = Configuration('nginx')
			old_include.read(self._app_inc_path)
			
		if old_include \
				and not force_reload \
				and	backend_include.get_list('upstream/server') == old_include.get_list('upstream/server') :
			self._logger.debug("nginx upstream configuration unchanged")
		else:
			self._logger.debug("nginx upstream configuration was changed")
			
			if os.access(self._app_inc_path, os.F_OK):
				self._logger.debug('Backup file %s as %s', self._app_inc_path, self._app_inc_path + '.save')				
				shutil.move(self._app_inc_path, self._app_inc_path+".save")
				
			self._logger.debug('Write new %s', self._app_inc_path)
			backend_include.write(self._app_inc_path)
			
			#Patching main config file
			self._logger.debug('Update main configuration file')
			if 'http://backend' in self._config.get_list('http/server/location/proxy_pass') and \
					self._app_inc_path in self._config.get_list('http/include'):
				
				self._logger.debug('File %s already included into nginx main config %s', 
								self._app_inc_path, nginx_conf_path)
			else:
				self._config.comment('http/server')
				self._config.read(os.path.join(bus.share_path, "nginx/server.tpl"))
				self._config.add('http/include', self._app_inc_path)
				self._config.write(nginx_conf_path)
			
			self._logger.info("Testing new configuration.")
		
			try:
				self._init_script.configtest()
			except initdv2.InitdError, e:
				self._logger.error("Configuration error detected:" +  str(e) + " Reverting configuration.")
				if os.path.isfile(self._app_inc_path):
					shutil.move(self._app_inc_path, self._app_inc_path+".junk")
				else:
					self._logger.debug('%s does not exist', self._app_inc_path)
				if os.path.isfile(self._app_inc_path+".save"):
					shutil.move(self._app_inc_path+".save", self._app_inc_path)
				else:
					self._logger.debug('%s does not exist', self._app_inc_path+".save")
			else:
				self._reload_service()

		bus.fire("nginx_upstream_reload")	


	def _update_vhosts(self):
		self._logger.debug("Requesting virtual hosts list")
		received_vhosts = self._queryenv.list_virtual_hosts()
		self._logger.debug("Virtual hosts list obtained (num: %d)", len(received_vhosts))
		
		https_config = ''
		
		if received_vhosts:
			https_certificate = self._queryenv.get_https_certificate()
			
			cert_path = self._cnf.key_path("https.crt")
			pk_path = self._cnf.key_path("https.key")
			
			if https_certificate[0]:
				msg = 'Writing ssl cert' 
				cert = https_certificate[0]
				write_file(cert_path, cert, msg=msg, logger=self._logger)
			else:
				self._logger.error('Scalr returned empty SSL Cert')
				return
				
			if len(https_certificate)>1 and https_certificate[1]:
				msg = 'Writing ssl key'
				pk = https_certificate[1]
				write_file(pk_path, pk, msg=msg, logger=self._logger)
			else:
				self._logger.error('Scalr returned empty SSL Cert')
				return

			for vhost in received_vhosts:
				if vhost.hostname and vhost.type == 'nginx': #and vhost.https
					raw = vhost.raw.replace('/etc/aws/keys/ssl/https.crt',cert_path)
					raw = raw.replace('/etc/aws/keys/ssl/https.key',pk_path)
					https_config += raw + '\n'

		else:
			self._logger.debug('Scalr returned empty virtualhost list')

		if https_config:
			if os.path.exists(self._https_inc_path) \
					and read_file(self._https_inc_path, logger=self._logger):
				time_suffix = str(datetime.now()).replace(' ','.')
				shutil.move(self._https_inc_path, self._https_inc_path + time_suffix)

			msg = 'Writing virtualhosts to https.include'
			write_file(self._https_inc_path, https_config, msg=msg, logger=self._logger)
	
