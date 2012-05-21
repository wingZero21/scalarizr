'''
Created on Nov 8, 2011

@author: dmitry
'''
import os
import logging
import re
import subprocess
import signal
import pymysql
import random

from scalarizr.config import BuiltinBehaviours
from scalarizr.services import  BaseService, ServiceError, BaseConfig
from scalarizr.util import system2, disttool, firstmatched, initdv2, wait_until, PopenError
from scalarizr.util.initdv2 import wait_sock, InitdError
from scalarizr.util.filetool import read_file, write_file, rchown


LOG = logging.getLogger(__name__)

MYSQL_DEFAULT_PORT=3306
MYSQL_PATH  = '/usr/bin/mysql' # old mysql_path
MYCNF_PATH 	= '/etc/mysql/my.cnf' if disttool.is_ubuntu() else '/etc/my.cnf' 
MYSQLD_PATH = '/usr/sbin/mysqld'  if disttool.is_ubuntu() else '/usr/libexec/mysqld' #old mysqld_path
MYSQLDUMP_PATH = '/usr/bin/mysqldump'
DEFAULT_DATADIR	= "/var/lib/mysql"

STORAGE_DATA_DIR = "mysql-data"
STORAGE_BINLOG = "mysql-misc/binlog"

BEHAVIOUR = SERVICE_NAME = BuiltinBehaviours.MYSQL


class MySQL(BaseService):
	
	service = None
	my_cnf = None
	_instance = None
	
		
	def __new__(cls, *args, **kwargs):
		if not cls._instance:
			cls._instance = super(MySQL, cls).__new__(
								cls, *args, **kwargs)
		return cls._instance
	
		
	def __init__(self):
		self._objects = {}
		self.service = initdv2.lookup(SERVICE_NAME)
	
	def init_master(self):
		pass
	
	def init_slave(self):
		pass
	
	def _init_service(self):
		pass
	
	def _init_replication(self, master=True):
		server_id = 1 if master else int(random.random() * 100000)+1
		self.my_cnf.server_id = server_id
		self.my_cnf.bind_address = None
		self.my_cnf.skip_networking = None

	@property
	def version(self):
		#5.1/5.5
		#percona/mysql
		pass
	
	@property
	def is_replication_master(self):
		pass
	
	def change_master_to(self):
		# client.change_master_to
		# check_replication_health and wait 
		pass
	
	def check_replication_health(self):
		# set slave status
		# on fail get status from error.log
		pass
	
	def move_mysqldir_to(self, storage_path):
		
		for directive, dirname in (
				('mysqld/log_bin', os.path.join(storage_path,STORAGE_BINLOG)), 
				('mysqld/datadir', os.path.join(storage_path,STORAGE_DATA_DIR))): 
			
			directory	= os.path.dirname(dirname)
			try:
				raw_value = self.my_cnf.get(directive)
				if not os.path.isdir(directory):
					os.makedirs(directory)
					src_dir = os.path.dirname(raw_value + "/") + "/"
					if os.path.isdir(src_dir):
						set_se_path = software.whereis('setsebool')
						if set_se_path:
							self._logger.debug('Make SELinux rule for rsync')
							system2((set_se_path[0], 'rsync_disable_trans', 'on'), raise_exc=False)
						self._logger.info('Copying mysql directory \'%s\' to \'%s\'', src_dir, directory)
						rsync = filetool.Rsync().archive()
						rsync.source(src_dir).dest(directory).exclude(['ib_logfile*'])
						system2(str(rsync), shell=True)
						self._mysql_config.set(directive, dirname)
					else:
						self._logger.info('Mysql directory \'%s\' doesn\'t exist. Creating new in \'%s\'', src_dir, directory)
				else:
					self._mysql_config.set(directive, dirname)
					
			except NoPathError:
				self._logger.debug('There is no such option "%s" in mysql config.' % directive)
				if not os.path.isdir(directory):
					os.makedirs(directory)
				
				self._mysql_config.add(directive, dirname)
	
			rchown("mysql", directory)
			# Adding rules to apparmor config 
			if disttool.is_debian_based():
				_add_apparmor_rules(directory)

	
	def flush_logs(self, data_dir):
		if not os.path.exists(data_dir):
			return
		
		info_files = ['relay-log.info', 'master.info']
		files = os.listdir(data_dir)
		
		for file in files:
			if file in info_files or file.find('relay-bin') != -1:
				os.remove(os.path.join(data_dir, file))
				
		
	def _get_my_cnf(self):
		return self._get('my_cnf', MySQLConf.find)
	
	
	def _set_my_cnf(self, obj):
		self._set('my_cnf', obj)	
				
		
	my_cnf = property(_get_my_cnf, _set_my_cnf)
	
	
class MySQLClient(object):
	_pool = None
	
	def __init__(self, user=None, passwd=None, db=None):
		self.db = None
		self.user = user
		self.passwd = passwd
			
	
	def test_connection(self):
		self._logger.debug('Checking MySQL service status')
		try:
			self.fetchone('SELECT 1')
		except pymysql.err.OperationalError, e:
			if 'Access denied for user' in str(e):
				return True
			elif "Can't connect to MySQL server on":
				return False
		except BaseException, e:
			LOG.debug('test_connection returned error: %s' % e)
		return True

	
	def list_databases(self):
		databases = [db[0] for db in self.fetchone('SHOW DATABASES') if db]
		if 'information_schema' in databases:
			databases.remove('information_schema')
		
	
	def start_slave(self):
		return self.fetchone('START SLAVE')
	
			
	def stop_slave(self, timeout):
		#TODO: think how to use timeouts
		'''
		timeout_reached = False
		if timeout_reached:
			raise ServiceError("Timeout (%d seconds) reached " + 
									"while waiting for slave stop" % (timeout,))
		'''
		return self.fetchone("STOP SLAVE")


	def stop_slave_io_thread(self, timeout):
		return self.fetchone("STOP SLAVE IO_THREAD")
	
	
	def lock_tables(self):
		return self.fetchone('FLUSH TABLES WITH READ LOCK')
	
		
	def unlock_tables(self):
		return self.fetchone('UNLOCK TABLES')
	
	
	def create_user(self, login, host, password, privileges=None):
		priv_count = self._priv_count()		
		if not privileges:
			cmd = "INSERT INTO mysql.user VALUES('%s','%s',PASSWORD('%s')" % (host, login, password) + ",'Y'"*priv_count + ",''"*4 +',0'*4+");" 
		else:
			cmd = "INSERT INTO mysql.user (Host, User, Password, %s) VALUES ('%s','%s',PASSWORD('%s'), %s);" \
					% (', '.join(privileges), host,login,password, ', '.join(["'Y'"]*len(privileges)))
		self.cli.fetchone(cmd)
		self.flush_privileges()
	
	
	def remove_user(self, login, host):
		return self.fetchone("DELETE FROM mysql.user WHERE User='%s' and Host='%s'" % (login, host))
	
	
	def user_exists(self, login, host):
		ret = self.fetchone("select Host,User from mysql.user where User='%s' and Host='%s'" % (login, host))
		return True if ret and ret['Host']==host and ret['User']==login else False
		
		
	def flush_privileges(self):
		return self.fetchone("FLUSH PRIVILEGES")
	
			
	def change_master_to(self, host, user, password, log_file, log_pos):
		self.fetchone('CHANGE MASTER TO MASTER_HOST="%(host)s", \
						MASTER_USER="%(user)s", \
						MASTER_PASSWORD="%(password)s", \
						MASTER_LOG_FILE="%(log_file)s", \
						MASTER_LOG_POS=%(log_pos)s, \
						MASTER_CONNECT_RETRY=15;' % vars())
	
	
	def slave_status(self):
		variables = {}
		out = self.fetchdict("SHOW SLAVE STATUS")
		if out:
			for name, value in out[0].keys():
				if name in ('Exec_Master_Log_Pos', 
							'Relay_Master_Log_File', 
							"Master_Log_File", 
							"Read_Master_Log_Pos", 
							'Slave_IO_Running', 
							'Slave_SQL_Running'):
					variables[name] = value
		else:
			raise ServiceError('SHOW SLAVE STATUS returned empty set. Slave is not started?')
		return variables
	
	
	def master_status(self):
		out = self.fetcdict('SHOW MASTER STATUS')
		log_file, log_pos = None, None
		if out:
			variables = out[0]
			log_file, log_pos = variables['File'], variables['Position']
		return (log_file, log_pos)
	
	
	def reset_master(self):
		return self.fetchone("RESET MASTER")
	
	
	def show_global_variables(self):
		d = {} 
		raw = self.fetchdict('SHOW GLOBAL VARIABLES', fetchone=False)
		for row in raw:
			name = row['Variable_name']
			val = row['Value']
			d[name] = val
		return d
		
	def check_password(self, user, password):
		hash_pairs = self.fetchall("SELECT PASSWORD('%s') AS hash, Password AS valid_hash FROM mysql.user WHERE mysql.user.User = '%s';" % 
				(password, user))
		
		for pair in hash_pairs:
			if pair[0] != pair[1]:
				return False
			
		return True
		
		
	def version(self):
		return self.fetchone('SELECT VERSION()')
	
		
	def get_connection(self, force=False):
		creds = (self.user, self.passwd, self.db)
		if force or not creds in self._pool:
			self._pool[creds] = pymysql.connect(host="127.0.0.1", user=self.user, passwd=self.passwd, db=self.db)
		return self._pool[creds]
	
	
	def _priv_count(self):
		res = self.fetch_dict("select * from mysql.user LIMIT 1;")
		return len([r for r in res.keys() if r.endswith('priv')])
	
		
	def _fetch(self, query, cursor = None, fetch_one=False):
		conn = self.get_connection()
		cursor = self.conn.cursor(cursor)
		try:
			cursor.execute(query)
		except pymysql.err.OperationalError, e:
			#catching mysqld restarts (e.g. sgt)
			if e.args[0] == 2013:
				conn = self.get_connection(true)
				cursor = self.conn.cursor(cursor)
				
		res = cursor.fetchone if fetch_one else cursor.fetchall()
		return res


	def fetch_dict(self, query, fetch_one=True):
		return self._fetch(query, pymysql.cursors.DictCursor, fetch_one)
	
	
	def fetchall(self, query):
		return self._fetch(query)
	
	
	def fetchone(self, query):
		return self._fetch(query, fetch_one=True)


class MySQLUser(object):
	
	login = None
	password = None
	host = None
	privileges = None
	
	def __init__(self, client, login, password=None, host=None, privileges=None):
		self.cli = client
		self.login = login
		self.password = password
		self.host = host
		self.privileges = privileges
	
	
	def create(self):
		if self.exists(self.login, self.host):
			raise ServiceError('Unable to create user %s@%s: already exists.')
		
		self.cli.create_user(self.login, self.host, self.password, self.privileges) 
		return self
	
	
	def check_password(self):
		if not self.exists():
			return False
		return self.cli.check_password(self.login, self.password)
	
	
	def exists(self):
		return self.cli.user_exists(self.login, self.host)
	
	
	def remove(self):
		return self.cli.remove_user(self.login, self.host)
		

	
class DataDir(object):
	
	def __init__(self):
		pass
	
	def move_to(self, dst):
		pass
	

class MySQLConf(BaseConfig):
	
	config_type = 'mysql'
	config_name = 'my.cnf'
	comment_empty = True
	
	
	@classmethod
	def find(cls):
		return cls(MYCNF_PATH)
		
	
	def _get_datadir(self):
		return self.get('mysqld/datadir')
	
	
	def _set_datadir(self, path):
		self.set('mysqld/datadir', path)	


	def _get_log_bin(self):
		return self.get('mysqld/log-bin')
	
	
	def set_log_bin(self, path):
		self.set('mysqld/log-bin', path)	


	def _get_server_id(self):
		return self.get('mysqld/server-id')
	
	
	def set_server_id(self, id):
		self.set('mysqld/server-id', id)	


	def _get_bind_address(self):
		return self.get('mysqld/bind-address')
	
	
	def set_bind_address(self, addr):
		self.set('mysqld/bind-address', addr)	


	def _get_skip_networking(self):
		return self.get('mysqld/skip-networking')
	
	
	def set_skip_networking(self, val):
		self.set('mysqld/skip-networking', val)	


	log_bin = property(_get_log_bin, set_log_bin)
	server_id = property(_get_server_id, set_server_id)
	bind_adress = property(_get_bind_address, set_bind_address)
	skip_networking = property(_get_skip_networking, set_skip_networking)
	datadir	 = property(_get_datadir, _set_datadir)
	datadir_default = DEFAULT_DATADIR
	
	
class MySQLDump(object):
	
	host = None
	port = None
	
	def __init__(self, root_user=None, root_password=None):
		self.root_user = root_user or 'root'
		self.root_password = root_password or ''
	
	def create(self, dbname, filename, opts=None):
		#TODO: move opts to handler
		#opts = config.split(bus.cnf.rawini.get('mysql', 'mysqldump_options'), ' ')
		opts = opts or []
		LOG.debug('Dumping database %s to %s' % (dbname, filename))
		opts = [MYSQLDUMP_PATH, '-u', self.root_user, '-p'] + opts + ['--databases']
		with open(filename, 'w') as fp: 
			system2(opts + [dbname], stdin=self.root_password, stdout=fp)


class MysqlInitScript(initdv2.ParametrizedInitScript):
	
	socket_file = None
	cli = None
	sgt_pid = None
	sgt_pid_path = None
	
	
	def __init__(self):
		#todo: provide user and password
		self.mysql_cli = MySQLClient()
		

		if disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
			initd_script = ('/usr/sbin/service', 'mysql')
		else:
			initd_script = firstmatched(os.path.exists, ('/etc/init.d/mysqld', '/etc/init.d/mysql'))
			
		pid_file = None
		try:
			out = system2("my_print_defaults mysqld", shell=True)
			m = re.search("--pid[-_]file=(.*)", out[0], re.MULTILINE)
			if m:
				pid_file = m.group(1)
			m = re.search("--socket=(.*)", out[0], re.MULTILINE)
			if m:
				self.socket_file = m.group(1)
		except:
			pass
		
		initdv2.ParametrizedInitScript.__init__(self, SERVICE_NAME, 
				initd_script, pid_file, socks=[initdv2.SockParam(MYSQL_DEFAULT_PORT, timeout=3600)])

	
	def _start_stop_reload(self, action):
		''' XXX: Temporary ugly hack (Ubuntu 1004 upstart problem - Job is already running)'''
		try:
			args = [self.initd_script] \
					if isinstance(self.initd_script, basestring) \
					else list(self.initd_script)
			args.append(action) 
			out, err, returncode = system2(args, close_fds=True, preexec_fn=os.setsid)
		except PopenError, e:
			if 'Job is already running' not in str(e):
				raise InitdError("Popen failed with error %s" % (e,))
		
		if action == 'start' and disttool.is_ubuntu() and disttool.version_info() >= (10, 4):
			try:
				LOG.debug('waiting for mysql process')
				wait_until(lambda: MYSQLD_PATH in system2(('ps', '-G', 'mysql', '-o', 'command', '--no-headers'))[0]
							, timeout=10, sleep=1)
			except:
				self._start_stop_reload('restart')
				return True
		
		if self.socks and (action != "stop" and not (action == 'reload' and not self.running)):
			for sock in self.socks:
				wait_sock(sock)
					
		return True
	
			
	def status(self):
		if self.socket_file:
			if os.path.exists(self.socket_file):
				return initdv2.Status.RUNNING if self.mysql_cli.test_connection() else initdv2.Status.NOT_RUNNING
			else:
				return initdv2.Status.NOT_RUNNING
		return initdv2.ParametrizedInitScript.status(self)

	
	def start(self):
		mysql_cnf_err_re = re.compile('Unknown option|ERROR')
		stderr = system2('%s --user=mysql --help' % MYSQLD_PATH, shell=True)[1]
		if re.search(mysql_cnf_err_re, stderr):
			raise Exception('Error in mysql configuration detected. Output:\n%s' % stderr)
		
		if not self.running:
			try:
				LOG.info("Starting %s" % self.behaviour)
				self._init_script.start()
				LOG.debug("%s started" % self.behaviour)
			except:
				if not self.running:
					raise
		
		return self._start_stop_reload('start')

	def _is_sgt_process_exists(self):
		out = system2(('ps', '-G', 'mysql', '-o', 'command', '--no-headers'))[0]
		return MYSQLD_PATH in out and 'skip-grant-tables' in out


	def start_skip_grant_tables(self):
		pid_dir = '/var/run/mysqld/'
		if not os.path.isdir(pid_dir):
			os.makedirs(pid_dir, mode=0755)
			mysql_user	= pwd.getpwnam("mysql")
			os.chown(pid_dir, mysql_user.pw_uid, -1)
		if self._is_sgt_process_exists():	
			args = [MYSQLD_PATH, '--user=mysql', '--skip-grant-tables']
			p = subprocess.Popen(args, stdin=subprocess.PIPE,stdout=subprocess.PIPE, stderr=subprocess.PIPE, preexec_fn=os.setsid)
		
		wait_until(lambda: self._is_sgt_process_exists(), timeout=10, sleep=1)
		wait_until(lambda: self.running, timeout=10, sleep=1)

	
	def stop_skip_grant_tables(self):
		if self._is_sgt_process_exists():
			os.kill(self.sgt_pid, signal.SIGTERM)
			wait_until(lambda: not self._is_sgt_process_exists(), timeout=10, sleep=1)
			wait_until(lambda: not self.running, timeout=10, sleep=1)
		


def _add_apparmor_rules(directory):
	if not os.path.exists('/etc/init.d/apparmor'):
		return
	try:
		file = open('/etc/apparmor.d/usr.sbin.mysqld', 'r')
	except IOError, e:
		pass
	else:
		app_rules = file.read()
		file.close()
		if not re.search (directory, app_rules):
			file = open('/etc/apparmor.d/usr.sbin.mysqld', 'w')
			if os.path.isdir(directory):
				app_rules = re.sub(re.compile('(.*)(})([^}]*)', re.S), '\\1\n'+directory+'/ r,\n'+'\\2\\3', app_rules)
				app_rules = re.sub(re.compile('(.*)(})([^}]*)', re.S), '\\1'+directory+'/** rwk,\n'+'\\2\\3', app_rules)
			else:
				app_rules = re.sub(re.compile('(.*)(})([^}]*)', re.S), '\\1\n'+directory+' r,\n'+'\\2\\3', app_rules)
			file.write(app_rules)
			file.close()
			apparmor_initd = ParametrizedInitScript('apparmor', '/etc/init.d/apparmor')
			try:
				apparmor_initd.reload()
			except InitdError, e:
				_logger.error('Cannot restart apparmor. %s', e)	

		
initdv2.explore(SERVICE_NAME, MysqlInitScript)

