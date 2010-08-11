'''
Created on Jun 23, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.behaviour import Behaviours
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Messages, Queues
import logging
import os
from scalarizr.util import configtool, fstool, system, initd, get_free_devname, filetool
from xml.dom.minidom import parse
from Queue import Queue, Empty
from threading import Timer
from scalarizr.libs.metaconf import *
import urllib2
import tarfile
import shutil
import time
import pexpect


OPT_SNAPSHOT_ID			= "snapshot_id"
OPT_STORAGE_VOLUME_ID	= "volume_id" 
TMP_EBS_MNTPOINT        = '/mnt/temp_storage'
CDB_TIMEOUT             = 120
CDB_MAX_ATTEMPTS        = 3 

initd_script = "/etc/init.d/cassandra"
if not os.path.exists(initd_script):
	raise HandlerError("Cannot find Cassandra init script at %s. Make sure that cassandra is installed" % initd_script)

pid_file = '/var/run/cassandra.pid'

logger = logging.getLogger(__name__)
logger.debug("Explore Cassandra service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("cassandra", initd_script)

# TODO: rewrite initd to handle service's ip address

class CassandraMessages:
	CREATE_DATA_BUNDLE = "Cassandra_CreateDataBundle"
	
	CREATE_DATA_BUNDLE_RESULT = "Cassandra_CreateDataBundleResult"
	'''
	@ivar status ok|error
	@ivar last_error
	@ivar bundles list(dict(remote_ip=ip, timestamp=123454, snapshot_id=snap-434244))
	'''
	
	INT_CREATE_DATA_BUNDLE = "Cassandra_IntCreateDataBundle"
	'''
	@ivar leader_host
	'''
	
	INT_CREATE_DATA_BUNDLE_RESULT = "Cassandra_IntCreateDataBundleResult"
	'''
	@ivar status ok|error
	@ivar last_error
	@ivar timestamp
	@ivar snapshot_id
	@ivar remote_ip
	'''

class StorageError(BaseException): pass

cassandra = None
def get_handlers ():
	globals()['cassandra'] = Cassandra()
	return [CassandraScalingHandler(), CassandraDataBundleHandler()]

class Cassandra(object):
	def __init__(self):
		config = bus.config
		
		self._sect_name = configtool.get_behaviour_section_name(Behaviours.CASSANDRA)
		self._sect = configtool.section_wrapper(bus.config, self._sect_name)
		
		self._role_name = config.get(configtool.SECT_GENERAL, configtool.OPT_ROLE_NAME)
		
		self._storage_path = self._sect.get('storage_path')
		self._storage_conf = self._sect.get('storage_conf')
		
		self.data_file_directory = self._storage_path + "/datafile" 
		self.commit_log_directory = self._storage_path + "/commitlog"

		self._config = Configuration('xml')

		self._private_ip = self._platform.get_private_ip()
		self._zone = self._platform.get_avail_zone()
		self._inst_id = self._platform.get_instance_id()
	pass


class CassandraHandler(Handler):
	
	_logger = None
	_queryenv = None
	_storage = None
	_storage_path = None
	_storage_conf = None
	_port = None
	_platform = None
	_zone = None
	
	_cdb_ok_hosts = None
	_cdb_timeouted_hosts = None
	_cdb_results  = None
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._queryenv = bus.queryenv_service
		self._platform = bus.platform
		config = bus.config
		
		self._sect_name = configtool.get_behaviour_section_name(Behaviours.CASSANDRA)
		self._sect = configtool.section_wrapper(bus.config, self._sect_name)
		
		self._role_name = config.get(configtool.SECT_GENERAL, configtool.OPT_ROLE_NAME)
		
		self._storage_path = self._sect.get('storage_path')
		self._storage_conf = self._sect.get('storage_conf')
		
		self.data_file_directory = self._storage_path + "/datafile" 
		self.commit_log_directory = self._storage_path + "/commitlog"

		self._config = Configuration('xml')

		self._private_ip = self._platform.get_private_ip()
		self._zone = self._platform.get_avail_zone()
		self._inst_id = self._platform.get_instance_id()
		
		bus.on("init", self.on_init)
		bus.on("before_host_down", self.on_before_host_down)

	def on_init(self):
		bus.on("before_host_up", self.on_before_host_up)
		
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return Behaviours.CASSANDRA in behaviour and \
				(message.name == Messages.HOST_INIT or
				message.name == Messages.HOST_UP or
				message.name == Messages.HOST_DOWN or
				message.name == CassandraMessages.CREATE_DATA_BUNDLE or 
				message.name == CassandraMessages.INT_CREATE_DATA_BUNDLE or
				message.name == CassandraMessages.INT_CREATE_DATA_BUNDLE_RESULT)
	
	def on_HostInit(self, message):
		if message.behaviour == Behaviours.CASSANDRA:
			if not message.local_ip:
				ip = message.remote_ip
			else:
				ip = message.local_ip
			self._add_iptables_rule(ip)
			
	def on_Cassandra_ChangeReplFactor(self, message):
		try:
			new_rf = message.rf
			keyspace = message.keyspace
			try:
				rf = self._config.get("Storage/Keyspaces/Keyspace[@Name='"+keyspace+"']/ReplicationFactor")
			except PathNotExistsError:
				raise HandlerError('Keyspace %s does not exist or configuration file is broken' % keyspace)
			
			if rf == new_rf:
				# New and old rfs are equal
				# Just send "OK" message
				pass
			
			self._config.set("Storage/Keyspaces/Keyspace[@Name='"+keyspace+"']/ReplicationFactor", new_rf)
			self._write_config()
			self._restart_cassandra()
			
			if new_rf < rf:
				self._cleanup()
			elif new_rf > rf:
				# Brainstorm here
				pass

				
		except (Exception, BaseException), e:
			#TODO: Send sad message with error
			pass
		

	def on_Cassandra_CreateDataBundle(self, message):
		try:
			self._cdb_ok_hosts = set()
			self._cdb_timeouted_hosts = set()
			self._cdb_results = []
			self._queue = Queue()
	
			"""
			Get node list and fill the queue
			"""
			out, err = system('nodetool -h localhost ring')[0:2]
			if err:
				raise HandlerError('Cannot get node list: %s' % err)
			
			lines = out.split('\n')
			ip_re = re.compile()
			for line in lines[1:]:
				if not line:
					continue
				ip = line.split()[0]
	
				if re.match('^\d{1,3}(\.\d{1,3}){3}$', ip) and ip != self._private_ip:
					self._queue.put((line.split()[0], 0))
			
			if self._queue.is_empty():
				raise HandlerError('Cannot get nodelist: queue is empty')
			
			self._cdb_attempts, self._cdb_host = self._queue.get(False)
			
			"""
			Send first message
			"""
			self._send_cdb_message(self._cdb_host, CassandraMessages.INT_CREATE_DATA_BUNDLE)
			
		except (Exception, BaseException), e:
			self._send_message(CassandraMessages.CREATE_DATA_BUNDLE_RESULT, dict(
											status = 'error',
											last_error = str(e) 
							   ))

	def on_Cassandra_IntCreateDataBundle(self, message):
		
		leader_host = message.leader_host
		int_msg  = bus.int_messaging_service
		producer = int_msg.new_producer(leader_host)
		try:
			ret = dict()

	
			self._stop_cassandra()
			volume_id          = self._sect.get(OPT_STORAGE_VOLUME_ID)
			ret['remote_ip']   = self._private_ip	
			ret['status']      = 'ok'
			ret['snapshot_id'] = self._create_snapshot(volume_id)
			ret['timestamp']   = time.strftime('%Y-%m-%d_%H-%M') 
			
			self._start_cassandra()
			
		except (Exception, BaseException), e:
			ret  = dict(status = 'error', last_error = str(e), remote_ip = self._private_ip)
		
		finally:
			message  = int_msg.msg_service.new_message(CassandraMessages.INT_CREATE_DATA_BUNDLE_RESULT, body = ret)
			producer.send(Queues.CONTROL, message)

	
	def on_Cassandra_IntCreateDataBundleResult(self, message):
		
		try:		
			if not self._cdb_results:
				self._cdb_results = []
			
			if message.remote_ip == self._cdb_host:
				self._cdb_timer.cancel()				

				if 'error' == message.status:
					
					if self._cdb_attempts + 1 >= CDB_MAX_ATTEMPTS:
						result  = dict()
						result['status']		= 'error' 
						result['last_error']	= message.last_error
						result['remote_ip']   = message.remote_ip
						self._cdb_results.append(result)
					else:
						self._queue.put((message.remote_ip, self._cdb_attempts + 1))
				else:
					result  = dict()
					result['snapshot_id'] = message.snapshot_id
					result['timestamp']   = message.timestamp
					result['remote_ip']   = message.remote_ip
					self._cdb_results.append(result)
					self._cdb_ok_hosts.add(message.remote_ip)				
					
					
				try:
					self._cdb_attempts, self._cdb_host = self._queue.get(False)					
					while not self._cdb_host in self._cdb_ok_hosts:
						self._cdb_attempts, self._cdb_host = self._queue.get(False)
						
				except Empty:
					self._send_message(CassandraMessages.CREATE_DATA_BUNDLE_RESULT, dict(
												status = 'ok',
												bundles = self._cdb_results
											))
					return
				
				self._send_cdb_message(self._cdb_host, CassandraMessages.INT_CREATE_DATA_BUNDLE)
				
	
			else:
				# Timeouted message from some node
				if not 'ok' == message.status:
					return
	
				result  = dict()
				result['snapshot_id'] = message.snapshot_id
				result['timestamp']   = message.timestamp
				result['remote_ip']   = message.remote_ip
				
				# Delete possible negative result if positive one arrived
				for result in self._cdb_results:
					if message.remote_ip in result.values() and result['status'] == 'error':
						self._cdb_results.remove(result)

				# Add positive result
				self._cdb_results.append(result)
				self._cdb_ok_hosts.add(message.remote_ip)

		except (Exception, BaseException), e:
			self._send_message(CassandraMessages.CREATE_DATA_BUNDLE_RESULT, dict(
											status = 'error',
											last_error = str(e) 
							   ))



	def _send_cdb_message(self, host = None, msg_name = None, body = None):
		int_msg  = bus.int_messaging_service
		message  = int_msg.msg_service.new_message(msg_name, body = body)
		producer = int_msg.new_producer(host)
		producer.send(Queues.CONTROL, message)
		self._cdb_timer = Timer(CDB_TIMEOUT, self._cdb_failed)

		
	def _cdb_failed(self):
		# Imitate  error message from current node 
		int_msg  = bus.int_messaging_service
		err_msg  = dict()
		err_msg['status'] = 'error'
		err_msg['last_error'] = 'Timeout error occured while CreateDataBundle. Host: %s, timeout: %d' % (self._cdb_host, CDB_TIMEOUT)
		message  = int_msg.msg_service.new_message(CassandraMessages.INT_CREATE_DATA_BUNDLE_RESULT, body = err_msg)
		self.on_Cassandra_IntCreateDataBundleResult(message)
		
	def on_before_host_up(self, message):		

		self._stop_cassandra()
		# Getting storage conf from url
		result = re.search('^s3://(.*?)/(.*?)$', message.storage_conf_url)
		if result:
			bucket_name = result.group(1)
			file_name   = result.group(2)
			s3_conn = self._platform.new_s3_conn()
			bucket = s3_conn.get_bucket(bucket_name)
			key = bucket.get_key(file_name)
			key.open_read()
			self._config.readfp(key)
			del(s3_conn)
		else:
			try:
				request = urllib2.Request(message.storage_conf_url)
				result  = urllib2.urlopen(request)
				self._config.readfp(result)
			except urllib2.URLError, e:
				raise HandlerError('Cannot retrieve cassandra sConsistencyLeveltorage configuration: %s', str(e))
		
		try:
			self._port = self._config.get('Storage/StoragePort')
		except:
			self._logger.error('Cannot determine storage port from configuration file')
			self._port = 7000
			# Adding port to config
			self._config.add('Storage/StoragePort', self._port)
		
		# Clear Seed list
		self._config.remove('Storage/Seeds/Seed')
		
		roles = self._queryenv.list_roles(behaviour = "cassandra")
		
		# Fill seed list from queryenv answer
		for role in roles:
			for host in role.hosts:
				if host.internal_ip:
					self._config.add('Storage/Seeds/Seed', host.internal_ip)
				else:
					self._config.add('Storage/Seeds/Seed', host.external_ip)
		
		self._config.set('Storage/ListenAddress', self._private_ip)
		self._config.set('Storage/ThriftAddress', '0.0.0.0')
		
		# Temporary config write
		self._write_config()

		# Determine startup type (server import, N-th startup, Scaling )
		if   hasattr(message, 'snapshot_url'):
			self._start_import_snapshot(message)
		elif hasattr(message, 'snapshot_id'):
			self._start_from_snap(message)
		elif hasattr(message, 'auto_bootstrap') and message.auto_bootstrap:
			self._start_bootstrap(message)
		else:
			raise HandlerError('Message does not containt enough data to determine start type')

	def _start_import_snapshot(self, message):
		
		storage_size = message.storage_size
		filename = os.path.basename(message.snapshot_url)
		snap_path = os.path.join(TMP_EBS_MNTPOINT, filename)
		result = re.search('^s3://(.*?)/(.*?)$', message.snapshot_url)
		# If s3 link
		if result:
			bucket_name = result.group(1)
			file_name   = result.group(2)
			s3_conn = self._platform.new_s3_conn()
			bucket = s3_conn.get_bucket(bucket_name)
			key = bucket.get_key(file_name)
			if not key:
				raise HandlerError('File %s does not exist on bucket %s', (file_name, bucket_name))
			# Determine snapshot size in Gb			
			length = int(key.size//(1024*1024*1024) +1)
			
			temp_ebs_size = length*10 if length*10 < 1000 else 1000
			tmp_ebs_devname, temp_ebs_dev = self._create_attach_mount_volume(temp_ebs_size, auto_mount=False, mpoint=TMP_EBS_MNTPOINT)
			self._logger.debug('Starting download cassandra snapshot: %s', message.snapshot_url)			
			key.get_contents_to_filename(snap_path)
			
		# Just usual http or ftp link
		else:
			try:
				result   = urllib2.urlopen(message.snapshot_url)
			except urllib2.URLError:
				raise HandlerError('Cannot download snapshot. URL: %s' % message.snapshot_url)
			
			# Determine snapshot size in Gb
			try:
				length = int(int(result.info()['content-length'])//(1024*1024*1024) + 1)
			except:
				self._logger.error('Cannot determine snapshot size. URL: %s', message.snapshot_url)
				length = 10
			
			temp_ebs_size = length*10 if length*10 < 1000 else 1000			
			tmp_ebs_devname, temp_ebs_dev = self._create_attach_mount_volume(temp_ebs_size, auto_mount=False, mpoint=TMP_EBS_MNTPOINT)
			
			self._logger.debug('Starting download cassandra snapshot: %s', message.snapshot_url)
			
			try:
				fp = open(snap_path, 'wb')
			except (Exception, BaseException), e:
				raise HandlerError('Cannot open snapshot file %s for write: %s', (filename, str(e)))
			else:
				while True:
					data = result.read(4096)
					if not data:
						break
					fp.write(data)
			finally:		
				fp.close()

		self._logger.debug('Trying to extract cassandra snapshot to temporary EBS storage')
		snap = tarfile.open(snap_path)
		snap.extractall(TMP_EBS_MNTPOINT)
		snap.close()
		self._logger.debug('Snapshot successfully extracted')
		os.remove(snap_path)

		ebs_devname, ebs_volume = self._create_attach_mount_volume(storage_size, auto_mount=True, mpoint=self._storage_path)
		self._update_config({OPT_STORAGE_VOLUME_ID : ebs_volume.id})
		self._create_valid_storage()
		
		self._logger.debug('Copying snapshot')
		rsync = filetool.Rsync().archive()
		rsync.source(TMP_EBS_MNTPOINT+os.sep).dest(self.data_file_directory)
		out = system(str(rsync))
		
		if out[2]:
			raise HandlerError('Error while copying snapshot content from temp ebs to permanent: %s', out[1])

		self._logger.debug('Snapshot successfully copied from temporary ebs to permanent')

		self._umount_detach_delete_volume(tmp_ebs_devname, temp_ebs_dev)
		self._config.set('Storage/AutoBootstrap', 'False')

		self._set_use_storage()
		snap_id = self._create_snapshot(ebs_volume.id)
		self._start_cassandra()
		message.cassandra = dict(volume_id = ebs_volume.id, snapshot_id=snap_id)

	def _start_from_snap(self, message):

		ebs_volume = self._create_attach_mount_volume(auto_mount=True, snapshot=message.snap_id, mpoint=self._storage_path)[-1]
		self._update_config({OPT_STORAGE_VOLUME_ID : ebs_volume.id})

		self._create_valid_storage()
		self._set_use_storage()

		self._start_cassandra()
		message.cassandra = dict(volume_id = ebs_volume.id)

	def _start_bootstrap(self, message):
		
		storage_size = message.storage_size	
		
		ebs_volume = self._create_attach_mount_volume(storage_size, auto_mount=True, snapshot=None, mpoint=self._storage_path)[-1]
		self._update_config({OPT_STORAGE_VOLUME_ID : ebs_volume.id})
		self._create_valid_storage()

		self._config.set('Storage/AutoBootstrap', 'True')
		self._set_use_storage()
		
		self._start_cassandra()

		# The new node will log "Bootstrapping" when this is safe, 2 minutes after starting.
		# http://wiki.apache.org/cassandra/Operations#line-57
		time.sleep(120)

		self._wait_until(self._bootstrap_finished, sleep = 10)
		message.cassandra = dict(volume_id = ebs_volume.id)

	def on_HostUp(self, message):
		if message.behaviour == Behaviours.CASSANDRA:
			
			if not message.local_ip:
				ip = message.remote_ip
			else:
				ip = message.local_ip

			self._config.add('Storage/Seeds/Seed', ip)
			self._write_config()
			self._restart_cassandra()
			
	def on_before_host_down(self):
		try:
			system('nodetool -h localhost decommission')
			self._wait_until(self._is_decommissioned)
			self._logger.info("Stopping Cassandra")
			initd.stop("cassandra")
		except initd.InitdError, e:
			self._logger.error("Cannot stop Cassandra")
			if initd.is_running("cassandra"):
				raise
			
	def _bootstrap_finished(self):
		try:
			cass = pexpect.spawn('nodetool -h localhost streams')
			out = cass.read()
			if re.search('Mode: Normal', out):
				return True
			return False
		finally:
			del(cass)
		
		
	def on_HostDown(self, message):
		
		if message.behaviour == Behaviours.CASSANDRA:
			
			if not message.local_ip:
				ip = message.remote_ip
			else:
				ip = message.local_ip
				
			try:
				self._config.remove('Storage/Seeds/Seed', ip)
				self._write_config()
				self._del_iptables_rule(ip)
				self._restart_cassandra()
			except:
				pass

	def _add_iptables_rule(self, ip):
		rule = "/sbin/iptables -A INPUT -s %s -p tcp --destination-port %s -j ACCEPT" % (ip, self._port)
		self._logger.debug("Adding rule to iptables: %s", rule)
		returncode = system(rule)[2]
		if returncode :
			self._logger.error("Cannot add rule")
			
	def _del_iptables_rule(self, ip):
		rule = "/sbin/iptables -D INPUT -s %s -p tcp --destination-port %s -j ACCEPT" % (ip, self._port)
		self._logger.debug("Deleting rule from iptables: %s", rule)
		returncode = system(rule)[2]
		if returncode :
			self._logger.error("Cannot delete rule")
			
	def _drop_iptable_rules(self):
		drop_rule = "/sbin/iptables -A INPUT -p tcp --destination-port %s -j DROP" % (self._port,)
		self._logger.debug("Drop iptables rules on port %s: %s", self._port, drop_rule)
		returncode = system(drop_rule)[2]
		if returncode :
			self._logger.error("Cannot drop rules")
			
	def _restart_cassandra(self):
		try:
			self._logger.info("Restarting Cassandra service")
			initd.restart("cassandra")
			self._logger.debug("Cassandra service restarted")
		except:
			self._logger.error("Cannot restart Cassandra")
			raise
	
	def _create_snapshot(self, vol_id):
		ec2_conn = self._platform.new_ec2_conn()
		snapshot  = ec2_conn.create_snapshot(vol_id)
		del(ec2_conn)
		return snapshot.id
			
	def _stop_cassandra(self):
		try:
			self._logger.info("Stopping Cassandra service")
			initd.stop("cassandra")
			self._logger.debug("Cassandra service stopped")
		except:
			self._logger.error("Cannot stop Cassandra")
			raise

	def _start_cassandra(self):
		try:
			self._logger.info("Starting Cassandra service")
			initd.start("cassandra")
			self._logger.debug("Cassandra service started")
		except:
			self._logger.error("Cannot start Cassandra")
			raise
		
	def _wait_until(self, target, args=None, sleep=5):
		args = args or ()
		while not target(*args):
			self._logger.debug("Wait %d seconds before the next attempt", sleep)
			time.sleep(sleep)
			
	def _create_attach_mount_volume(self, size=None, auto_mount=False, snapshot=None, mpoint=None):
		
		if not size and not snapshot:
			raise HandlerError('Cannot create volume without size or snapshot')
		
		if not self._zone:
			self._zone     = self._platform.get_avail_zone()
		if not self._inst_id:
			self._inst_id  = self._platform.get_instance_id()
		
		ec2_conn = self._platform.new_ec2_conn()
		self._logger.debug('Creating new EBS volume')
		ebs_volume = ec2_conn.create_volume(size, self._zone, snapshot)
		
		self._logger.debug('Waiting for new ebs volume. ID=%s', (ebs_volume.id,))
		self._wait_until(lambda: ebs_volume.update() == "available")
		
		device   = get_free_devname()
		
		self._logger.debug('Attaching volume ID=%s', (ebs_volume.id,))		
		ebs_volume.attach(self._inst_id, device)
		self._wait_until(lambda: ebs_volume.update() and ebs_volume.attachment_state() == "attached")
	
		if not os.path.exists(mpoint):
			os.makedirs(mpoint)
			
		fstool.mount(device, mpoint, make_fs=True, auto_mount=auto_mount )
		
		del(ec2_conn)
		
		return device, ebs_volume
	
	def _umount_detach_delete_volume(self, devname, volume):

		fstool.umount(devname)
		
		volume.detach()
		self._wait_until(lambda: volume.update() and volume.volume_state() == "available")
		volume.delete()
		
	def _create_valid_storage(self):
		if not os.path.exists(self.data_file_directory):
			os.makedirs(self.data_file_directory)
		if not os.path.exists(self.commit_log_directory):
			os.makedirs(self.commit_log_directory)

	def _write_config(self):
		self._config.write(open(self._storage_conf, 'w'))
		
	def _set_use_storage(self):

		self._config.set('Storage/CommitLogDirectory', self.commit_log_directory)
		self._config.remove('Storage/DataFileDirectories/DataFileDirectory')
		self._config.add('Storage/DataFileDirectories/DataFileDirectory', self.data_file_directory)

		self._write_config()
	
	def _is_decommissioned(self):
		try:
			cass = pexpect.spawn('nodetool -h localhost info')
			out = cass.read()
			if re.search('Decommissioned', out):
				return True
			return False
		finally:
			del(cass)
			
	def _update_config(self, data): 
		updates = {self._sect_name: data}
		configtool.update(configtool.get_behaviour_filename(Behaviours.CASSANDRA, ret=configtool.RET_PRIVATE), updates)
		
	def _cleanup(self):
		out,err = system('nodetool -h localhost cleanup')[0:2]
		if err: 
			raise HandlerError('Cannot do cleanup: %s' % err)
		
		
class StorageProvider(object):
	
	_providers = None
	_instance = None
	
	def __new__(cls):
		if cls._instance is None:
			o = object.__new__(cls)
			o._providers = dict()
			cls._instance = o
		return cls._instance
	
	def new_storage(self, name, *args, **kwargs):
		if not name in self._providers:
			raise StorageError("Cannot create storage from undefined provider '%s'" % (name,))
		return self._providers[name](*args, **kwargs) 
	
	def register_storage(self, name, cls):
		if name in self._providers:
			raise StorageError("Storage provider '%s' already registered" % (name,))
		self._providers[name] = cls
		
	def unregister_storage(self, name):
		if not name in self._providers:
			raise StorageError("Storage provider '%s' is not registered" % (name,))
		del self._providers[name]
	
class Storage(object):
	def __init__(self):
		pass

	def init(self, mpoint, *args, **kwargs):
		pass

	def copy_data(self, src, *args, **kwargs):
		pass

class EbsStorage(Storage):
	pass

class EphemeralStorage(Storage):
	_platform = None
	def __init__(self):
		self._platform = bus.platform
		self._logger = logging.getLogger(__name__)

	def init(self, mpoint, *args, **kwargs):
		devname = '/dev/' + self._platform.get_block_device_mapping()["ephemeral0"]

		try:
			self._logger.debug("Trying to mount device %s and add it to fstab", devname)
			fstool.mount(device = devname, mpoint = mpoint, options = ["-t auto"], auto_mount = True)
		except fstool.FstoolError, e:
			if fstool.FstoolError.NO_FS == e.code:
				self._logger.debug("Trying to create file system on device %s, mount it and add to fstab", devname)
				fstool.mount(device = devname, mpoint = mpoint, options = ["-t auto"], make_fs = True, auto_mount = True)
			else:
				raise

	def copy_data(self, src, *args, **kwargs):
		pass

StorageProvider().register_storage("eph", EphemeralStorage)	