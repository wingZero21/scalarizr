'''
Created on Jun 23, 2010

@author: marat
@author: Dmytro Korsakov
'''
from scalarizr.bus import bus
from scalarizr.config import BuiltinBehaviours
from scalarizr.handlers import Handler, HandlerError
from scalarizr.messaging import Messages, Queues
import logging
import os
from scalarizr.util import configtool, fstool, system, initd, get_free_devname, filetool
from scalarizr.util import iptables
from scalarizr.util.iptables import IpTables, RuleSpec
from xml.dom.minidom import parse
from ConfigParser import NoOptionError
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
OPT_STORAGE_DEVICE_NAME	= "device_name"
TMP_EBS_MNTPOINT        = '/mnt/temp_storage'
CDB_TIMEOUT             = 60
CDB_MAX_ATTEMPTS        = 3
CRF_TIMEOUT				= 100
CRF_MAX_ATTEMPTS		= 3
initd_script = "/etc/init.d/cassandra"
if not os.path.exists(initd_script):
	raise HandlerError("Cannot find Cassandra init script at %s. Make sure that cassandra is installed" % initd_script)

pid_file = '/var/run/cassandra.pid'

logger = logging.getLogger(__name__)
logger.debug("Explore Cassandra service to initd module (initd_script: %s, pid_file: %s)", initd_script, pid_file)
initd.explore("cassandra", initd_script)

# TODO: rewrite initd to handle service's ip address


class CassandraMessages:
	
	CHANGE_RF					= 'Cassandra_ChangeReplFactor'
	'''
	@ivar rf_changes list( dict( name = 'Keyspace1', rf = 3)) 
	'''
	CHANGE_RF_RESULT			= 'Cassandra_ChangeReplFactorResult'
	'''
	@ivar status ok|error
	@ivar last_error
	@ivar bundles list(dict(remote_ip=ip, timestamp=123454, snapshot_id=snap-434244))
	'''
	
	INT_CHANGE_RF				= 'Cassandra_IntChangeReplFactor'
	'''
	@ivar leader_host
	'''
	
	INT_CHANGE_RF_RESULT		= 'Cassandra_IntChangeReplFactorResult'
	'''
	@ivar status ok|error
	@ivar last_error
	'''
	
	CREATE_DATA_BUNDLE			= "Cassandra_CreateDataBundle"
	
	CREATE_DATA_BUNDLE_RESULT 	= "Cassandra_CreateDataBundleResult"
	'''
	@ivar status ok|error
	@ivar last_error
	@ivar bundles list(dict(status = ok|error, last_error = , remote_ip=ip, timestamp=123454, snapshot_id=snap-434244))
	'''
	
	INT_CREATE_DATA_BUNDLE 		= "Cassandra_IntCreateDataBundle"
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
		self._logger = logging.getLogger(__name__)
		
		config = bus.config
		self.queryenv = bus.queryenv_service
		
		self.platform = bus.platform
		
		self.private_ip = self.platform.get_private_ip()
		self.zone = self.platform.get_avail_zone()
		self.inst_id = self.platform.get_instance_id()
		
		
		self.sect_name = BuiltinBehaviours.CASSANDRA
		self.sect = configtool.section_wrapper(config, self.sect_name)

		self.role_name = config.get(configtool.SECT_GENERAL, configtool.OPT_ROLE_NAME)

		self.storage_path = self.sect.get('storage_path')
		self.storage_conf = self.sect.get('storage_conf')

		self.data_file_directory = self.storage_path + "/datafile"
		self.commit_log_directory = self.storage_path + "/commitlog"

	def restart_service(self):
		self.stop_service()
		self.start_service()
				
	def stop_service(self):
		try:
			self._logger.info("Stopping Cassandra service")
			initd.stop("cassandra")
			self._logger.debug("Cassandra service stopped")
		except:
			self._logger.error("Cannot stop Cassandra")
			raise

	def start_service(self):
		try:
			self._logger.info("Starting Cassandra service")
			initd.start("cassandra")
			self._logger.debug("Cassandra service started")
		except:
			self._logger.error("Cannot start Cassandra")
			raise

	def create_snapshot(self, vol_id):
		ec2_conn = cassandra.platform.new_ec2_conn()
		desc = 'cassandra-backup_' + self.role_name + '_' + time.strftime('%Y%m%d%H%M%S')
		snapshot  = ec2_conn.create_snapshot(vol_id, )
		del(ec2_conn)
		return snapshot.id

	def get_node_queue(self):
		
		cassandra.start_service()
		queue = Queue()
		"""
		out, err = system('nodetool -h localhost ring')[0:2]
		if err:
			raise HandlerError('Cannot get node list: %s' % err)
		
		lines = out.split('\n')
		ip_re = re.compile('^(\d{1,3}(\.\d{1,3}){3})')
		for line in lines[1:]:
			if not line:
				continue
			
			result = re.search(ip_re , line)
			
			if result:
				self._logger.info('ADDING NODE %s TO QUEUE' % result.group(1))
				queue.put((result.group(1), 0))
		"""
		roles = cassandra.queryenv.list_roles(behaviour = "cassandra")
		
		# Fill seed list from queryenv answer
		for role in roles:
			for host in role.hosts:
				if host.internal_ip:
					queue.put((host.internal_ip, 0))
				else:
					queue.put((host.external_ip, 0))

		return queue


class CassandraScalingHandler(Handler):
	
	_port = None

	def __init__(self):
		
		self._logger = logging.getLogger(__name__)
		self._config = Configuration('xml')
		self._iptables = IpTables()
		try:
			self._config.read(cassandra.storage_conf)
		except:
			pass

		bus.on("init", self.on_init)
		bus.on("before_host_down", self.on_before_host_down)
		

	def on_init(self):
		bus.on("before_host_up", self.on_before_host_up)
		bus.on("host_init_response", self.on_host_init_response)
			
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BuiltinBehaviours.CASSANDRA in behaviour and \
				( message.name == Messages.HOST_INIT or
				message.name == Messages.HOST_UP or
				message.name == Messages.HOST_DOWN or
				message.name == Messages.BEFORE_HOST_TERMINATE )
	
	def on_HostInit(self, message):
		if message.behaviour == BuiltinBehaviours.CASSANDRA:
			if not message.local_ip:
				ip = message.remote_ip
			else:
				ip = message.local_ip
			self._insert_iptables_rule(ip)
#
#Begin copypasta
#
			
	def on_Cassandra_ChangeReplFactor(self, message):
		
		try:
			self._rf_changes = message.rf_changes
			self._crf_status = 'ok'
			self._crf_last_err = ''
			self._crf_ok_hosts = set()
			self._crf_timeouted_hosts = set()
			self._crf_results = []
			
			self._queue = cassandra.get_node_queue()
			
			if self._queue.empty():
				raise HandlerError('Cannot get nodelist: queue is empty')
	
			self._crf_host, self._crf_attempts = self._queue.get(False)
			
			"""
			Send first message
			"""
			self._send_crf_message(self._crf_host, CassandraMessages.INT_CREATE_DATA_BUNDLE, self._rf_changes)
		
		except (Exception, BaseException), e:
				self._send_message(CassandraMessages.CHANGE_RF_RESULT, dict(
												status = 'error',
												last_error = str(e) 		
								   ))

	def on_Cassandra_IntChangeReplFactor(self, message):
		
		leader_host = message.leader_host
		int_msg  = bus.int_messaging_service
		producer = int_msg.new_producer(leader_host)
		
		increase_ks_list = list()
		try:
			changes = message.rf_changes
			for keyspace in changes:
				
				keyspace_name = keyspace['name']
				new_rf		  = keyspace['rf']
				
				try:
					rf = self._config.get("Storage/Keyspaces/Keyspace[@Name='"+keyspace_name+"']/ReplicationFactor")
				except PathNotExistsError:
					raise HandlerError('Keyspace %s does not exist or configuration file is broken' % keyspace_name)
				
				if rf < new_rf:
					increase_ks_list.append(keyspace_name)
				self._config.set("Storage/Keyspaces/Keyspace[@Name='"+keyspace_name+"']/ReplicationFactor", new_rf)
				
			self._write_config()
			cassandra.restart_service()

			self._cleanup()
			for keyspace in increase_ks_list:
				self._repair(keyspace)

		except (Exception, BaseException), e:
			ret  = dict(status = 'error', last_error = str(e), remote_ip = cassandra.private_ip)
		
		finally:
			message  = int_msg.msg_service.new_message(CassandraMessages.INT_CHANGE_RF_RESULT, body = ret)
			producer.send(Queues.CONTROL, message)
		
	def on_Cassandra_IntChangeReplFactorResult(self, message):	
		try:		
			if not self._crf_results:
				self._crf_results = []
			
			if message.remote_ip == self._crf_host:
				self._crf_timer.cancel()				

				if 'error' == message.status:					
					if self._crf_attempts + 1 >= CRF_MAX_ATTEMPTS:
						result  = dict()
						result['status']		= 'error'
						result['remote_ip']   = message.remote_ip
						result['last_error']	= message.last_error
						self._crf_results.append(result)
						self._crf_status = 'error'
						self._crf_last_err = 'Host: %s, Error: %s' % (message.remote_ip, message.last_error)
					else:
						self._queue.put((message.remote_ip, self._crf_attempts + 1))
				else:
					result  = dict()
					result['status']		= 'ok'
					result['remote_ip']   = message.remote_ip
					self._crf_results.append(result)
					self._crf_ok_hosts.add(message.remote_ip)
											
					
				try:
					self._crf_attempts, self._crf_host = self._queue.get(False)					
					while self._crf_host in self._crf_ok_hosts:
						self._crf_host, self._crf_attempts = self._queue.get(False)
				except Empty:
					res = dict(status = self._crf_status, bundles = self._crf_results)
					if 'error' == self._crf_status:
						res.update({'last_error':self._crf_last_err})					
					self._send_message(CassandraMessages.CHANGE_RF_RESULT, res)
					return
				
				self._send_crf_message(self._crf_host, CassandraMessages.INT_CHANGE_RF, self._rf_changes)
				
	
			else:
				# Timeouted message from some node
				if not 'ok' == message.status:
					return
	
				result  = dict()
				result['status']		= 'ok' 
				result['remote_ip']   = message.remote_ip
				
				# Delete possible negative result if positive one arrived
				for result in self._crf_results:
					if message.remote_ip in result.values() and result['status'] == 'error':
						self._crf_results.remove(result)

				# Add positive result
				self._crf_results.append(result)
				self._crf_ok_hosts.add(message.remote_ip)

		except (Exception, BaseException), e:
			self._send_message(CassandraMessages.CHANGE_RF_RESULT, dict(
											status = 'error',
											last_error = str(e) 
							   ))

	def _send_crf_message(self, host = None, msg_name = None, body = None):
		int_msg  = bus.int_messaging_service
		message  = int_msg.msg_service.new_message(msg_name, body = body)
		producer = int_msg.new_producer(host)
		producer.send(Queues.CONTROL, message)
		self._cdb_timer = Timer(CRF_TIMEOUT, self._crf_failed)


	def _crf_failed(self):
		# Imitate  error message from current node
		int_msg  = bus.int_messaging_service
		err_msg  = dict()
		err_msg['status'] = 'error'
		err_msg['last_error'] = 'Timeout error occured while ChangeReplFactor. Host: %s, timeout: %d' % (self._crf_host, CRF_TIMEOUT)
		message  = int_msg.msg_service.new_message(CassandraMessages.INT_CHANGE_RF_RESULT, body = err_msg)
		self.on_Cassandra_IntChangeReplFactorResult(message)

#
#End copypasta
#
	def on_host_init_response(self, message):

		if not message.body.has_key("cassandra"):
			raise HandlerError("HostInitResponse message for Cassandra behaviour must have 'cassandra' property")
		self._logger.debug("Update cassandra config with %s", message.cassandra)
		self._update_config(message.cassandra)

	def on_before_host_up(self, message):

		cassandra.stop_service()
		# Getting storage conf from url
		self._config = Configuration('xml')
		storage_conf_url = cassandra.sect.get('storage_conf_url')
		result = re.search('^s3://(.*?)/(.*?)$', storage_conf_url)
		if result:
			bucket_name = result.group(1)
			file_name   = result.group(2)
			s3_conn = cassandra.platform.new_s3_conn()
			bucket = s3_conn.get_bucket(bucket_name)
			key = bucket.get_key(file_name)
			key.open_read()
			self._config.readfp(key)
			del(s3_conn)
		else:
			try:
				request = urllib2.Request(storage_conf_url)
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
		
		
		
		roles = cassandra.queryenv.list_roles(behaviour = "cassandra")
		ips = []
		# Fill seed list from queryenv answer and creating iptables rules
		for role in roles:
			for host in role.hosts:
				if host.internal_ip:
					ips.append(host.internal_ip)
				else:
					ips.append(host.external_ip)
		
		for ip in ips:
			self._config.add('Storage/Seeds/Seed', ip)
			self._insert_iptables_rule(ip)
		
		self._insert_iptables_rule(cassandra.private_ip)
		self._drop_iptable_rules()
		
		
		
		no_seeds = False
		if not self._config.get_list('Storage/Seeds/'):
			self._config.add('Storage/Seeds/Seed', cassandra.private_ip)
			no_seeds = True
					
		self._config.set('Storage/ListenAddress', cassandra.private_ip)
		self._config.set('Storage/ThriftAddress', '0.0.0.0')
		
		# Temporary config write
		self._write_config()

		# Determine startup type (server import, N-th startup, Scaling )
		try:
			cassandra.sect.get('snapshot_url')
			self._start_import_snapshot(message)
		except NoOptionError:
			try:
				cassandra.sect.get('snapshot_id')
				self._start_from_snap(message)
			except NoOptionError:
				if no_seeds:
					raise HandlerError('Cannot start bootstrap without seeds')
				self._start_bootstrap(message)
				
				
	def _start_import_snapshot(self, message):
		try:
			storage_size = cassandra.sect.get('storage_size')
			snapshot_url = cassandra.sect.get('snapshot_url')
			filename = os.path.basename(snapshot_url)
			snap_path = os.path.join(TMP_EBS_MNTPOINT, filename)
			result = re.search('^s3://(.*?)/(.*?)$', snapshot_url)
			# If s3 link
			if result:
				bucket_name = result.group(1)
				file_name   = result.group(2)
				s3_conn 	= cassandra.platform.new_s3_conn()
				bucket 		= s3_conn.get_bucket(bucket_name)
				key 		= bucket.get_key(file_name)
				if not key:
					raise HandlerError('File %s does not exist on bucket %s', (file_name, bucket_name))
				# Determine snapshot size in Gb			
				length = int(key.size//(1024*1024*1024) +1)
				
				if length > storage_size:
					raise HandlerError('Snapshot length (%s) is bigger then storage size (%s)' % (length, storage_size))
				
				temp_ebs_size = length*10 if length*10 < 1000 else 1000
				tmp_ebs_devname, temp_ebs_dev = self._create_attach_mount_volume(temp_ebs_size, auto_mount=False, mpoint=TMP_EBS_MNTPOINT)
				self._logger.debug('Starting download cassandra snapshot: %s', snapshot_url)			
				key.get_contents_to_filename(snap_path)
				
			# Just usual http or ftp link
			else:
				try:
					result   = urllib2.urlopen(snapshot_url)
				except urllib2.URLError:
					raise HandlerError('Cannot download snapshot. URL: %s' % snapshot_url)
				
				# Determine snapshot size in Gb
				try:
					length = int(int(result.info()['content-length'])//(1024*1024*1024) + 1)
				except:
					self._logger.error('Cannot determine snapshot size. URL: %s', snapshot_url)
					length = storage_size
					
				if length > storage_size:
					raise HandlerError('Snapshot length (%s) is bigger then storage size (%s)' % (length, storage_size))
				
				temp_ebs_size = length*10 if length*10 < 1000 else 1000			
				tmp_ebs_devname, temp_ebs_dev = self._create_attach_mount_volume(temp_ebs_size, auto_mount=False, mpoint=TMP_EBS_MNTPOINT)
				
				self._logger.debug('Starting download cassandra snapshot: %s', snapshot_url)
				
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
			
			ebs_devname, ebs_volume = self._create_attach_mount_volume(storage_size, auto_mount=True, mpoint=cassandra.storage_path)
			self._update_config({OPT_STORAGE_VOLUME_ID : ebs_volume.id, OPT_STORAGE_DEVICE_NAME : ebs_devname})
			self._create_valid_storage()
			
			self._logger.debug('Copying snapshot')
			rsync = filetool.Rsync().archive()
			rsync.source(TMP_EBS_MNTPOINT+os.sep).dest(cassandra.data_file_directory)
			out = system(str(rsync))
	
			if out[2]:
				raise HandlerError('Error while copying snapshot content from temp ebs to permanent: %s', out[1])
	
			self._logger.debug('Snapshot successfully copied from temporary ebs to permanent')
	
			self._config.set('Storage/AutoBootstrap', 'False')
	
			self._set_use_storage()
			snap_id = cassandra.create_snapshot(ebs_volume.id)
			cassandra.start_service()
			message.cassandra = dict(volume_id = ebs_volume.id, snapshot_id=snap_id)
		finally:
			try:
				self._umount_detach_delete_volume(tmp_ebs_devname, temp_ebs_dev)
				os.removedirs(TMP_EBS_MNTPOINT)
			except:
				pass


	def _start_from_snap(self, message):
		snap_id = cassandra.sect.get('snapshot_id')
		ebs_devname, ebs_volume = self._create_attach_mount_volume(auto_mount=True, snapshot=snap_id, mpoint=cassandra.storage_path, mkfs=False)
		self._update_config({OPT_STORAGE_VOLUME_ID : ebs_volume.id, OPT_STORAGE_DEVICE_NAME : ebs_devname})

		self._create_valid_storage()
		self._set_use_storage()

		cassandra.start_service()
		message.cassandra = dict(volume_id = ebs_volume.id)

	def _start_bootstrap(self, message):
		
		storage_size = cassandra.sect.get('storage_size')
		
		ebs_devname, ebs_volume = self._create_attach_mount_volume(storage_size, auto_mount=True, snapshot=None, mpoint=cassandra.storage_path)
		self._update_config({OPT_STORAGE_VOLUME_ID : ebs_volume.id, OPT_STORAGE_DEVICE_NAME : ebs_devname})
		self._create_valid_storage()

		self._config.set('Storage/AutoBootstrap', 'True')
		self._set_use_storage()
		
		cassandra.start_service()
		self._logger.debug('Sleep 120 seconds because of http://wiki.apache.org/cassandra/Operations#line-57')

		# The new node will log "Bootstrapping" when this is safe, 2 minutes after starting.
		# http://wiki.apache.org/cassandra/Operations#line-57
		time.sleep(120)
		self._logger.debug('Waiting for bootstrap finish')
		self._wait_until(self._bootstrap_finished, sleep = 10)
		message.cassandra = dict(volume_id = ebs_volume.id)

	def on_HostUp(self, message):
		if message.behaviour == BuiltinBehaviours.CASSANDRA:
			
			if not message.local_ip:
				ip = message.remote_ip
			else:
				ip = message.local_ip
				
			seeds = self._config.get_list('Storage/Seeds/')
			
			if not ip in seeds:
				self._config.add('Storage/Seeds/Seed', ip)
				self._write_config()
				cassandra.restart_service()
			
	def on_BeforeHostTerminate(self, *args):
		cassandra.start_service()
		out, err = system('nodetool -h localhost decommission')[0:2]
		if err:
			raise HandlerError('Cannot decommission node: %s' % err)
		self._wait_until(self._is_decommissioned)
		cassandra.stop_service()
		
	def on_before_host_down(self, *args):
		cassandra.stop_service()

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
		
		if message.behaviour == BuiltinBehaviours.CASSANDRA:
			
			if not message.local_ip:
				ip = message.remote_ip
			else:
				ip = message.local_ip
				
			try:
				self._config.remove('Storage/Seeds/Seed', ip)
				self._write_config()
				self._del_iptables_rule(ip)
				cassandra.restart_service()
			except:
				pass
			


	def _insert_iptables_rule(self, ip):
		storage_rule = RuleSpec(protocol=iptables.P_TCP, dport='7000', jump='ACCEPT', source = ip)
		thrift_rule  = RuleSpec(protocol=iptables.P_TCP, dport='9160', jump='ACCEPT', source = ip)
		self._iptables.insert_rule(None, storage_rule)
		self._iptables.insert_rule(None, thrift_rule)
		
			
	def _del_iptables_rule(self, ip):
		storage_rule = RuleSpec(protocol=iptables.P_TCP, dport='7000', jump='ACCEPT', source = ip)
		thrift_rule  = RuleSpec(protocol=iptables.P_TCP, dport='9160', jump='ACCEPT', source = ip)
		self._iptables.delete_rule(storage_rule)
		self._iptables.delete_rule(thrift_rule)
			
	def _drop_iptable_rules(self):
		storage_rule = RuleSpec(protocol=iptables.P_TCP, dport='7000', jump='DROP')
		thrift_rule  = RuleSpec(protocol=iptables.P_TCP, dport='9160', jump='DROP')
		self._iptables.append_rule(storage_rule)
		self._iptables.append_rule(thrift_rule)

	def _wait_until(self, target, args=None, sleep=5):
		args = args or ()
		while not target(*args):
			self._logger.debug("Wait %d seconds before the next attempt", sleep)
			time.sleep(sleep)
			
	def _create_attach_mount_volume(self, size=None, auto_mount=False, snapshot=None, mpoint=None, mkfs=True):
		
		if not size and not snapshot:
			raise HandlerError('Cannot create volume without size or snapshot')

		if not cassandra.zone:
			cassandra.zone     = cassandra.platform.get_avail_zone()
		if not cassandra.inst_id:
			cassandra.inst_id  = cassandra.platform.get_instance_id()
		
		ec2_conn = cassandra.platform.new_ec2_conn()
		self._logger.debug('Creating new EBS volume')
		ebs_volume = ec2_conn.create_volume(size, cassandra.zone, snapshot)
		
		self._logger.debug('Waiting for new ebs volume. ID=%s', (ebs_volume.id,))
		self._wait_until(lambda: ebs_volume.update() == "available")
		
		device   = get_free_devname()
		
		self._logger.debug('Attaching volume ID=%s', (ebs_volume.id,))		
		ebs_volume.attach(cassandra.inst_id, device)
		self._wait_until(lambda: ebs_volume.update() and ebs_volume.attachment_state() == "attached")
	
		if not os.path.exists(mpoint):
			os.makedirs(mpoint)
			
		fstool.mount(device, mpoint, make_fs=mkfs, auto_mount=auto_mount )
		
		del(ec2_conn)
		
		return device, ebs_volume
	
	def _umount_detach_delete_volume(self, devname, volume):

		fstool.umount(devname)
		
		volume.detach()
		self._wait_until(lambda: volume.update() and volume.volume_state() == "available")
		volume.delete()
		
	def _create_valid_storage(self):
		if not os.path.exists(cassandra.data_file_directory):
			os.makedirs(cassandra.data_file_directory)
		if not os.path.exists(cassandra.commit_log_directory):
			os.makedirs(cassandra.commit_log_directory)

	def _write_config(self):
		self._config.write(open(cassandra.storage_conf, 'w'))
		
	def _set_use_storage(self):

		self._config.set('Storage/CommitLogDirectory', cassandra.commit_log_directory)
		self._config.remove('Storage/DataFileDirectories/DataFileDirectory')
		self._config.add('Storage/DataFileDirectories/DataFileDirectory', cassandra.data_file_directory)

		self._write_config()
	
	def _is_decommissioned(self):
		try:
			cass = pexpect.spawn('nodetool -h localhost streams')
			out = cass.read()
			if re.search('Decommissioned', out):
				return True
			return False
		finally:
			del(cass)
			
	def _update_config(self, data): 
		cnf = bus.cnf
		cnf.update_ini(BuiltinBehaviours.CASSANDRA, {cassandra.sect_name: data})
		
	def _cleanup(self):
		out,err = system('nodetool -h localhost cleanup')[0:2]
		if err: 
			raise HandlerError('Cannot do cleanup: %s' % err)
		
	def _repair(self, keyspace):
		out,err = system('nodetool -h localhost repair %s' % keyspace)[0:2]
		if err: 
			raise HandlerError('Cannot do cleanup: %s' % err)
		
class CassandraDataBundleHandler(Handler):

	
	_cdb_ok_hosts			= None
	_cdb_timeouted_hosts	= None
	_cdb_results			= None
	_queue					= None

	def __init__(self):
		self._logger = logging.getLogger(__name__)

	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return BuiltinBehaviours.CASSANDRA in behaviour and \
				( message.name == CassandraMessages.CREATE_DATA_BUNDLE or
				message.name == CassandraMessages.INT_CREATE_DATA_BUNDLE or
				message.name == CassandraMessages.INT_CREATE_DATA_BUNDLE_RESULT)

	def on_Cassandra_CreateDataBundle(self, message):
		try:
			self._cdb_status = 'ok'
			self._cdb_last_err = ''
			self._cdb_ok_hosts = set()
			self._cdb_timeouted_hosts = set()
			self._cdb_results = []
			
			
			self._queue = cassandra.get_node_queue()
			
			if self._queue.empty():
				raise HandlerError('Cannot get nodelist: queue is empty')

			self._cdb_host, self._cdb_attempts = self._queue.get(False)

			"""
			Send first message
			"""
			body = {'leader_host' : cassandra.private_ip}
			
			self._send_cdb_message(self._cdb_host, CassandraMessages.INT_CREATE_DATA_BUNDLE, body)
			
		except (Exception, BaseException), e:
			self._send_message(CassandraMessages.CREATE_DATA_BUNDLE_RESULT, dict(
											status = 'error',
											last_error = str(e) 
							   ))

	def on_Cassandra_IntCreateDataBundle(self, message):
		
		leader_host = message.leader_host
		"""
		int_msg  = bus.int_messaging_service
		producer = int_msg.new_producer(leader_host)
		self._logger.info('###################### Message ######################')
		self._logger.info(message)
		"""
		umounted = False
		try:
			ret = dict()

			cassandra.stop_service()
			system('sync')			
						
			device_name        = cassandra.sect.get(OPT_STORAGE_DEVICE_NAME)
			fstool.umount(device_name)
			umounted = True
			
			volume_id          = cassandra.sect.get(OPT_STORAGE_VOLUME_ID)
			ret['remote_ip']   = cassandra.private_ip	
			ret['status']      = 'ok'
			ret['snapshot_id'] = cassandra.create_snapshot(volume_id)
			ret['timestamp']   = time.strftime('%Y-%m-%d_%H-%M')

		except (Exception, BaseException), e:
			ret.update(dict(status = 'error', last_error = str(e), remote_ip = cassandra.private_ip))
	
		finally:
			"""
			message  = int_msg.msg_service.new_message(CassandraMessages.INT_CREATE_DATA_BUNDLE_RESULT, body = ret)
			producer.send(Queues.CONTROL, message)
			"""
			if umounted:
				fstool.mount(device_name, cassandra.storage_path)
			cassandra.start_service()
			self._send_int_message(leader_host, CassandraMessages.INT_CREATE_DATA_BUNDLE_RESULT, ret, include_pad=True)		
	
	def on_Cassandra_IntCreateDataBundleResult(self, message):

		try:
			if not self._cdb_results:
				self._cdb_results = []
			
			if message.remote_ip == self._cdb_host:
				try:
					self._cdb_timer.cancel()
				except:
					pass

				if 'error' == message.status:
				
					if self._cdb_attempts + 1 >= CDB_MAX_ATTEMPTS:
						result  = dict()
						result['status']		= 'error'
						result['last_error']	= message.last_error
						result['remote_ip']     = message.remote_ip
						self._cdb_results.append(result)
						self._cdb_status = 'error'
						self._cdb_last_err = 'Host: %s, Error: %s' % (message.remote_ip, message.last_error)
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
					self._cdb_host, self._cdb_attempts = self._queue.get(False)
					while self._cdb_host in self._cdb_ok_hosts:
						self._cdb_host, self._cdb_attempts = self._queue.get(False)

				except Empty:
					res = dict(status = self._cdb_status, bundles = self._cdb_results)
					if 'error' == self._cdb_status:
						res.update({'last_error':self._cdb_last_err})
					self._send_message(CassandraMessages.CREATE_DATA_BUNDLE_RESULT, res)
					return
				
				body = {'leader_host' : cassandra.private_ip}
				self._send_cdb_message(self._cdb_host, CassandraMessages.INT_CREATE_DATA_BUNDLE, body)


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
		"""
		int_msg  = bus.int_messaging_service
		message  = int_msg.msg_service.new_message(msg_name, body = body, include_pad=True)
		producer = int_msg.new_producer(host)
		producer.send(Queues.CONTROL, message)
		"""
		try:
			self._send_int_message(host, msg_name, body, include_pad = True)
			self._cdb_timer = Timer(CDB_TIMEOUT, self._cdb_failed, [host])
			self._cdb_timer.start()
		except urllib2.URLError, e:
			message = self._get_failed_message(host)
			if self._queue.empty():
				timer = Timer(CDB_TIMEOUT, self._cdb_failed, [message])
				timer.start()
			else:
				self._cdb_failed(message)

		
	def _cdb_failed(self, message):
		# Imitate  error message from current node
		"""
		message  = int_msg.msg_service.new_message(CassandraMessages.INT_CREATE_DATA_BUNDLE_RESULT, body = err_msg)
		self.on_Cassandra_IntCreateDataBundleResult(message)
		"""
		self._send_int_message(cassandra.private_ip, message)

	def _get_failed_message(self, host):
		err_msg  = dict()
		err_msg['status'] = 'error'
		err_msg['remote_ip'] = host
		err_msg['last_error'] = 'Timeout error occured while CreateDataBundle. Host: %s, timeout: %d' % (self._cdb_host, CDB_TIMEOUT)
		message = self._new_message(CassandraMessages.INT_CREATE_DATA_BUNDLE_RESULT, err_msg, include_pad = True)

		return message
		
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