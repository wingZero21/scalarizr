from __future__ import with_statement

import sys
import os
import glob
import string
import logging
import threading

# from novaclient.v1_1 import client as nova_client
from cinderclient.v1 import client as cinder_client

from scalarizr import linux
from scalarizr import storage2
from scalarizr import util
from scalarizr.node import __node__
from scalarizr.storage2.volumes import base
from scalarizr.linux import coreutils



LOG = storage2.LOG


def name2device(name):
	if name.startswith('/dev/xvd'):
		return name
	if storage2.RHEL_DEVICE_ORDERING_BUG or os.path.exists('/dev/xvda1'):
		name = name.replace('/vd', '/xvd')
	if storage2.RHEL_DEVICE_ORDERING_BUG:
		name = name[0:8] + chr(ord(name[8])+4) + name[9:]
	return name


def device2name(device):
	if device.startswith('/dev/vd'):
		return device
	elif storage2.RHEL_DEVICE_ORDERING_BUG:
		device = device[0:8] + chr(ord(device[8])-4) + device[9:]
	return device.replace('/xvd', '/vd')


class FreeDeviceLetterMgr(object):

	def __init__(self):
		self._all = set(string.ascii_lowercase[2:16])
		self._acquired = set()
		self._lock = threading.Lock()
		self._local = threading.local()	

	
	def __enter__(self):
		letters = list(self._all - self._acquired)
		letters.sort()
		for l in letters:
			pattern = name2device('/dev/vd' + l) + '*'
			if not glob.glob(pattern):
				with self._lock:
					if not l in self._acquired:
						self._acquired.add(l)
						self._local.letter = l
						return self
		msg = 'No free letters for block device name remains'
		raise storage2.StorageError(msg)

	def get(self):
		return self._local.letter
	
	def __exit__(self, *args):
		if hasattr(self._local, 'letter'):
			self._acquired.remove(self._local.letter)
			del self._local.letter


# TODO: move this class to config or something
class OpenstackCredentials:
	USER = 'admin'
	PASSWORD = 'password'
	TENANT = 'demo'
	SERVER_ADDRESS = 'http://192.168.1.100'

	AUTH_URL = '%s:5000/v2.0' % SERVER_ADDRESS
	KEYSTONE_ENDPOINT = AUTH_URL
	GLANCE_ENDPOINT = '%s:9292' % SERVER_ADDRESS


class CinderFacade(object):

	def _cinder_connect(self):
		return cinder_client.Client(OpenstackCredentials.USER,
									OpenstackCredentials.PASSWORD,
									OpenstackCredentials.TENANT,
									OpenstackCredentials.AUTH_URL)

	def __init__(self):
		self.cinder_connection = None
		self.connect = self.reconnect
		self._method_to_service = {
			'create': 'volumes',
			'get': 'volumes',
			'list': 'volumes',
			'delete': 'volumes',
			'update': 'volumes',
			'attach': 'volumes',
			'detach': 'volumes',
			'reserve': 'volumes',
			'unreserve': 'volumes',
			'begin_detaching': 'volumes',
			'roll_detaching': 'volumes',
			'initialize_connection': 'volumes',
			'terminate_connection': 'volumes',
			'snapshot_create': 'volume_snapshots',
			'snapshot_get': 'volume_snapshots',
			'snapshot_list': 'volume_snapshots',
			'snapshot_delete': 'volume_snapshots'}
		self._method_to_cinder_method = {
			'create': 'create',
			'get': 'get',
			'list': 'list',
			'delete': 'delete',
			'update': 'update',
			'attach': 'attach',
			'detach': 'detach',
			'reserve': 'reserve',
			'unreserve': 'unreserve',
			'begin_detaching': 'begin_detaching',
			'roll_detaching': 'roll_detaching',
			'initialize_connection': 'initialize_connection',
			'terminate_connection': 'terminate_connection',
			'snapshot_create': 'create',
			'snapshot_get': 'get',
			'snapshot_list': 'list',
			'snapshot_delete': 'delete'}

	def __getattr__(self, name):
		service = getattr(self.cinder_connection, 
						self._method_to_service[name])
		return getattr(service, self._method_to_cinder_method[name])
		
	def reconnect(self):
		self.cinder_connection = self._cinder_connect()

	#TODO: make connection check more properly
	@property
	def has_connection(self):
		self.reconnect()
		return self.cinder_connection != None


class CinderVolume(base.Volume):

	_global_timeout = 3600
	_free_device_letter_mgr = FreeDeviceLetterMgr()

	def _check_cinder_connection(self):
		assert self._cinder.has_connection, \
				self.error_messages['no_connection']

	def __init__(self, 
				size=None,
				snapshot_id=None,
				avail_zone=None,
				tags=None,
				volume_type='standard',
				**kwds):
		base.Volume.__init__(self,
							 size=size and int(size) or None,
							 snapshot_id=snapshot_id,
							 avail_zone=avail_zone,
							 tags=tags,
							 volume_type=volume_type,
							 **kwds)
		self.error_messages.update({
			'no_id_or_conn': 'Volume has no ID or Cinder volume connection ' \
							'required for volume construction'})	
		self.error_messages.update({
			'no_connection': 'Cinder connection should be available ' \
							'to perform this operation'})
		self._cinder = CinderFacade()

	def _server_id(self):
		return __node__['openstack']['server_id']

	def _ensure(self):
		assert self._cinder.has_connection or self.id, \
				self.error_messages['no_id_or_conn']

		if self._cinder:
			volume = None
			name = None
			if self.id:
				volume = self._cinder.get(self.id)

				if volume.availability_zone != self.avail_zone:
					LOG.warn('Cinder volume %s is in the different '
						'availability zone (%s). Snapshoting it '
						'and create a new Cinder volume in %s', 
						volume.id, volume.availability_zone, self.avail_zone)
					self.snapshot_id = self._create_snapshot(self.id).id
					self.id = None
					volume = None
				else:
					self.size = volume.size

			#TODO: take tags from snapshot, if it exists
			if not self.id:
				volume = self._create_volume(size=self.size, 
											snapshot_id=self.snapshot_id,
											avail_zone=self.avail_zone,
											volume_type=self.volume_type)
				self.size = volume.size
				self.id = volume.id

			server_ids = map(lambda info: info['server_id'], 
							volume.attachments)
			if not (volume.status == 'in-use' and \
					self._server_id() in server_ids):
				self._wait_status_transition()
				if len(volume.attachments) > 0:
					self._detach_volume(volume.attachments[0]['server_id'])
				
				with self._free_device_letter_mgr:
					name = '/dev/vd%s' % self._free_device_letter_mgr.get()
					self._attach_volume(device_name=name)
			else:
				name = volume.attachments[0]['device']

			self._config.update({
				'id': volume.id,
				'avail_zone': volume.availability_zone,
				'name': name,
				'size': volume.size,
				'snapshot_id': volume.snapshot_id})

			if self.name:
				self.device = name2device(self.name)

	def _create_volume(self, 
					   size=None,
					   name=None,
					   snapshot_id=None,
					   display_description=None,
					   user_id=None,
					   project_id=None,
					   avail_zone=None,
					   imageRef=None,
					   tags=None,
					   volume_type='standard'):
		LOG.debug('Creating Cinder volume (zone: %s size: %s snapshot: %s ' \
					'volume_type: %s)', avail_zone, size,
					 snapshot_id, volume_type)
		volume = self._cinder.create(size=size, 
									display_name=name,
									snapshot_id=snapshot_id,
									display_description=display_description,
									user_id=user_id,
									project_id=project_id,
									availability_zone=avail_zone,
									imageRef=imageRef,
									metadata=tags,
									volume_type=volume_type)
		LOG.debug('Cinder volume %s created', volume.id)
		LOG.debug('Checking that Cinder volume %s is available', volume.id)
		self._wait_status_transition(volume.id)
		LOG.debug('Cinder volume %s is now available', volume.id)
		return volume

	def _create_snapshot(self, volume_id=None, description=None, nowait=False):
		volume_id = self.id

		LOG.debug('Creating snapshot of Cinder volume', volume_id)
		coreutils.sync()		
		snapshot = self._cinder.snapshot_create(volume_id, 
							force=True,
							display_description=description)
		LOG.debug('Snapshot %s created for Cinder volume %s', 
				snapshot.id, volume_id)
		if not nowait:
			self._wait_snapshot(snapshot)	
		return snapshot

	def _snapshot(self, description, tags, **kwds):
		snapshot = self._create_snapshot(self.id, description, 
										kwds.get('nowait', True))
		return storage2.snapshot(
				type='cinder', 
				id=snapshot.id, 
				description=snapshot.description,
				tags=tags)

	def _attach_volume(self, server_id=None, device_name='auto'):
		if server_id == None:
			server_id = self._server_id()
		volume_id = self.id
		self._check_cinder_connection()

		#volume attaching
		LOG.debug('Attaching Cinder volume %s (device: %s)', volume_id,
				 device_name)
		self._cinder.attach(volume_id, server_id, device_name)

		#waiting for attaching transitional state
		LOG.debug('Checking that Cinder volume %s is attached', volume_id)
		self._wait_status_transition(volume_id)
		LOG.debug('Cinder volume %s attached', volume_id)
		

		# Checking device availability in OS
		device = name2device(device_name)
		LOG.debug('Cinder device name %s is mapped to %s in operation system', 
				device_name, device)
		LOG.debug('Checking that device %s is available', device)
		# msg = 'Device %s is not available in operation system. ' \
		# 		'Timeout reached (%s seconds)' % (
		# 		device, self._global_timeout)
		# util.wait_until(
		# 	lambda: os.access(device, os.F_OK | os.R_OK), 
		# 	sleep=1, 
		# 	logger=LOG, 
		# 	timeout=self._global_timeout,
		# 	error_text=msg
		# )
		LOG.debug('Device %s is available', device)

	def _detach(self, force, **kwds):
		self._detach_volume()
	
	def _detach_volume(self):
		volume_id = self.id

		self._check_cinder_connection()
		volume = self._cinder.get(volume_id)

		LOG.debug('Detaching Cinder volume %s', volume_id)
		if volume.status != 'available':
			try:
				self._cinder.detach(volume_id)
			except:
				pass #TODO: handle possible exceptions

			LOG.debug('Checking that Cinder volume %s is available', volume_id)

			def exit_condition():
				vol = self._cinder.get(volume_id)
				return vol.status in ('available', 'in-use')

			msg = "Cinder volume %s is not in 'available' state. " \
					"Timeout reached (%s seconds)" % \
					(volume_id, self._global_timeout)

			util.wait_until(
				exit_condition, 
				logger=LOG, 
				timeout=self._global_timeout,
				error_text=msg)

			LOG.debug('Cinder volume %s is available', volume_id)	

		else:
			LOG.debug('Cinder volume %s is already available', volume_id)

	def _destroy(self, force, **kwds):
		self._check_cinder_connection()

		volume = self._cinder.get(self.id)
		# raise BaseException(volume.status)
		if len(volume.attachments) > 0:
			self._detach_volume(volume.attachments[0]['server_id'])
		# self._wait_status_transition()

		self._cinder.delete(self.id)
		self.id = None

	def _clone(self, config):
		config.pop('device', None)
		config.pop('avail_zone', None)

	def _wait_status_transition(self, volume_id=None):
		"""
		Wait until volume enters stable state (not 'detaching' or 'attaching')
		:param volume_id: 
		"""
		if not volume_id:
			volume_id = self.id

		status = self._cinder.get(volume_id).status
		vol = [None]
		def exit_condition():
			vol[0] = self._cinder.get(volume_id)
			return vol[0].status not in ('attaching', 'detaching', 'creating')

		if not exit_condition():
			msg = 'Cinder volume %s hangs in transitional state. ' \
				'Timeout reached (%s seconds)' % (volume_id,
												  self._global_timeout)
			util.wait_until(
				exit_condition,
				logger=LOG,
				timeout=self._global_timeout,
				error_text=msg)
			if vol[0].status == 'error':
				msg = 'Cinder volume %s enters error state after %s.' % \
					(volume_id, status)
				raise storage2.StorageError(msg)
	
	def _wait_snapshot(self, snapshot_id):
		LOG.debug('Checking that Cinder snapshot %s is completed', snapshot_id)

		msg = "Cinder snapshot %s wasn't completed. " \
				"Timeout reached (%s seconds)" % (
				snapshot_id, self._global_timeout)
		snap = [None]

		def exit_condition():
			snap[0] = self._cinder.snapshot_get(snapshot_id)
			return snap[0].status != 'creating'

		util.wait_until(
			exit_condition, 
			logger=LOG,
			timeout=self._global_timeout,
			error_text=msg
		)
		if snap[0].status == 'error':
			msg = 'Cinder snapshot %s creation failed.' \
					'AWS status is "error"' % snapshot_id
			raise storage2.StorageError(msg)

		elif snap[0].status == 'available': 
			LOG.debug('Snapshot %s completed', snapshot_id)	


class CinderSnapshot(base.Snapshot):

	_status_map = {
		'creating': base.Snapshot.IN_PROGRESS,
		'available': base.Snapshot.COMPLETED,
		'error': base.Snapshot.FAILED
	}

	def _check_cinder_connection(self):
		assert self._cinder.has_connection, \
				self.error_messages['no_connection']
	
	def __init__(self, **kwds):
		base.Snapshot.__init__(self, **kwds)
		self._cinder = CinderFacade()		

	def _status(self):
		self._check_cinder_connection()
		snapshot = self._cinder.snapshot_get(self.id)
		return self._status_map[snapshot.status]

	def _destroy(self):
		self._check_cinder_connection()
		self._cinder.snapshot_delete(self.id)


storage2.volume_types['cinder'] = CinderVolume
storage2.snapshot_types['cinder'] = CinderSnapshot
