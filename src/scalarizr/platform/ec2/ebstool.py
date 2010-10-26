'''
Created on Aug 25, 2010

@author: marat
'''

from scalarizr.util import wait_until
from scalarizr.platform import PlatformError

import logging, os, time

from boto.ec2.volume import Volume
from boto.exception import BotoServerError
from boto.ec2.snapshot import Snapshot

DEFAULT_TIMEOUT = 300 	# 5 min
SNAPSHOT_TIMEOUT = 2700	# 45 min

def wait_snapshot(ec2_conn, snap_id, logger=None, timeout=SNAPSHOT_TIMEOUT):
	'''
	Waits until snapshot becomes 'completed' or 'error'
	'''
	time_until = time.time() + timeout	
	logger = logger or logging.getLogger(__name__)
	
	if isinstance(snap_id, basestring):
		snap = Snapshot(ec2_conn)
		snap.id = snap_id
	else:
		snap = snap_id
	
	logger.debug('Checking that snapshot %s is completed', snap.id)
	wait_until(
		lambda: snap.update() and snap.status != 'pending', 
		logger=logger, time_until=time_until
	)
	if snap.status == 'error':
		raise PlatformError('Snapshot %s creation failed' % snap.id)
	elif snap.status == 'completed':
		logger.debug('Snapshot %s completed', snap.id)


def create_volume(ec2_conn, size, avail_zone, snap_id=None, logger=None, timeout=DEFAULT_TIMEOUT):
	time_until = time.time() + timeout	
	logger = logger or logging.getLogger(__name__)
	
	msg = 'Creating EBS volume%s%s in avail zone %s' % (
		size and ' (size: %sG)' % size or '', 
		snap_id and ' from snapshot %s' % snap_id or '',
		avail_zone
	)
	logger.debug(msg)
	vol = ec2_conn.create_volume(size, avail_zone, snap_id)
	logger.debug('EBS volume %s created%s', vol.id, snap_id and ' from snapshot %s' % snap_id or '')
	
	logger.debug('Checking that EBS volume %s is available', vol.id)
	wait_until(
		lambda: vol.update() == "available", 
		logger=logger, time_until=time_until
	)
	logger.debug('EBS volume %s available', vol.id)		
	
	return vol

def attach_volume(ec2_conn, volume_id, instance_id, devname, to_me=False, logger=None, timeout=DEFAULT_TIMEOUT):
	time_until = time.time() + timeout	
	logger = logger or logging.getLogger(__name__)
	if isinstance(volume_id, basestring):
		vol = Volume(ec2_conn)
		vol.id = volume_id
	else:
		vol = volume_id
		
	msg = 'Attaching volume %s as device %s%s' % (vol.id, devname, not to_me and ' instance %s' % instance_id or '')
	logger.debug(msg)
	vol.attach(instance_id, devname)
	
	logger.debug('Checking that volume %s is attached', vol.id)
	wait_until(
		lambda: vol.update() and vol.attachment_state() == 'attached', 
		logger=logger, time_until=time_until
	)
	logger.debug('Volume %s attached',  vol.id)
	
	if to_me:
		logger.debug('Checking that device %s is available', devname)
		wait_until(
			lambda: os.access(devname, os.F_OK | os.R_OK), 
			sleep=1, logger=logger, time_until=time_until
		)
		logger.debug('Device %s is available', devname)
		
	return vol

def detach_volume(ec2_conn, volume_id, logger=None, timeout=DEFAULT_TIMEOUT):
	time_until = time.time() + timeout
	logger = logger or logging.getLogger(__name__)
	if isinstance(volume_id, basestring):
		vol = Volume(ec2_conn)
		vol.id = volume_id
	else:
		vol = volume_id
		
	logger.debug('Detaching volume %s', vol.id)
	try:
		vol.detach()
	except BotoServerError, e:
		if e.code != 'IncorrectState':
			raise
	logger.debug('Checking that volume %s is available', vol.id)
	wait_until(
		lambda: vol.update() == 'available', 
		logger=logger, time_until=time_until
	)
	logger.debug('Volume %s is available', vol.id)
	

def delete_volume(ec2_conn, volume_id, logger=None):
	logger = logger or logging.getLogger(__name__)
	logger.debug('Deleting volume %s', volume_id)
	ec2_conn.delete_volume(isinstance(volume_id, basestring) and volume_id or volume_id.id)
