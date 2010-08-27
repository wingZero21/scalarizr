'''
Created on Aug 25, 2010

@author: marat
'''
from scalarizr.util import wait_until
from boto.ec2.volume import Volume
import logging, os
from boto.exception import BotoServerError

def create_volume(ec2_conn, size, avail_zone, snap_id=None, logger=None):
	logger = logger or logging.getLogger(__name__)
	msg = 'Creating EBS volume%s%s in avail zone %s' % (
		size and ' (size: %dG)' % size or '', 
		snap_id and ' from snapshot %s' % snap_id or '',
		avail_zone
	)
	logger.debug(msg)
	vol = ec2_conn.create_volume(size, avail_zone, snap_id)
	logger.debug('EBS volume %s created%s', vol.id, snap_id and ' from snapshot %s' % snap_id or '')
	
	logger.debug('Checking that EBS volume %s is available', vol.id)
	wait_until(lambda: vol.update() == "available", logger=logger)
	logger.debug('EBS volume %s available', vol.id)		
	
	return vol

def attach_volume(ec2_conn, volume_id, instance_id, devname, to_me=False, logger=None):
	logger = logger or logging.getLogger(__name__)
	if isinstance(volume_id, str):
		vol = Volume(ec2_conn)
		vol.id = volume_id
	else:
		vol = volume_id
		
	msg = 'Attaching volume %s as device %s%s' % (vol.id, devname, not to_me and ' instance %s' % instance_id or '')
	logger.debug(msg)
	vol.attach(instance_id, devname)
	
	logger.debug('Checking that volume %s is attached', vol.id)
	wait_until(lambda: vol.update() and vol.attachment_state() == 'attached', logger=logger)
	logger.debug('Volume %s attached',  vol.id)
	
	if to_me:
		logger.debug('Checking that device %s is available', devname)
		wait_until(lambda: os.access(devname, os.F_OK | os.R_OK), sleep=1, logger=logger)
		logger.debug('Device %s is available', devname)
		
	return vol

def detach_volume(ec2_conn, volume_id, logger=None):
	logger = logger or logging.getLogger(__name__)
	if isinstance(volume_id, str):
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
	wait_until(lambda: vol.update() == 'available')
	logger.debug('Volume %s is available', vol.id)
	

def delete_volume(ec2_conn, volume_id, logger=None):
	logger = logger or logging.getLogger(__name__)
	logger.debug('Deleting volume %s', volume_id)
	ec2_conn.delete_volume(isinstance(volume_id, str) and volume_id or volume_id.id)
