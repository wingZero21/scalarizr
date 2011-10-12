'''
Created on Aug 25, 2010

@author: marat
'''

from scalarizr.util import wait_until, system2

import logging, os


DEFAULT_TIMEOUT = 2400 		# 40 min
SNAPSHOT_TIMEOUT = 3600		# 1 h
LOG = logging.getLogger(__name__)


def create_snapshot(conn, volume_id, logger=None, timeout=SNAPSHOT_TIMEOUT, wait_completion=False):
	if hasattr(volume_id, 'id'):
		volume_id = volume_id.id
	logger = logger or LOG
	
	# Create snapshot
	logger.debug('Creating snapshot of volume %s', volume_id)
	system2('sync', shell=True)
	snap = conn.createSnapshot(volume_id)
	logger.debug('Snapshot %s created for volume %s', snap.id, volume_id)
	

	if wait_completion:
		wait_snapshot(conn, snap, logger, timeout)
		
	return snap


def wait_snapshot(conn, snap_id, logger=None, timeout=SNAPSHOT_TIMEOUT):
	'''
	Waits until snapshot becomes 'completed' or 'error'
	'''
	logger = logger or LOG
	if hasattr(snap_id, 'id'):
		snap_id = snap_id.id
	
	logger.debug('Checking that snapshot %s is completed', snap_id)
	wait_until(
		lambda: conn.listSnapshots(id=snap_id)[0].state == 'BackedUp', 
		logger=logger, timeout=timeout,
		error_text="Ssnapshot %s wasn't completed in a reasonable time" % snap_id
	)
	logger.debug('Snapshot %s completed', snap_id)


def create_volume(conn, name, size=None, disk_offering_id=None, snap_id=None, 
				logger=None, timeout=DEFAULT_TIMEOUT):
	logger = logger or LOG
	
	msg = "Creating volume '%s'%s%s%s" % (
		name,
		size and ' (size: %sG)' % size or '', 
		snap_id and ' from snapshot %s' % snap_id or '',
		disk_offering_id and ' with disk offering %s' % disk_offering_id or ''
	)
	logger.debug(msg)
	
	if snap_id:
		wait_snapshot(conn, snap_id, logger)
	
	vol = conn.createVolume(name, size=size, diskOfferingId=disk_offering_id, snapshotId=snap_id)
	logger.debug('Volume %s created%s', vol.id, snap_id and ' from snapshot %s' % snap_id or '')
	
	if vol.state != 'Ready':
		logger.debug('Checking that volume %s is available', vol.id)
		wait_until(
			lambda: conn.listVolumes(id=vol.id)[0].state == 'Ready', 
			logger=logger, timeout=timeout,
			error_text="Volume %s wasn't available in a reasonable time" % vol.id
		)
		logger.debug('Volume %s available', vol.id)		
	
	return vol


def attach_volume(conn, volume_id, instance_id, devname=None, 
				to_me=False, logger=None, timeout=DEFAULT_TIMEOUT):
	logger = logger or LOG
	if hasattr(volume_id, 'id'):
		volume_id = volume_id.id
		
	msg = 'Attaching volume %s%s%s' % (volume_id, 
				devname and ' as device %s' % devname or '', 
				not to_me and ' instance %s' % instance_id or '')
	logger.debug(msg)
	conn.attachVolume(volume_id, instance_id, devname)
	
	
	logger.debug('Checking that volume %s is attached', volume_id)
	wait_until(
		lambda: conn.listVolumes(volume_id)[0].state == 'Ready', 
		logger=logger, timeout=timeout,
		error_text="Volume %s wasn't attached in a reasonable time"
				" (vm_id: %s)." % ( 
				volume_id, instance_id)
	)
	logger.debug('Volume %s attached',  volume_id)
	
	if not devname:
		devname = conn.listVolumes(volume_id)[0].deviceid
	devname = real_devname(devname)
	if to_me:
		logger.debug('Checking that device %s is available', devname)
		wait_until(
			lambda: os.access(devname, os.F_OK | os.R_OK), 
			sleep=1, logger=logger, timeout=timeout,
			error_text="Device %s wasn't available in a reasonable time" % devname
		)
		logger.debug('Device %s is available', devname)
		
	return conn.listVolumes(volume_id)[0], devname


def get_system_devname(devname):
	return devname.replace('/sd', '/xvd') if os.path.exists('/dev/xvda1') else devname
real_devname = get_system_devname


def get_ebs_devname(devname):
	return devname.replace('/xvd', '/sd')


def detach_volume(conn, volume_id, force=False, logger=None, timeout=DEFAULT_TIMEOUT):
	logger = logger or LOG
	if hasattr(volume_id, 'id'):
		volume_id = volume_id.id
		
	logger.debug('Detaching volume %s', volume_id)
	conn.detachVolume(volume_id)

	logger.debug('Checking that volume %s is available', volume_id)
	wait_until(
		lambda: conn.listVolumes(id=volume_id)[0].state == 'Allocated',
		logger=logger, timeout=timeout,
		error_text="Volume %s wasn't available in a reasonable time" % volume_id
	)
	logger.debug('Volume %s is available', volume_id)
	

def delete_volume(conn, volume_id, logger=None):
	logger = logger or LOG
	if hasattr(volume_id, 'id'):
		volume_id = volume_id.id
	logger.debug('Deleting volume %s', volume_id)
	conn.deleteVolume(volume_id)



