'''
Created on Jan 16, 2013

@author: marat, uty
'''
from __future__ import with_statement

import logging
import os
import sys
import string
import threading
import glob

from scalarizr import node, linux
from scalarizr.util import wait_until
from scalarizr.util import system2
from scalarizr import storage2
from scalarizr.storage2.volumes import base


__cloudstack__ = node.__node__['cloudstack']
LOG = logging.getLogger(__name__)


if os.path.exists('/dev/xvda1'):
    _device_prefix = '/dev/xvd'
elif glob.glob('/dev/vda*'):
    _device_prefix = '/dev/vd'
else:
    _device_prefix = '/dev/sd'

def get_system_devname(letter):
    return _device_prefix + letter


def deviceid_to_devname(deviceid):
    for i in range(deviceid + 1):
        device = _device_prefix + string.ascii_letters[i]
        if not glob.glob(device + '*'):
            return device
    raise Exception('Cant find device for deviceid: %s' % deviceid)


def devname_to_deviceid(devname):
    return string.ascii_letters.find(devname[-1])


class FreeDeviceLetterMgr(object):

    def __init__(self):
        self._all = set(string.ascii_lowercase[1:16])
        self._acquired = set()
        self._lock = threading.Lock()
        self._local = threading.local()

    def __enter__(self):
        letters = list(self._all - self._acquired)
        letters.sort()
        for l in letters:
            #
            #pattern = get_system_devname(l) + '*'
            if not (glob.glob('/dev/sd' + l + '*') or glob.glob('/dev/xvd' + l + '*')):
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


class CSVolume(base.Volume):

    _free_device_letter_mgr = FreeDeviceLetterMgr()
    _global_timeout = 3600

    def __init__(self,
                 name=None,
                 snapshot_id=None,
                 zone_id=None,
                 disk_offering_id=None,
                 size=None,
                 **kwds):
        '''
        :type name: string
        :param name: Volume name

        :type snapshot_id: string
        :param snapshot_id: Snapshot id

        :type zone_id: int
        :param zone_id: Availability zone id

        :param disk_offering_id: Disk offering ID

        :type size: int or string
        :param size: Volume size in GB
        '''
        base.Volume.__init__(self,
                             name=name,
                             snapshot_id=snapshot_id,
                             zone_id=zone_id,
                             size=size and int(size) or None,
                             disk_offering_id=disk_offering_id,
                             **kwds)
        self._native_vol = None
        self.error_messages.update({
            'no_id_or_conn': 'Volume has no ID and Cloudstack connection '
            'required for volume construction is not available'
        })
        self.error_messages.update({
            'no_connection': 'Cloudstack connection should be available '
            'to perform this operation'})

    def _new_conn(self):
        return __cloudstack__['new_conn']

    def _check_connection(self):
        self._conn = self._new_conn()
        assert self._conn, self.error_messages['no_connection']

    def _clone(self, config):
        config.pop('device', None)
        config.pop('zone_id', None)

    def _check_attachement(self):
        self._native_vol = self._conn.listVolumes(id=self.id)[0]
        if self._attached():
            if self._native_vol.virtualmachineid == __cloudstack__['instance_id']:
                LOG.debug('Volume %s is attached to this instance', self.id)
                return

            LOG.warning('Volume %s is not available. '
                        'It is attached to different instance %s. '
                        'Now scalarizr will detach it',
                        self.id, self._native_vol.virtualmachineid)
            # We should wait for state chage
            if self._native_vol.vmstate == 'Stopping':
                def vm_state_changed():
                    self._native_vol = self._conn.listVolumes(self._native_vol.id)[0]
                    return not hasattr(self._native_vol, 'virtualmachineid') or \
                        self._native_vol.vmstate != 'Stopping'
                wait_until(vm_state_changed)

            # If stil attached, detaching
            if hasattr(self._native_vol, 'virtualmachineid'):
                self._detach()
                LOG.debug('Volume %s detached', self.id)

        LOG.debug('Attaching volume %s to this instance', self.id)
        with self._free_device_letter_mgr:
            letter = self._free_device_letter_mgr.get()
            devname = get_system_devname(letter)
            self._attach(__cloudstack__['instance_id'],
                         devname_to_deviceid(devname))

    def _ensure(self):
        self._native_vol = None
        snapshot_id = None
        self._conn = self._new_conn()
        devname = self.device

        if self._conn:
            try:
                if self.snap:
                    snapshot_id = self.snap['id']
                    self.id = None
                    self.size = None

                if self.id:
                    LOG.debug('Volume %s has been already created', self.id)
                    vol_list = self._conn.listVolumes(id=self.id)
                    if len(vol_list) == 0:
                        raise storage2.StorageError("Volume %s doesn't exist" %
                                                    self.id)
                    self._native_vol = vol_list[0]
                    self._check_attachement()                    

                if not self.id:
                    LOG.debug('Creating new volume')
                    if not self.disk_offering_id:
                        # Any size you want
                        for dskoffer in self._conn.listDiskOfferings():
                            if not dskoffer.disksize and dskoffer.iscustomized:
                                self.disk_offering_id = dskoffer.id
                                break
                    with self._free_device_letter_mgr:
                        letter = self._free_device_letter_mgr.get()
                        devname = get_system_devname(letter)
                        self._native_vol = self._create_volume(
                            name='%s-%02d' % (__cloudstack__['instance_id'],
                                              devname_to_deviceid(devname)),
                            zone_id=__cloudstack__['zone_id'],
                            size=self.size,
                            disk_offering_id=self.disk_offering_id,
                            snap_id=snapshot_id)
                        self.id = self._native_vol.id
                        devname = self._attach(__cloudstack__['instance_id'],
                                     devname_to_deviceid(devname))[1]
                        self._native_vol = self._conn.listVolumes(id=self.id)[0]
            except:
                exc_type, exc_value, exc_trace = sys.exc_info()
                if self._native_vol:
                    LOG.debug('Detaching volume')
                    try:
                        self._conn.detachVolume(id=self._native_vol.id)
                    except:
                        pass

                raise storage2.StorageError, \
                    'Volume construction failed: %s' % exc_value, \
                    exc_trace

            self._config.update({
                'id': self._native_vol.id,
                'size': self._native_vol.size / (1024 * 1024 * 1024),
                'device': devname,
                'zone_id': self._native_vol.zoneid,
                'disk_offering_id': getattr(self._native_vol,
                                            'diskofferingid',
                                            None)})
            self._native_vol = None

    def _snapshot(self, description, tags, **kwds):
        '''
        @type nowait: bool
        @param nowait: Wait for snapshot completion. Default: True
        '''

        self._check_connection()
        snapshot = self._create_snapshot(self.id, kwds.get('nowait', True))
        return storage2.snapshot(
            type='csvol',
            id=snapshot.id,
            description=description,
            tags=tags)

    def _destroy(self, force=False, **kwargs):
        self._detach()
        self._delete()

    def _create_snapshot(self, volume_id, nowait=True):
        self._check_connection()
        LOG.debug('Creating snapshot of volume %s', volume_id)
        system2('sync', shell=True)
        snap = self._conn.createSnapshot(volume_id)
        LOG.debug('Snapshot %s created for volume %s', snap.id, volume_id)

        if not nowait:
            self._wait_snapshot(snap)

        return snap

    def _wait_snapshot(self, snap_id):
        '''
        Waits until snapshot becomes 'completed' or 'error'
        '''
        self._check_connection()
        if hasattr(snap_id, 'id'):
            snap_id = snap_id.id

        LOG.debug('Checking that snapshot %s is completed', snap_id)

        def exit_condition():
            return self._conn.listSnapshots(id=snap_id)[0].state == 'BackedUp'
        wait_until(exit_condition,
                   logger=LOG,
                   timeout=self._global_timeout,
                   error_text="Snapshot %s wasn't completed in a reasonable time" % snap_id)
        LOG.debug('Snapshot %s completed', snap_id)

    def _create_volume(self,
                       name,
                       zone_id,
                       size=None,
                       disk_offering_id=None,
                       snap_id=None):
        self._check_connection()
        if snap_id:
            disk_offering_id = None

        msg = "Creating volume '%s' in zone %s%s%s%s" % (
            name,
            zone_id,
            size and ' (size: %sG)' % size or '',
            snap_id and ' from snapshot %s' % snap_id or '',
            disk_offering_id and ' with disk offering %s' % disk_offering_id or '')
        LOG.debug(msg)

        if snap_id:
            self._wait_snapshot(snap_id)

        vol = self._conn.createVolume(name=name,
                                      size=size,
                                      diskOfferingId=disk_offering_id,
                                      snapshotId=snap_id,
                                      zoneId=zone_id)
        LOG.debug('Volume %s created%s', vol.id, snap_id and ' from snapshot %s' % snap_id or '')

        if vol.state not in ('Allocated', 'Ready'):
            LOG.debug('Checking that volume %s is available', vol.id)
            wait_until(
                lambda: self._conn.listVolumes(id=vol.id)[0].state in ('Allocated', 'Ready'),
                logger=LOG,
                timeout=self._global_timeout,
                error_text="Volume %s wasn't available in a reasonable time" % vol.id
            )
            LOG.debug('Volume %s available', vol.id)

        return vol

    def _attach(self, instance_id, device_id=None):
        self._check_connection()
        volume_id = self.id or self._native_vol.id

        msg = 'Attaching volume %s%s%s' % (volume_id,
                                           device_id and ' as device %s' % deviceid_to_devname(device_id) or '',
                                           ' instance %s' % instance_id or '')
        LOG.debug(msg)
        self._conn.attachVolume(volume_id, instance_id, device_id)

        LOG.debug('Checking that volume %s is attached', volume_id)


        wait_until(self._attached,
                   logger=LOG,
                   timeout=self._global_timeout,
                   error_text="Volume %s wasn't attached in a reasonable time"
                   " (vm_id: %s)." % (volume_id, instance_id))
        LOG.debug('Volume %s attached', volume_id)
        vol = self._conn.listVolumes(id=volume_id)[0]

        # Not true device name
        #devname = deviceid_to_devname(vol.deviceid)
        channel = '/tmp/udev-block-device'

        def scsi_attached():
            # Rescan all SCSI buses
            scsi_host = '/sys/class/scsi_host'
            for name in os.listdir(scsi_host):
                with open(scsi_host + '/' + name + '/scan', 'w') as fp:
                    fp.write('- - -')
            return os.access(channel, os.F_OK | os.R_OK)
        LOG.debug('Checking that device is available in OS')
        wait_until(scsi_attached,
                   sleep=1,
                   logger=LOG,
                   timeout=self._global_timeout,
                   error_text="Device wasn't available in OS in a reasonable time")
        LOG.debug('Device is available in OS')

        with open(channel) as fp:
            devname = fp.read()
        os.remove(channel)        

        return vol, devname

    def _detach(self, force=False, **kwds):
        self._check_connection()
        volume_id = self.id or self._native_vol.id

        LOG.debug('Detaching volume %s', volume_id)
        try:
            self._conn.detachVolume(volume_id)
        except Exception, e:
            if 'not attached' not in str(e):
                raise

        LOG.debug('Checking that volume %s is available', volume_id)
        wait_until(self._detached,
                   logger=LOG,
                   timeout=self._global_timeout,
                   error_text="Volume %s wasn't available in a reasonable time" % volume_id)
        LOG.debug('Volume %s is available', volume_id)

    def _delete(self):
        self._check_connection()
        volume_id = self.id or self._native_vol.id
        LOG.debug('Deleting volume %s', volume_id)
        self._conn.deleteVolume(volume_id)
        self.id = None

    def _attached(self):
        vol = None
        if self.id:
            vol = self._conn.listVolumes(id=self.id)[0]
        else:
            vol = self._native_vol
        return hasattr(vol, 'virtualmachineid')

    def _detached(self):
        return not self._attached()


class CSSnapshot(base.Snapshot):

    _status_map = {'Creating': base.Snapshot.IN_PROGRESS,
                   'BackingUp': base.Snapshot.IN_PROGRESS,
                   'BackedUp': base.Snapshot.COMPLETED,
                   'error': base.Snapshot.FAILED}

    def _new_conn(self):
        try:
            return __cloudstack__['new_conn']
        except:
            pass

    def _check_connection(self):
        self._conn = self._new_conn()
        assert self._conn, self.error_messages['no_connection']

    def __init__(self, **kwds):
        base.Snapshot.__init__(self, **kwds)

    def _status(self):
        snapshots = self._new_conn().listSnapshots(id=self.id)
        if not snapshots:
            raise storage2.StorageError('listSnapshots returned empty list for snapshot %s' % self.id)
        return self._status_map[snapshots[0].state]

    def _destroy(self):
        self._check_conn()
        self._conn.deleteSnapshot(id=self.id)


storage2.volume_types['csvol'] = CSVolume
storage2.snapshot_types['csvol'] = CSSnapshot
