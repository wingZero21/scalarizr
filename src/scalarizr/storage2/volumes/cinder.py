from __future__ import with_statement

import os
import sys
import glob
import string
import threading
from time import sleep

import novaclient.exceptions as nova_exc

from scalarizr import node
from scalarizr import storage2
from scalarizr import util
from scalarizr.storage2.volumes import base
from scalarizr.linux import coreutils
from scalarizr import linux

__openstack__ = node.__node__['openstack']
LOG = storage2.LOG


def name2device(name):
    if name.startswith('/dev/xvd'):
        return name
    if storage2.RHEL_DEVICE_ORDERING_BUG or os.path.exists('/dev/xvda1'):
        name = name.replace('/vd', '/xvd')
    if storage2.RHEL_DEVICE_ORDERING_BUG:
        name = name[0:8] + chr(ord(name[8]) + 4) + name[9:]
    if name.startswith('/dev/vd'):
        devices = ['/dev/vd' + a for a in string.ascii_lowercase[1:16]]
        devices = [dev for dev in devices if not os.path.exists(dev)]
        if devices:
            return devices[0]
    return name


def device2name(device):
    if device.startswith('/dev/vd'):
        return device
    elif storage2.RHEL_DEVICE_ORDERING_BUG:
        device = device[0:8] + chr(ord(device[8]) - 4) + device[9:]
    return device.replace('/xvd', '/vd')


class FreeDeviceLetterMgr(object):

    def __init__(self):
        # skip vdb cause enter.it has problems with it
        # skip vdc cause rax-ng always returns 'The supplied device path (/dev/vdb) is in use'
        self._all = set(string.ascii_lowercase[4:16])
        self._acquired = set()
        self._lock = threading.Lock()
        self._local = threading.local()

    def __enter__(self):
        with self._lock:
            cinder = __openstack__['new_cinder_connection']
            cinder.reconnect()

            volumes = cinder.volumes.list()
            devices = [atmt['device'] 
                        for vol in volumes 
                        for atmt in vol.attachments
                        if atmt['server_id'] == __openstack__['server_id']]
            acquired = set(device[-1] for device in devices)

            letters = list(self._all - acquired)
            letters.sort()
            if letters:
                letter = letters[0]
                self._acquired.add(letter)
                self._local.letter = letter
                return self

            msg = 'No free letters for block device name remains'
            raise storage2.StorageError(msg)

    def get(self):
        return self._local.letter

    def __exit__(self, *args):
        if hasattr(self._local, 'letter'):
            self._acquired.remove(self._local.letter)
            del self._local.letter


class CinderVolume(base.Volume):
    attach_lock = threading.Lock()
    letters = set(string.ascii_lowercase[1:16])

    @classmethod
    def taken_devices(cls):
        devs = glob.glob('/dev/xvd*') + glob.glob('/dev/vd*') + glob.glob('/dev/sd*')
        devs = [x for x in devs if x[-1] in string.ascii_lowercase]
        return devs

    @classmethod
    def taken_letters(cls):
        lets = [x[-1] for x in cls.taken_devices()]
        return set(lets)


    _global_timeout = 3600
    _free_device_letter_mgr = FreeDeviceLetterMgr()


    def _check_cinder_connection(self):
        if not self._cinder:
            self._cinder = __openstack__['new_cinder_connection']
        assert self._cinder.has_connection, \
            self.error_messages['no_connection']

    def _check_nova_connection(self):
        if not self._nova:
            self._nova = __openstack__['new_nova_connection']
        assert self._nova.has_connection, \
            self.error_messages['no_connection']

    def __init__(self,
                 size=None,
                 snapshot_id=None,
                 avail_zone=None,
                 tags=None,
                 volume_type=None,
                 **kwds):
        base.Volume.__init__(self,
                             size=size and int(size) or None,
                             snapshot_id=snapshot_id,
                             avail_zone=avail_zone,
                             tags=tags,
                             volume_type=volume_type,
                             **kwds)
        self.error_messages.update({
            'no_id_or_conn': 'Volume has no ID or Cinder volume connection '
            'required for volume construction'})
        self.error_messages.update({
            'no_connection': 'Cinder connection should be available '
            'to perform this operation'})
        self._cinder = __openstack__['new_cinder_connection']
        self._nova = __openstack__['new_nova_connection']
        # http://www.linux-kvm.org/page/Hotadd_pci_devices
        for mod in ('acpiphp', 'pci_hotplug'):
            try:
                coreutils.modprobe(mod)
            except:
                # Ignore errors like FATAL: Module acpiphp not found
                pass

    def mount(self):
        # Workaround : cindervolume remounts ro sometimes, fsck it first
        mounted_to = self.mounted_to()
        if self.is_fs_created() and not mounted_to:
            self._check_attr('device')
            self._check_attr('fstype')
            fs = storage2.filesystem(self.fstype)
            if fs.type.startswith('ext'):
                rcode = linux.system(("/sbin/e2fsck", "-fy", self.device), raise_exc=False)[2]
                if rcode not in (0, 1):
                    raise storage2.StorageError('Fsck failed to correct file system errors')
        super(CinderVolume, self).mount()

    def _server_id(self):
        srv_id = __openstack__['server_id']
        return srv_id

    def _ensure(self):
        assert (self._cinder and self._cinder.has_connection) or self.id, \
            self.error_messages['no_id_or_conn']

        if self._cinder:
            volume = None
            if self.id:
                volume = self._cinder.volumes.get(self.id)

                if volume.availability_zone != self.avail_zone:
                    LOG.warn('Cinder volume %s is in the different '
                             'availability zone (%s). Snapshoting it '
                             'and create a new Cinder volume in %s',
                             volume.id,
                             volume.availability_zone, self.avail_zone)
                    self.snapshot_id = self._create_snapshot(self.id).id
                    self.id = None
                    volume = None
                else:
                    self.size = volume.size
            elif self.snap:
                self.snapshot_id = self.snap['id']
                #TODO: take tags from snapshot, if they exist
                # if not self.tags:
                #     self.tags = self.snap.get('tags', {})

            if not self.id:

                volume = self._create_volume(size=self.size,
                                             snapshot_id=self.snapshot_id,
                                             avail_zone=self.avail_zone,
                                             volume_type=self.volume_type)
                self.size = volume.size
                self.id = volume.id

            server_ids = map(lambda info: info['server_id'],
                             volume.attachments)
            my_server_id = __openstack__['server_id']
            if not (volume.status == 'in-use' and
                    my_server_id in server_ids):
                self._wait_status_transition()
                if len(volume.attachments) > 0:
                    self._detach_volume()
                device = self._attach_volume(server_id=my_server_id)
            elif not self.device:
                device = volume.attachments[0]['device']

            self._config.update({
                'id': volume.id,
                'avail_zone': volume.availability_zone,
                'size': volume.size,
                'volume_type': volume.volume_type,
                'device': device})

        # TODO: check device availability

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
                       volume_type=None):
        LOG.debug('Creating Cinder volume (zone: %s size: %s snapshot: %s '
                  'volume_type: %s)', avail_zone, size,
                  snapshot_id, volume_type)
        volume = self._cinder.volumes.create(size=size,
                                             display_name=name,
                                             snapshot_id=snapshot_id,
                                             display_description=
                                             display_description,
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
        self._check_cinder_connection()

        LOG.debug('Creating snapshot of Cinder volume %s', volume_id)
        coreutils.sync()
        snapshot = self._cinder.volume_snapshots.create(volume_id,
                                                        force=True,
                                                        display_description=description)
        LOG.debug('Snapshot %s created for Cinder volume %s',
                  snapshot.id, volume_id)
        if not nowait:
            self._wait_snapshot(snapshot.id)
        return snapshot

    def _snapshot(self, description, tags, **kwds):
        snapshot = self._create_snapshot(self.id, description,
                                         kwds.get('nowait', True))
        return storage2.snapshot(
            type='cinder',
            id=snapshot.id,
            description=snapshot.display_description,
            tags=tags)


    def _attach_volume(self, server_id=None):
        '''
        :rtype: tuple(cloud_device_name, os_device_name)
        '''
        if server_id is None:
            server_id = self._server_id()
        volume_id = self.id
        self._check_nova_connection()

        ops_delay = 10
        with self.attach_lock:
            for _ in xrange(5):
                taken_before = self.taken_devices()
                self._nova.volumes.create_server_volume(server_id, volume_id, None)

                #waiting for attaching transitional state
                LOG.debug('Checking that Cinder volume %s is attached', volume_id)
                new_status = self._wait_status_transition(volume_id)
                if new_status == 'in-use':
                    LOG.debug('Cinder volume %s attached', volume_id)
                    break
                elif new_status == 'available':
                    LOG.warn('Volume %s status changed to "available" instead of "in-use"')
                    LOG.debug('Will try attach volume again after %d seconds', ops_delay)
                    continue
                else:
                    msg = 'Unexpected status transition "available" -> "{0}".' \
                            ' Cinder volume {1}'.format(new_status, volume_id)
                    raise storage2.StorageError(msg)

            util.wait_until(lambda: self.taken_devices() > taken_before,
                    start_text='Checking that volume %s is available in OS' % volume_id,
                    timeout=30,
                    sleep=1,
                    error_text='Volume %s attached but not available in OS' % volume_id)

            devices = list(self.taken_devices() - taken_before)
            if len(devices) > 1:
                msg = "While polling for attached device, got multiple new devices: %s. " \
                    "Don't know which one to select".format(devices)
                raise Exception(msg)
            return devices[0]



        '''
                except nova_exc.Conflict:
                    raise
                except nova_exc.ClientException, e:
                    LOG.warn('Exception caught while trying'
                             'to attach volume %s: \n%s ', volume_id, e)
                    LOG.debug('Will try again after %d seconds.', ops_delay)
                    sleep(ops_delay)
                else:
                    break

        # It's important to calculate device name here, cause 
        # after device attachment, it will be counted as used      
        device = name2device(device_name) 

        #volume attaching  
        LOG.debug('Attaching Cinder volume %s (device: %s) to server %s',
                  volume_id,
                  device_name,
                  server_id)
        self._check_nova_connection()
        ops_delay = 10
        for _ in xrange(5):
            for _ in xrange(5):
                try:
                    self._nova.volumes.create_server_volume(
                            server_id, volume_id, device_name)
                except nova_exc.Conflict:
                    raise
                except nova_exc.ClientException, e:
                    LOG.warn('Exception caught while trying'
                             'to attach volume %s: \n%s ', volume_id, e)
                    LOG.debug('Will try again after %d seconds.', ops_delay)
                    sleep(ops_delay)
                else:
                    break

            #waiting for attaching transitional state
            LOG.debug('Checking that Cinder volume %s is attached', volume_id)
            new_status = self._wait_status_transition(volume_id)
            if new_status == 'in-use':
                LOG.debug('Cinder volume %s attached', volume_id)
                break
            elif new_status == 'available':
                LOG.warn('Volume %s status changed to "available" instead of "in-use"')
                LOG.debug('Will try attach volume again after %d seconds', ops_delay)
                continue
            else:
                msg = 'Unexpected status transition "available" -> "{0}".' \
                        ' Cinder volume {1}'.format(new_status, volume_id)
                raise storage2.StorageError(msg)


        # Checking device availability in OS
        LOG.debug('Cinder device name %s is mapped to %s in operation system',
                  device_name, device)
        LOG.debug('Checking that device %s is available', device)

        msg = 'Device %s is not available in operation system. ' \
              'Timeout reached (%s seconds)' % (
              device, self._global_timeout)
        util.wait_until(lambda: os.access(device, os.F_OK | os.R_OK),
                        sleep=1,
                        logger=LOG,
                        timeout=self._global_timeout,
                        error_text=msg)
        LOG.debug('Device %s is available', device)
        return device
        '''

    def _detach(self, force, **kwds):
        self._detach_volume()

    def _detach_volume(self):
        volume_id = self.id

        self._check_cinder_connection()
        volume = self._cinder.volumes.get(volume_id)

        LOG.debug('Detaching Cinder volume %s', volume_id)
        if volume.status != 'available':
            try:
                # self._cinder.volumes.detach(volume_id)
                self._check_nova_connection()
                server_id = volume.attachments[0]['server_id']
                self._nova.volumes.delete_server_volume(server_id, volume_id)
            except:
                e = sys.exc_info()[1]
                LOG.error('Exception caught when detaching volume: %s', e)

            LOG.debug('Checking that Cinder volume %s is detached '
                      'and available', volume_id)

            def exit_condition():
                vol = self._cinder.volumes.get(volume_id)
                return vol.status == 'available'

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

        volume = self._cinder.volumes.get(self.id)
        if len(volume.attachments) > 0:
            self._detach_volume()

        self._cinder.volumes.delete(self.id)
        self.id = None

    def _clone(self, config):
        config.pop('device', None)
        config.pop('avail_zone', None)

    def _wait_status_transition(self, volume_id=None):
        """
        Wait until volume enters stable state (not 'detaching' or 'attaching')
        :param volume_id:
        :return: volume status
        """
        if not volume_id:
            volume_id = self.id

        status = self._cinder.volumes.get(volume_id).status
        vol = [None]

        def exit_condition():
            vol[0] = self._cinder.volumes.get(volume_id)
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
        return vol[0].status

    def _wait_snapshot(self, snapshot_id):
        LOG.debug('Checking that Cinder snapshot %s is completed', snapshot_id)

        msg = "Cinder snapshot %s wasn't completed. " \
            "Timeout reached (%s seconds)" % (
                snapshot_id, self._global_timeout)
        snap = [None]

        def exit_condition():
            snap[0] = self._cinder.volume_snapshots.get(snapshot_id)
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
        self._cinder = __openstack__['new_cinder_connection']

    def _status(self):
        self._check_cinder_connection()
        snapshot = self._cinder.volume_snapshots.get(self.id)
        return self._status_map[snapshot.status]

    def _destroy(self):
        self._check_cinder_connection()
        self._cinder.volume_snapshots.delete(self.id)


storage2.volume_types['cinder'] = CinderVolume
storage2.snapshot_types['cinder'] = CinderSnapshot
