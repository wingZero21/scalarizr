from __future__ import with_statement
__author__ = 'Nick Demyanchuk'

import os
import sys
import time
import random
import logging
import datetime
import urlparse

from apiclient.http import MediaFileUpload, MediaIoBaseDownload
from apiclient.errors import HttpError

from scalarizr.bus import bus
from scalarizr import storage, util
from scalarizr.node import __node__
from scalarizr.storage import transfer



LOG = logging.getLogger(__name__)
CHUNK_SIZE = 2*1024*1024




class GoogleCSTransferProvider(transfer.TransferProvider):

    schema = 'gcs'
    urlparse.uses_netloc.append(schema)

    def list(self, remote_path):
        bucket, path = self._parse_path(remote_path)

        req = self.cloudstorage.objects().list(
                bucket=bucket, prefix=path, delimiter='/'
        )
        resp = req.execute()
        return tuple('gcs://%s' % o['name'] for o in resp['items'])


    def get(self, remote_path, local_path):
        LOG.debug('Downloading %s from cloud storage (local path: %s)', remote_path, local_path)
        bucket, name = self._parse_path(remote_path)
        local_path = os.path.join(local_path, os.path.basename(remote_path))
        f = open(local_path, 'w')
        request = self.cloudstorage.objects().get_media(
                bucket=bucket, object=name)
        media = MediaIoBaseDownload(f, request, chunksize=CHUNK_SIZE)
        done = False

        while not done:
            status, done = media.next_chunk()
            if status:
                LOG.debug('Downloaded %d%%' % int(status.progress() * 100))

        LOG.debug('Download complete.')
        return local_path


    def put(self, local_path, remote_path):
        LOG.debug('Uploading %s to cloud storage (remote path: %s)', local_path, remote_path)

        filename = os.path.basename(local_path)
        bucket, name = self._parse_path(remote_path)
        name = os.path.join(name, filename)

        buckets = self._list_buckets()
        if bucket not in buckets:
            self._create_bucket(bucket)

        media = MediaFileUpload(local_path,
                'application/octet-stream',
                resumable=True)

        response = None
        req = self.cloudstorage.objects().insert(
                bucket=bucket, name=name, media_body=media
        )
        last_progress = 0
        exponent_backoff = [1, 2, 4, 8, 16, 32]                        
        while response is None:
            try:
                status, response = req.next_chunk()
                if status:
                    percentage = int(status.progress() * 100)
                    if percentage - last_progress >= 10:
                        LOG.debug("Uploaded %d%%." % percentage)
                        last_progress = percentage
                exponent_backoff = [1, 2, 4, 8, 16, 32]                        
            except HttpError, e:
                LOG.debug('Caught %s' % e)
                if not exponent_backoff or not int(e.resp.status) in (500, 502, 503, 504):
                    raise


                sec_to_wait = exponent_backoff.pop(0)
                # add random milliseconds
                sec_to_wait += random.random()
                LOG.debug('retry in %s' % sec_to_wait)
                time.sleep(sec_to_wait)

        LOG.debug('Upload completed.')
        return 'gcs://%s' % os.path.join(bucket, name)



    def _parse_path(self, path):
        o = urlparse.urlparse(path)
        if o.scheme != self.schema:
            raise transfer.TransferError('Wrong schema')
        return o.hostname, o.path[1:]


    def _list_buckets(self):
        pl = bus.platform
        proj_id = pl.get_numeric_project_id()

        req = self.cloudstorage.buckets().list(projectId=proj_id)
        resp = req.execute()
        if 'items' not in resp:
            return []
        buckets = [b['id'] for b in resp['items']]
        return buckets


    def _create_bucket(self, bucket_name):
        pl = bus.platform
        proj_id = pl.get_numeric_project_id()

        req_body = dict(id=bucket_name, projectId=proj_id)
        req = self.cloudstorage.buckets().insert(body=req_body)
        try:
            req.execute()
        except:
            e = sys.exc_info()[1]
            if not 'You already own this bucket' in str(e):
                raise


    @property
    def cloudstorage(self):
        pl = bus.platform
        return pl.new_storage_client()


class GceEphemeralVolume(storage.Volume):
    pass


class GceEphemeralVolumeProvider(storage.VolumeProvider):
    type = 'gce_ephemeral'
    vol_class = GceEphemeralVolume

    def create(self, **kwargs):
        # Name - full device name (google created link name)
        name = kwargs.get('name')
        if not name:
            raise storage.StorageError('Device_name attribute should be non-empty')

        device = '/dev/disk/by-id/google-%s' % name
        if not os.path.exists(device):
            raise storage.StorageError("Device '%s' not found" % device)

        kwargs['device'] = device
        super(GceEphemeralVolumeProvider, self).create(**kwargs)


    def create_snapshot(self, vol, snap, **kwargs):
        raise storage.StorageError("Snapshotting is unsupported by GCE"
                                                                "ephemeral disks.")


    def create_from_snapshot(self, **kwargs):
        raise storage.StorageError("GCE ephemeral disks have no snapshots.")


storage.Storage.explore_provider(GceEphemeralVolumeProvider)


class GcePersistentVolume(storage.Volume):

    @property
    def link(self):
        compute = __node__['gce']['compute_connection']
        project_id = __node__['gce']['project_id']
        return '%s%s/disks/%s' % (compute._baseUrl, project_id, self.name)


class GcePersistentSnapshot(storage.Snapshot):
    pass


class GcePersistentVolumeProvider(GceEphemeralVolumeProvider):
    type = 'gce_persistent'
    vol_class = GcePersistentVolume
    snap_class = GcePersistentSnapshot


    def create_snapshot(self, vol, snap, **kwargs):
        compute = __node__['gce']['compute_connection']
        project_id = __node__['gce']['project_id']
        now_raw = datetime.datetime.utcnow()
        now_str = now_raw.strftime('%d-%b-%Y-%H-%M-%S-%f')
        snap_name = '%s-snap-%s' % (vol.id, now_str)

        op = compute.snapshots().insert(project=project_id,
                                                        body = dict(
                                                                        name=snap_name,
                                                                        sourceDisk=vol.link,
                                                                        sourceDiskId=vol.id,
                                                                        description=snap.description
                                                        ))

        wait_for_operation_to_complete(compute, project_id, op['name'])
        gce_snap = compute.snapshots().get(project=project_id,
                                                                snapshot=snap_name,
                                                                fields='id').execute()

        snap.id = gce_snap['id']
        snap.name = snap_name

        return snap


    def create_from_snapshot(self, **kwargs):
        raise storage.StorageError("Can't create from snapshot - attaching to "
                                "running instances is unsupported")


    def destroy_snapshot(self, snap):
        try:
            connection = __node__['gce']['compute_connection']
            project_id = __node__['gce']['project_id']

            op = connection.snapshots().delete(project=project_id, snapshot=snap.name).execute()
            wait_for_operation_to_complete(connection, project_id, op['name'])
        except:
            e = sys.exc_info()[1]
            raise storage.StorageError('Failed to delete google disk snapshot.'
                                                                    ' Error: %s' % e)



storage.Storage.explore_provider(GcePersistentVolumeProvider)
