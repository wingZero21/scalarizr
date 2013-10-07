__author__ = 'Nick Demyanchuk'

import os
import sys
import json
import uuid
import logging
from scalarizr.linux import lvm2


LOG = logging.getLogger(__name__)

vg_name = 'vagrant'
cgroup_mpoint = '/sys/fs/cgroup' # Ubuntu default


class StorageManager():


    def __init__(self):
        self.volumes = dict()
        self.snapshots = dict()


    def _server_terminated(self, server_id):
        pass


    def __call__(self, environ, start_response):
        try:
            try:
                length = int(environ['CONTENT_LENGTH'])
                data = environ['wsgi.input'].read(length)
                data = json.loads(data)

                method = getattr(self, data['method'])
                params = data['params']
            except:
                start_response('400 Bad request', [], sys.exc_info())
                return str(sys.exc_info()[1])

            try:
                payload = method(**params)
                result = dict(status='ok')
                if payload:
                    result['payload'] = payload
            except:
                e = sys.exc_info()[1]
                result = dict(status='error', error=str(e))

            headers = [('Content-type', 'application/json'),
                        ('Content-length', str(len(result))),]
            start_response('200 OK', headers)
            return result
        except:
            start_response('500 Internal Server Error', [], sys.exc_info())
            LOG.exception('Unhandled exception')
            return ''


    def create_volume(self, **params):
        size = params.get('size')
        snapshot = params.get('snapshot')

        assert size or snapshot, 'Not enough params to create volume'
        if snapshot:
            snapshot = self.describe_snapshot(snapshot)
        else:
            # Size in Gigabytes
            size = int(size)

        id = str(uuid.uuid4())[:7]

        lvm2.lvcreate(vg_name, name=id, size='%sG' % size)
        lvinfo = lvm2.lvs(lvm2.lvpath(vg_name, id)).values()[0]

        device = os.path.realpath(lvinfo.lv_path)
        stat = os.stat(device)
        maj, min = (os.major(stat.st_rdev), os.minor(stat.st_rdev))

        self.volumes[id] = dict(id=id, device=device, attached_to=None,
                                maj=maj, min=min)
        return dict(id=id, device=lvinfo.lv_path)


    def attach_volume(self, volume_id, instance_id):
        assert volume_id in self.volumes, 'Volume "%s" not found' % volume_id
        volume = self.volumes[volume_id]
        attached_to = volume['attached_to']
        assert attached_to == None, 'Volume already attached to instance "%s"' % attached_to

        with open('devices.allow', 'w') as f:
            f.write("b %s:%s rwm\n" % (volume['maj'], volume['min']))
        volume['attached_to'] = instance_id
        return dict(volume=volume)


    def detach_volume(self, volume_id, instance_id):
        assert volume_id in self.volumes, 'Volume "%s" not found' % volume_id
        volume = self.volumes[volume_id]
        attached_to = volume['attached_to']
        assert attached_to == instance_id, 'Volume not atached to instance "%s"' % instance_id

        with open("devices.deny", 'w') as f:
            f.write("b %s:%s rwm\n" % (volume['maj'], volume['min']))


    def create_snapshot(self, volume_id):
        pass


    def describe_snapshot(self, id):
        try:
            return self.snapshots[id]
        except KeyError:
            raise Exception('Snapshot %s not found' % id)