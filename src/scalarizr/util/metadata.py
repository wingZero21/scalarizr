# pylint: disable=R0924

import urllib2
import json
import logging
import socket
import urlparse
import re
import os
import posixpath
import glob
import time

from scalarizr import linux


LOG = logging.getLogger(__name__)


class Error(Exception):
    pass

class NoProviderError(Error):
    pass

class NoUserDataError(Error):
    pass


def parse_user_data(data):
    return dict(re.findall("([^=]+)=([^;]*);?", data))


class Meta(object):
    platform = None
    def __getitem__(self, name):
        raise NotImplementedError()

    def user_data(self):
        # Scalr encoded user-data
        raise NotImplementedError()

    def supported(self):
        raise NotImplementedError()

    def __repr__(self):
        return self.__class__.__name__[:-4].lower()


class UrlMeta(Meta):
    base_url = None
    user_data_rel = None
    socket_timeout = 5

    def __getitem__(self, rel):
        result = list(urlparse.urlparse(self.base_url))
        result[2] = posixpath.normpath(result[2] + '/' + rel)
        url = urlparse.urlunparse(result)
        try:
            return urllib2.urlopen(url, timeout=self.socket_timeout).read().strip()
        except urllib2.HTTPError, e:
            if e.code == 404:
                msg = "Such meta-data doesn't exists: {0}".format(url)
                raise KeyError(msg)
            raise

    def user_data(self):
        try:
            user_data = self[self.user_data_rel]
        except KeyError:
            raise NoUserDataError()
        else:
            return parse_user_data(user_data)

    def supported(self):
        try:
            pr = urlparse.urlparse(self.base_url)
            socket.gethostbyname(pr.hostname)
            return True
        except:
            return False


class FileMeta(Meta):
    # pylint: disable=W0223

    def __init__(self, filename):
        self.filename = filename

    def user_data(self):
        try:
            user_data = open(self.filename).read().strip()
        except:
            raise NoUserDataError()
        else:
            return parse_user_data(user_data)

    def supported(self):
        return os.access(self.filename, os.R_OK)

    def __repr__(self):
        return 'file({0})'.format(self.filename)


class Ec2Meta(UrlMeta):
    platform = 'ec2'
    base_url = 'http://169.254.169.254/latest/meta-data'
    user_data_rel = '../user-data'


class CloudStackMeta(UrlMeta):
    platform = 'cloudstack'

    def __init__(self, router_host=None, 
            leases_pattern='/var/lib/dhc*/dhclient*.leases'):
        if not router_host:
            router_host = CloudStackMeta.dhcp_server_identifier(leases_pattern)
        LOG.debug('Use router_host: %s', router_host)
        self.base_url = 'http://{0}/latest'.format(router_host)
        self.user_data_rel = 'user-data'

    @staticmethod
    def dhcp_server_identifier(leases_pattern=None):
        router_host = None
        try:
            leases_file = glob.glob(leases_pattern)[0]
            LOG.debug('Use DHCP leases file: %s', leases_file)
        except IndexError:
            msg = "Pattern {0} doesn't matches any leases files".format(leases_pattern)
            raise Error(msg)
        for line in open(leases_file):
            if 'dhcp-server-identifier' in line:
                router_host = filter(None, line.split(';')[0].split(' '))[2]
        return router_host


class OpenStackMeta(UrlMeta):
    platform = 'openstack'
    user_data_rel = 'meta'

    def __init__(self, meta_data_url='http://169.254.169.254/openstack/latest/meta_data.json'):
        self.meta_data_url = meta_data_url
        self.cached = None

    def __getitem__(self, key):
        if not self.cached:
            data = urllib2.urlopen(self.meta_data_url, 
                    timeout=self.socket_timeout).read().strip()
            self.cached = json.loads(data)
        return self.cached[key]


class GceMeta(UrlMeta):
    platform = 'gce'
    base_url = 'http://metadata/computeMetadata/v1beta1/instance'
    user_data_rel = 'attributes/scalr'


def meta(timeout=None):
    if linux.os.windows:
        pvds = (Ec2Meta(), 
                OpenStackMeta(), 
                FileMeta('C:\\Program Files\\Scalarizr\\etc\\private.d\\.user-data'))
    else:
        pvds = (Ec2Meta(), 
                OpenStackMeta(), 
                CloudStackMeta(), 
                GceMeta(), 
                FileMeta('/etc/.scalr-user-data'), 
                FileMeta('/etc/scalr/private.d/.user-data'))
    for _ in range(0, timeout or 1):
        for obj in pvds:
            if obj.supported():
                return obj
        if timeout:
            time.sleep(1)

    msg = "meta-data provider not found. We've tried those ones: {0}".format(pvds)
    raise NoProviderError(msg)

