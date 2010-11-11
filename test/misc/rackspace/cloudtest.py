__author__ = 'Dmytro Korsakov'

import os
import time
import subprocess
import cloudfiles
import logging


def timeit(method):

    def timed(*args, **kw):
        ts = time.time()
        result = method(*args, **kw)
        te = time.time()

        print '%r (%r, %r) %2.2f sec' % \
              (method.__name__, args, kw, te-ts)
        return result

    return timed


class CloudTest:

    PREFIX  = 'snapshot'

    def __init__(self, login, key):
        self.login = login
        self.key = key
        self.logger = logging.getLogger(__name__)

    @timeit
    def make_shadow_copy(self, source_volume, dest_dir, chunk_size = '30M'):
        cmd1 = ['dd', 'if=%s' % source_volume]
        cmd2 = ['gzip']
        cmd3 = ['split', '-a','3', '-b', '%s'%chunk_size, '-', '%s/%s.gz.' % (dest_dir, self.PREFIX)]
        p1 = subprocess.Popen(cmd1, stdout=subprocess.PIPE)
        p2 = subprocess.Popen(cmd2, stdin=p1.stdout, stdout=subprocess.PIPE)
        p3 = subprocess.Popen(cmd3, stdin=p2.stdout, stdout=subprocess.PIPE)
        self.logger.info('Making shadow copy')
        output = p3.communicate()[0]
        print output

    @timeit
    def upload_files(self,location, remote_location, container_name='test_container', cleanup=True):
        conn = cloudfiles.get_connection(username=self.login, api_key=self.key, serviceNet=True)

        files = []
        for file in os.listdir(location):
            if file.startswith(self.PREFIX):
                files.append(file)

        try:
            container = conn.create_container(container_name)
        except BaseException, Exception:
            self.logger.error('Cannot create container')

        for file in files:
            self.logger.info('Uploading %s to container %s' % (file, container_name))
            full_path = os.path.join(location,file)
            basename = os.path.basename(file)
            o = container.create_object(remote_location+basename)
            o.load_from_filename(full_path)

            if cleanup:
                self.logger.info('Deleting file')
                os.remove(full_path)

        print 'done.'


if __name__ == '__main__':

    login = 'rackcloud05'
    key = '27630d6e96e72fa43233a185a0518f0e'

    location = '/mnt/dest/'
    remote_location = 'backup01'



    CT = CloudTest(login, key)
    CT.make_shadow_copy('/dev/loop0', '/mnt/dest')
    CT.upload_files(location, remote_location)