import logging
import sys
import time

from cloudservers.exceptions import CloudServersException, NotFound

from scalarizr.bus import bus
from scalarizr.config import ScalarizrState
from scalarizr.util import wait_until
from scalarizr.util import system2
from scalarizr.node import __node__
from scalarizr.api.image import ImageAPIDelegate
from scalarizr.api.image import ImageAPIError


_logger = logging.getLogger(__name__)


class RackspaceImageAPIDelegate(ImageAPIDelegate):

    duplicate_errmsg = 'Another image is currently creating from this server. ' \
        'Image: id=%s name=%s status=%s\n' \
        'Rackspace allows to create only ONE image per server at a time. ' \
        'Try again later.'

    def _find_server_id(self, conn):
        pl = __node__['platform']

        _logger.debug('Lookup server %s on CloudServers', pl.get_private_ip())
        try:
            server = conn.servers.find(private_ip=pl.get_private_ip())
            _logger.debug('Found server %s. server id: %s', pl.get_private_ip(), server.id)
        except NotFound:
            raise ImageAPIError('Server %s not found in servers list' % pl.get_private_ip())

        return server.id

    def _find_image(self, conn, server_id, n_retries=10, retry_delay=30):
        for _ in xrange(n_retries):
            try:
                return conn.images.find(serverId=server_id)
            except NotFound:
                _logger.info('Image not found. Sleeping %s seconds and retry', retry_delay)
                time.sleep(retry_delay)
        else:
            raise ImageAPIError('Image not found. Retry limits exceed')


    def _create_image(self, conn, image_name, server_id):
        # TODO: rewrite this
        try:
            return conn.images.create(image_name, server_id)

        except CloudServersException, e:
            if 'Unhandled exception occurred during processing' in str(e):
                return None
            if 'Cannot create a new backup request while saving a prior backup or migrating' in str(e):
                _logger.warning('Rackspace answered with "%s"', str(e))
                _logger.info('Searching image %s', image_name)
                image = self._find_image(conn, server_id)
                if image.name != image_name:
                    raise ImageAPIError(self.duplicate_errmsg % (image.id, image.name, image.status))
                return image
            else:
                raise

    def snapshot(self, op, role_name):
        conn = __node__['platform'].new_cloudservers_conn()
        server_id = self._find_server_id(conn)
        for image in conn.images.findall(serverId=server_id):
            if image.status not in ('ACTIVE', 'FAILED'):
                raise ImageAPIError(
                    self.duplicate_errmsg % (image.id, image.name, image.status))

        image = None
        image_name = role_name + "-" + time.strftime("%Y%m%d%H%M%S")

        try:
            system2("sync", shell=True)
            _logger.info("Creating server image: serverId=%s name=%s", server_id, image_name)
            for _ in xrange(0, 2):
                image = self._create_image(conn, image_name, server_id)
                if not image:
                    continue
                _logger.debug('Image created: id=%s', image.id)
                break
            else:
                _logger.debug('Image is not created')
                raise ImageAPIError('Image is not created')

            _logger.info('Checking that image %s is completed', image.id)

            start_time = time.time()
            def completed(image_id):
                try:
                    image = conn.images.get(image_id)
                    if time.time() - start_time > 191:
                        globals()['start_time'] = time.time()
                        _logger.info('Progress: %s', image.progress)
                    return image.status in ('ACTIVE', 'FAILED')
                except:
                    _logger.debug('Caught exception', exc_info=sys.exc_info())
            wait_until(
                completed, 
                args=(image.id, ),
                sleep=30,
                logger=_logger,
                timeout=3600,
                error_text="Image %s wasn't completed in a reasonable time" % image.id)

            image = conn.images.get(image.id)
            if image.status == 'FAILED':
                raise ImageAPIError('Image %s becomes failed' % image.id)
            _logger.info('Image %s completed and available for use!', image.id)
        except:
            exc_type, exc_value, exc_trace = sys.exc_info()
            if image:
                try:
                    conn.images.delete(image)
                except:
                    _logger.debug('Image delete exception', exc_info=sys.exc_info())
            raise exc_type, exc_value, exc_trace

        return image.id
