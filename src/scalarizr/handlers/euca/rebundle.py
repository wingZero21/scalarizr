from __future__ import with_statement
'''
Created on Oct 12, 2010

@author: marat
'''

from scalarizr.bus import bus
from scalarizr.handlers import rebundle as rebundle_hdlr
from scalarizr.handlers.ec2 import rebundle as ec2_rebundle_hdlr
from scalarizr.handlers import HandlerError
from scalarizr import linux

import os
import glob


LOG = ec2_rebundle_hdlr.LOG

def get_handlers ():
    return [EucaRebundleHandler()]


class EucaRebundleStrategy(ec2_rebundle_hdlr.RebundleInstanceStoreStrategy):
    def run(self):
        if not linux.which('euca-bundle-vol'):
            raise HandlerError('euca-bundle-vol command not found, please install "euca2ools" package')

        cert_path = pk_path = cloud_cert_path = fstab_path = None
        try:
            cert, pk = self._platform.get_cert_pk()
            cert_path = bus.cnf.write_key('euca-cert.pem', cert)
            pk_path = bus.cnf.write_key('euca-pk.pem', pk)
            cloud_cert_path = bus.cnf.write_key('euca-cloud-cert.pem', self._platform.get_ec2_cert())
            access_key, secret_key = self._platform.get_access_keys()

            environ = os.environ.copy()
            environ.update({
                'EUCALYPTUS_CERT': cloud_cert_path,
                'EC2_CERT': cert_path,
                'EC2_PRIVATE_KEY': pk_path,
                'EC2_USER_ID': self._platform.get_account_id(),
                'EC2_ACCESS_KEY': access_key,
                'EC2_SECRET_KEY': secret_key,
                'EC2_URL': self._platform.get_access_data('ec2_url'),
                'S3_URL': self._platform.get_access_data('s3_url')
            })

            with open('/etc/fstab') as fp:
                fstab_path = bus.cnf.write_key('euca-fstab', fp.read())
            self._fix_fstab(filename=fstab_path)

            # Create image object for gathering directories exclude list
            image = rebundle_hdlr.LinuxImage('/', 
                        os.path.join(self._destination, self._image_name), 
                        self._excludes)
            
            excludes = list(image.excludes)
            try:
                excludes.append(glob.glob('/var/lib/dhcp*')[0])
            except IndexError:
                pass
            if linux.os.redhat_family or linux.os.oracle_family:
                excludes.append('/selinux/*')

            LOG.info('Executing euca-bundle-vol')
            out = linux.system((
                    linux.which('euca-bundle-vol'), 
                    '--arch', linux.os['arch'],
                    '--size', str(self._image_size),
                    '--destination', self._destination,
                    '--exclude', ','.join(excludes),
                    '--fstab', fstab_path,
                    '--prefix', self._image_name,
                    '--volume', '/'
                ),
                env=environ
            )[0]
            LOG.info(out)

            LOG.info('Uploading image')
            files_prefix = os.path.join(self._destination, self._image_name)
            files = glob.glob(files_prefix + '*')
            s3_manifest_path = self._upload_image_files(files, files_prefix + '.manifest.xml')

            return self._register_image(s3_manifest_path)

        finally:
            linux.system('chmod 755 {0}/keys/euca-*'.format(bus.cnf.private_path()), shell=True)
            linux.system('rm -f {0}/keys/euca-*'.format(bus.cnf.private_path()), shell=True)
            linux.system('rm -f {0}/{1}.*'.format(self._destination, self._image_name), shell=True)


class EucaRebundleHandler(ec2_rebundle_hdlr.Ec2RebundleHandler):
    def __init__(self):
        ec2_rebundle_hdlr.Ec2RebundleHandler.__init__(self, instance_store_strategy_cls=EucaRebundleStrategy)

    @property
    def _s3_bucket_name(self):
        pl = bus.platform
        return 'scalr2-images-%s' % pl.get_account_id()
