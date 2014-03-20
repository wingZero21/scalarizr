import logging
import os
import shutil
import sys
import time

from scalarizr.api.image import ImageAPI
from scalarizr.api.image import ImageAPIError
from scalarizr.util import system2
from scalarizr.linux import mount
from scalarizr.linux import rsync
from scalarizr.storage2.util import loop
from scalarizr import linux
from scalarizr.storage2 import filesystem

_logger = logging.getLogger(__name__)


class LinuxImage(object):
    SPECIAL_DIRS = ('/dev', '/media', '/mnt', '/proc', '/sys', '/cdrom', '/tmp')
    _volume = None
    # Image file
    path = None
    # Image device name
    # Returned by _create_image def
    devname = None
    # Image mount point
    mpoint = None
    # Directories excludes list
    excludes = None
    _excluded_mpoints = None
    _mtab = None

    def __init__(self, volume, path=None, excludes=None):
        self._mtab = mount.mounts()
        self._volume = volume
        self.mpoint = '/mnt/img-mnt'
        self.path = path

        # Create rsync excludes list
        self.excludes = set(self.SPECIAL_DIRS)  # Add special dirs
        self.excludes.update(excludes or ())    # Add user input
        self.excludes.add(self.mpoint)                  # Add image mount point
        if self.path:
            self.excludes.add(self.path)            # Add image path
        # Add all mounted filesystems, except bundle volume
        self._excluded_mpoints = [entry.mpoint for entry in self._mtab.list_entries()
            if entry.mpoint.startswith(self._volume) and entry.mpoint != self._volume]
        self.excludes.update(self._excluded_mpoints)

    def make(self):
        self.devname = self._create_image()
        self._format_image()
        system2("sync", shell=True)  # Flush so newly formatted filesystem is ready to mount.
        self._mount_image()
        self._make_special_dirs()
        self._copy_rec(self._volume, self.mpoint)
        system2("sync", shell=True)  # Flush buffers
        return self.mpoint

    def clean(self):
        self.umount()
        if os.path.exists(self.mpoint):
            os.rmdir(self.mpoint)

    def umount(self):
        if self.mpoint in self._mtab:
            _logger.debug("Unmounting '%s'", self.mpoint)
            system2("umount -d " + self.mpoint, shell=True, raise_exc=False)

    def _format_image(self):
        _logger.info("Formatting image")

        vol_entry = None
        for v in self._mtab:
            if v.device.startswith('/dev'):
                vol_entry = v
                break

        if vol_entry.device == '/dev/root' and not os.path.exists(vol_entry.device):
            vol_entry = [v for v in mount.mounts('/etc/mtab') 
                if v.device.startswith('/dev')][0]
        fs = filesystem(vol_entry.fstype)

        # create filesystem
        fs.mkfs(self.devname)

        # set EXT3/4 options
        if fs.type.startswith('ext'):
            # max mounts before check (-1 = disable)
            system2(('/sbin/tune2fs', '-c', '1', self.devname))
            # time based (3m = 3 month)
            system2(('/sbin/tune2fs', '-i', '3m', self.devname))

        # set label
        label = fs.get_label(vol_entry.device)
        if label:
            fs.set_label(self.devname, label)

        _logger.debug('Image %s formatted', self.devname)

    def _create_image(self):
        pass

    def _mount_image(self, options=None):
        _logger.info("Mounting image")
        if self.mpoint in  self._mtab:
            raise ImageAPIError("Image already mounted")
        options = options or []
        mount.mount(self.devname, self.mpoint, *options)

    def _make_special_dirs(self):
        _logger.info('Making special directories')

        # Create empty special dirs
        for dirname in self.SPECIAL_DIRS:
            spec_dir = self.mpoint + dirname
            if os.path.exists(dirname) and not os.path.exists(spec_dir):
                _logger.debug("Create spec dir %s", dirname)
                os.makedirs(spec_dir)
                if dirname == '/tmp':
                    os.chmod(spec_dir, 01777)

        # Create excluded mpoints dirs (not under special dirs)
        for dirname in self._excluded_mpoints:
            if not [dirname for spec_dir in self.SPECIAL_DIRS if dirname.startswith(spec_dir)]:
                if not os.path.exists(self.mpoint + dirname):
                    _logger.debug('Create mpoint dir %s', dirname)
                    os.makedirs(self.mpoint + dirname)

        # MAKEDEV is incredibly variable across distros, so use mknod directly.
        devdir = os.path.join(self.mpoint, 'dev')
        mknod = '/bin/mknod'
        nods = (
            'console c 5 1',
            'full c 1 7',
            'null c 1 3',
            'zero c 1 5',
            'tty c 5 0',
            'tty0 c 4 0',
            'tty1 c 4 1',
            'tty2 c 4 2',
            'tty3 c 4 3',
            'tty4 c 4 4',
            'tty5 c 4 5',
            'xvc0 c 204 191')
        for nod in nods:
            nod = nod.split(' ')
            nod[0] = devdir + '/' + nod[0]
            system2([mknod] + nod)

        _logger.debug("Special directories were created")

    def _copy_rec(self, source, dest, xattr=True):
        _logger.info("Copying %s into the image %s", source, dest)
        rsync_longs = dict(archive=True, sparse=True, times=True)
        if self.excludes:
            rsync_longs['exclude'] = list(self.excludes)
        #rsync = filetool.Rsync()
        #rsync.archive().times().sparse().links().quietly()
        #rsync.archive().sparse().xattributes()
        #rsync.archive().sparse().times()

        if xattr:
            rsync_longs['xattrs'] = True
        try:
            rsync.rsync(source, dest, **rsync_longs)
        except linux.LinuxError, e:
            if e.returncode == 24:
                _logger.warn(
                    "rsync exited with error code 24. This means a partial transfer due to vanished " +
                    "source files. In most cases files are copied normally")
            elif e.returncode == 23:
                _logger.warn(
                    "rsync seemed successful but exited with error code 23. This probably means " +
                    "that your version of rsync was built against a kernel with HAVE_LUTIMES defined, " +
                    "although the current kernel was not built with this option enabled. The bundling " +
                    "process will thus ignore the error and continue bundling.  If bundling completes " +
                    "successfully, your image should be perfectly usable. We, however, recommend that " +
                    "you install a version of rsync that handles this situation more elegantly.")
            elif e.returncode == 1 and xattr:
                _logger.warn(
                    "rsync with preservation of extended file attributes failed. Retrying rsync " +
                    "without attempting to preserve extended file attributes...")
                self._copy_rec(source, dest, xattr=False)
            else:
                raise


class LinuxLoopbackImage(LinuxImage):
    """
    This class encapsulate functionality to create an file loopback image
    from a volume. The image is created using dd. Sub-directories of the
    volume, including mounts of local filesystems, are copied to the image.
    Symbolic links are preserved.
    """

    MAX_IMAGE_SIZE = 10*1024
    _size = None

    def __init__(self, volume, image_file, image_size, excludes=None):
        '''
        @param volume: Path to mounted volume to create the bundle from. Ex: '/'
        @param image_file:  Destination file to store the bundled image. Ex: /mnt/img
        @param image_size: Image file size in Mb. Ex: 1408 (1Gb)
        @param excludes: list of directories and files to exclude. Ex: /mnt, /root/.*
        '''
        LinuxImage.__init__(self, volume, image_file, excludes)
        self._size = image_size or self.MAX_IMAGE_SIZE

    def make(self):
        _logger.info("Make image %s from volume %s (excludes: %s)",
            self.path, self._volume, ":".join(self.excludes))
        LinuxImage.make(self)

    def _create_image(self):
        _logger.debug('Creating loop device (file: %s, size: %s)', self.path, self._size)
        devname = loop.mkloop(self.path, size=self._size, quick=True)
        _logger.debug('Created loop device %s associated with file %s', devname, self.path)
        return devname

    def clean(self):
        LinuxImage.clean(self)
        if self.devname:
            loop.rmloop(self.devname)


class EC2ImageAPI(ImageAPI):
    pass
