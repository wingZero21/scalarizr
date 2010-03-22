'''
Created on Mar 11, 2010

@author: marat
'''

from scalarizr.core import Bus, BusEntries
from scalarizr.core.handlers import Handler
from scalarizr.platform import PlatformError
from scalarizr.platform.ec2 import AwsPlatformOptions
from scalarizr.messaging import Messages
import scalarizr.util as szutil
import logging
import time
import os
import re
import pipes
import M2Crypto
import binascii
import subprocess


_os = os.uname()[0].lower()

def get_handlers ():
	return [Ec2RebundleHandler()]

class Ec2RebundleHandler(Handler):
	_logger = None
	
	_platform = None
	"""
	@ivar scalarizr.platform.ec2.AwsPlatform: 
	"""
	
	def __init__(self):
		self._logger = logging.getLogger(__name__)
		self._platform = Bus()[BusEntries.PLATFORM]
	
	def on_Rebundle(self, message):
		""" !!!! """
		self._platform.set_config_options(dict(
			account_id="12121212",
			cert="trololo",
			pk="trolololo"
		))
		""" !!!! """
	
	
		aws_account_id = self._platform.get_config_option(AwsPlatformOptions.ACCOUNT_ID)
		avail_zone = self._platform.get_avail_zone()
		region = avail_zone[0:2]
		prefix = message.body["role_name"] + "-" + str(int(time.time()))
		cert, pk = self._platform.get_cert_pk()
		
		# Create exclude directories list
		excludes = message.body["excludes"].split(":") \
				if message.body.has_key("excludes") else []
		excludes += ["/mnt"]		
		
		try:
			self._bundle_vol(
				prefix=prefix, 
				destination="/mnt", 
				excludes=excludes, 
				cert=cert, 
				pk=pk, 
				account_id=aws_account_id, 
				kernel_id=self._platform.get_kernel_id(), 
				ramdisk_id=self._platform.get_ramdisk_id(), 
				region=region, 
				avail_zone=avail_zone
			)
		except (BaseException, Exception), e:
			self._logger.error("Cannot bundle image. Error: %s", str(e))
			raise
			
		bucket = "scalr-images-%s-%s" % (region, aws_account_id)
		
		
		pass

	def _bundle_vol(self, prefix="", volume="/", destination=None, 
				size=None, excludes=[], 
				cert=None, pk=None, account_id=None, 
				kernel_id="", ramdisk_id="", region=None, avail_zone=None):
		
		self._logger.info("Check requirements to run bundle")
		
		self._logger.debug("Checking user is root")
		if not self._is_super_user():
			raise PlatformError("You need to be root to run rebundle")
		self._logger.debug("User check success")
		
		image_file = destination + "/" + prefix
		if size is None:
			size = LoopbackImage.MAX_IMAGE_SIZE	
		
		self._logger.info("Creating directory exclude list")
		# Create list of directories to exclude from the image
		if excludes is None:
			excludes = []
		
		# Exclude mounted non-local filesystems if they are under the volume root
		mtab = Mtab()
		print mtab.list_entries()
		print list(entry.mpoint for entry in mtab.list_entries() if entry.fstype in Mtab.LOCAL_FS_TYPES)
		
		excludes += list(entry.mpoint for entry in mtab.list_entries()  
				if entry.fstype in Mtab.LOCAL_FS_TYPES)
		
		# Exclude the image file if it is under the volume root.
		if image_file.startswith(volume):
			excludes.append(image_file)
		
		# Unique list
		excludes = list(set(excludes))
		self._logger.debug("Exclude list: " + str(excludes))		
		
		# Create image from volume
		self._logger.info("Creating loopback image device")
		image = LoopbackImage(volume, image_file, LoopbackImage.MAX_IMAGE_SIZE, excludes)
		image.make()
		
		
	def _bundle_image(self, image_file, user, arch, destination,
					user_private_key, user_cert, ec2_cert,
					optional_args, inherit=True):
		
		# Create named pipes.
		digest_pipe = os.path.join('/tmp', 'ec2-bundle-image-digest-pipe')
		if os.path.exists(digest_pipe):
			os.remove(digest_pipe)
		try:
			os.mkfifo(digest_pipe)
		except:
			self._logger.error("Cannot create named pipe %s", digest_pipe)
			raise
		
	
		# Load and generate necessary keys.
		name = os.path.basename(image_file)
		manifest_file = os.path.join(destination, name + '.manifest.xml')
		bundled_file_path = os.path.join(destination, name + '.tar.gz.enc')
		try:
			user_public_key = M2Crypto.X509.load_cert_string(user_cert).get_pubkey()
		except:
			self._logger.error("Cannot read user certificate")
			raise
		try:
			ec2_public_key = M2Crypto.X509.load_cert_string(ec2_cert).get_pubkey()
		except:
			self._logger.error("Cannot read ec2 certificate")
			raise
		key = binascii.b2a_hex(M2Crypto.Rand.rand_bytes(16))
		iv = binascii.b2a_hex(M2Crypto.Rand.rand_bytes(8))


		# Bundle the AMI.
		# The image file is tarred - to maintain sparseness, gzipped for
		# compression and then encrypted with AES in CBC mode for
		# confidentiality.
		# To minimize disk I/O the file is read from disk once and
		# piped via several processes. The tee is used to allow a
		# digest of the file to be calculated without having to re-read
		# it from disk.
		_os = globals()["_os"]
		openssl = "/usr/sfw/bin/openssl" if _os == "sunos" else "openssl"
		tar = Tar()
		tar.create().dereference().sparse()
		tar.add(os.path.basename(image_file), os.path.dirname(image_file))
		digest_file = os.path.join('/tmp', 'ec2-bundle-image-digest.sha1')
		
		szutil.system(" | ".join([
			"{openssl} sha1 -r -out {digest_file} < {digest_pipe} & {tar}", 
			"tee {digest_pipe}",  
			"gzip", 
			"{openssl} enc -e -aes-128-cbc -K {key} -iv {iv} > {bundled_file_path}"]).format(
				openssl=openssl, digest_pipe=digest_pipe, tar=str(tar),
				key=key, iv=iv, bundled_file_path=bundled_file_path
		))
		try:
			digest = open(digest_file).read()
			try:
				digest = digest.split(" ")[0]
			except IndexError, e:
				self._logger.error("Cannot extract digest from string '%s'. %s", digest, e)
		except OSError, e:
			self._logger.error("Cannot read file with image digest '%s'. %s", digest_file, e)
			raise


		# Split the bundled AMI. 
		# Splitting is not done as part of the compress, encrypt digest
		# stream, so that the filenames of the parts can be easily
		# tracked. The alternative is to create a dedicated output
		# directory, but this leaves the user less choice.
		
		
		"""
	  parts = Bundle::split( bundled_file_path, name, destination )
	  
	  # Sum the parts file sizes to get the encrypted file size.
	  bundled_size = 0
	  parts.each do |part|
		bundled_size += File.size( File.join( destination, part ) )
	  end		
		"""
		
			
	def _is_super_user(self):
		out = szutil.system("id -u")[0]
		return out.strip() == "0"
	
	def accept(self, message, queue, behaviour=None, platform=None, os=None, dist=None):
		return message.name == Messages.REBUNDLE and platform == "ec2"
	
	
class Fstab:
	"""
	Wrapper over /etc/fstab
	"""
	LOCATION = None
	_entries = None
	_filename = None
	_re = None
	
	def __init__(self, filename=None):
		self._filename = filename if not filename is None else Mtab.LOCATION
		self._entries = None
		self._re = re.compile("^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+).*$")
		
	def list_entries(self, rescan=False):
		if not self._entries or rescan:
			self._entries = []
			f = open(self._filename, "r")
			for line in f:
				if line[0:1] == "#":
					continue
				m = self._re.match(line)
				if m:
					self._entries.append(_TabEntry(
						m.group(1), m.group(2), m.group(3), m.group(4), line.strip()
					))
			f.close()
			
		return list(self._entries)

class Mtab(Fstab):
	"""
	Wrapper over /etc/mtab
	"""
	LOCAL_FS_TYPES = None	
		
class _TabEntry(object):
	device = None
	mpoint = None
	fstype = None
	options = None	
	value = None
	
	def __init__(self, device, mpoint, fstype, options, value):
		self.device = device
		self.mpoint = mpoint
		self.fstype = fstype
		self.options = options		
		self.value = value

		

if _os == "linux":
	Fstab.LOCATION = "/etc/fstab"	
	Mtab.LOCATION = "/etc/mtab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs')
	
elif _os == "sunos":
	Fstab.LOCATION = "/etc/vfstab"	
	Mtab.LOCATION = "/etc/mnttab"
	Mtab.LOCAL_FS_TYPES = ('ext2', 'ext3', 'xfs', 'jfs', 'reiserfs', 'tmpfs', 
		'ufs', 'sharefs', 'dev', 'devfs', 'ctfs', 'mntfs',
		'proc', 'lofs',   'objfs', 'fd', 'autofs')


print Mtab.LOCAL_FS_TYPES

if _os == "linux":
	class LinuxLoopbackImage:
		"""
		This class encapsulate functionality to create an file loopback image
		from a volume. The image is created using dd. Sub-directories of the 
		volume, including mounts of local filesystems, are copied to the image. 
		Symbolic links are preserved. 	
		"""
		
		MAX_IMAGE_SIZE = 10*1024
		
		_volume = None
		_image_file = None
		_image_size = None	
		_image_mpoint = "/mnt/img-mnt"
		_excludes = None	
		_logger = None
		
		def __init__(self, volume, image_file, image_size, excludes):
			self._logger = logging.getLogger(__name__)
			
			self._volume = volume
			self._image_file = image_file
			self._image_size = image_size
			self._excludes = excludes
	
			if self._image_mpoint.startswith(volume):
				self._excludes.append(self._image_mpoint)
		
		
		def make(self):
			self._logger.info("Copying %s into the image file %s...", self._volume, self._image_file)
			self._logger.info("Exclude list: %s", ":".join(self._excludes))
	
			try:
				self._create_image_file()
				self._format_image()
				szutil.system("sync")  # Flush so newly formatted filesystem is ready to mount.
				self._mount_image()
				self._make_special_dirs()
				self._copy_rec(self._volume, self._image_mpoint)
			except:
				self._cleanup()
				raise
			
		def _create_image_file(self):
			self._logger.debug("Create image file")
			szutil.system("dd if=/dev/zero of=" + self._image_file + " bs=1M count=1 seek=" + str(self._image_size))
		
		def _format_image(self):
			self._logger.debug("Format image file")
			szutil.system("/sbin/mkfs.ext3 -F " + self._image_file)
			szutil.system("/sbin/tune2fs -i 0 " + self._image_file)
		
		def _mount_image(self):
			"""
			Mount the image file as a loopback device. The mount point is created
			if necessary.		
			"""
			self._logger.debug("Mount image file")
			if not os.path.exists(self._image_mpoint):
				os.makedirs(self._image_mpoint)
			if self._is_mounted(self._image_mpoint):
				raise PlatformError("Image already mounted")
			szutil.system("mount -o loop " + self._image_file + " " + self._image_mpoint)
		
		def _make_special_dirs(self):
			self._logger.debug("Make special directories")
			# Make /proc /sys /mnt
			os.makedirs(self._image_mpoint + "/mnt")
			os.makedirs(self._image_mpoint + "/proc")
			os.makedirs(self._image_mpoint + "/sys")
			
			# Make device nodes.
			dev_dir = self._image_mpoint + "/dev"
			os.makedirs(dev_dir)
			# MAKEDEV is incredibly variable across distros, so use mknod directly.
			szutil.system("mknod " + dev_dir + "/null c 1 3")
			szutil.system("mknod " + dev_dir + "/zero c 1 5")
			szutil.system("mknod " + dev_dir + "/tty c 5 0")
			szutil.system("mknod " + dev_dir + "/console c 5 1")
			szutil.system("ln -s null " + dev_dir +"/X0R")		
		
		def _copy_rec(self, source, dest, xattr=True):
			self._logger.debug("Copy volume to image file")
			rsync = Rsync()
			rsync.archive().times().sparse().links().quietly()
			if xattr:
				rsync.xattributes()
			rsync.exclude(self._excludes)
			rsync.source(os.path.join(source, "/*")).dest(dest)
			exitcode = szutil.system(str(rsync))[2]
			if exitcode == 23 and Rsync.usable():
				self._logger.warning(
					"rsync seemed successful but exited with error code 23. This probably means " +
	           		"that your version of rsync was built against a kernel with HAVE_LUTIMES defined, " +
	             	"although the current kernel was not built with this option enabled. The bundling " +
				 	"process will thus ignore the error and continue bundling.  If bundling completes " +
	           		"successfully, your image should be perfectly usable. We, however, recommend that " +
			   		"you install a version of rsync that handles this situation more elegantly.")
			elif exitcode == 1 and xattr:
				self._logger.warning(
					"rsync with preservation of extended file attributes failed. Retrying rsync " +
	           		"without attempting to preserve extended file attributes...")
				self._copy_rec(source, dest, xattr=False)
		
		"""
		def _update_fstab(self):
			pass
		"""
		
		def _cleanup (self):
			self._unmount(self._image_mpoint)
			
		def _is_mounted(self, mpoint):
			mtab = Mtab()
			for entry in mtab.list_entries():
				if entry.mpoint == mpoint:
					self._logger.info("entry.mpoint="+entry.mpoint+" mpoint="+mpoint)
					self._logger.warning("_is_mounted = True")					
					return True
			return False
			
		def _unmount(self, mpoint):
			if not self._is_mounted(mpoint):
				szutil.system("umount -d " + mpoint)
				
	LoopbackImage = LinuxLoopbackImage
	
elif _os == "sunos":
	class SolarisLoopbakImage:
		
		MAX_IMAGE_SIZE = 10*1024
		
		"""
		@todo: Solaris support
		"""
		pass
	
	LoopbackImage = SolarisLoopbakImage


			
class Rsync(object):
	"""
	Wrapper for rsync
	"""
	
	EXECUTABLE = "rsync"
	_options = None
	_src = None
	_dst = None
	_executable = None
	_quiet = None
	
	def __init__(self, executable=None):
		self._executable = executable if executable is not None else Rsync.EXECUTABLE
		self._options = []
		self._src = self._dst = None
		self._quiet = False
		
	def archive(self):
		self._options.append('-rlpgoD')
		return self
	
	def times(self):
		self._options.append('-t')
		return self
	
	def recursive(self):
		self._options.append('-r')
		return self
		
	def sparse(self):
		self._options.append('-S')
		return self

	def links(self):
		self._options.append('-l')
		return self
		
	def dereference(self):
		self._options.append('-L')
		return self
	
	def xattributes(self):
		self._options.append('-X')
		return self

	def exclude(self, files):
		for file in files:
			self._options.append("--exclude " + pipes.quote(file))
		return self

	def version(self):
		self._options.append("--version")
		return self
		
	def source(self, path):
		self._src = path
		return self
		
	def dest(self, path):
		self._dst = path
		return self
		
	def quietly(self):
		self._quiet = True
		return self
	
	def __str__(self):
		return "{executable} {options} {src} {dst} {quiet}".format(
			executable=self._executable,
			options=" ".join(self._options),
			src=self._src,
			dst=self._dst,
			quiet="2>&1 > /dev/null" if self._quiet else ""
		).strip()

	@staticmethod
	def usable():
		"""
		@todo: implement
		"""
		return True


class Tar:
	EXECUTABLE = "/usr/sfw/bin/gtar" if globals()["_os"] == "sunos" else "tar"
	
	_executable = None
	_options = None
	_files = None
	
	def __init__(self, executable=None):
		self._executable = executable if executable is not None else self.EXECUTABLE
		self._options = []
		self._files = []
		
	def version(self):
		self._options.append("--version")
		return self
	
	def verbose(self):
		self._options.append("-v")
		return self
	
	def create(self):
		self._options.append("-c")
		return self
		
	def bzip2(self):
		self._options.append("-j")
		return self

	def diff(self):
		self._options.append("-d")
		return self
		
	def gzip(self):
		self._options.append("-z")
		return self

	def extract(self):
		self._options.append("-x")
		return self

	def update(self):
		self._options.append("-u")
		return self

	def sparse(self):
		self._options.append("-S")
		return self

	def dereference(self):
		self._options.append("-h")
		return self

	def archive(self, filename):
		self._options.append("-f " + filename if filename is not None else "-")
		return self
	
	def chdir(self, dir):
		self._options.append("-C " + dir)
		return self
	
	def add(self, filename, dir=None):
		item = filename if dir is None else "-C "+dir+" "+filename
		self._files.append(item)
		return self
	
	def __str__(self):
		return "{execurable} {options} {files}".format(
			executable=self._executable,
			options=" ".join(self._options),
			files=" ".join(self._files)
		).strip()
	

class FileUtil:
	BUFFER_SIZE = 1024 * 1024	# Buffer size in bytes.
	PART_SUFFIX = '.part.'	
	
	@staticmethod
	def split(filename, part_name_prefix, cb_size, dst_dir):
		f = None
		try:
			try:
				#f =
				pass
			except OSError:
				pass
		finally:
			if f is not None:
				f.close()
	
	@staticmethod	
	def _write_chunk(source_file, chunk_file, chunk_size):
		cb_written = 0  # Bytes written.
		cb_left = chunk_size	# Bytes left to write in this chunk.
		
		"""
	while (!sf.eof? && cb_left > 0) do
	  buf = sf.read(BUFFER_SIZE < cb_left ? BUFFER_SIZE : cb_left)
	  cf.write(buf)
	  cb_written += buf.length
	  cb_left = cs - cb_written
	end
	sf.eof	
	"""	
		
		pass